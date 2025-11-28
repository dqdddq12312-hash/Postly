from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, abort, send_from_directory, g
import atexit
import os
import tempfile
import logging
import threading
from flask_sqlalchemy import SQLAlchemy
from utils.performance_monitor import monitor_performance, log_cache_hit, PerformanceTimer
from sqlalchemy import func, inspect
from dotenv import load_dotenv
from datetime import datetime, timedelta
import secrets
import requests
from urllib.parse import quote
from auth import PasswordHelper, login_required
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from tiktok_service import (
    TikTokApiError,
    build_tiktok_oauth_url,
    exchange_tiktok_code_for_token,
    fetch_tiktok_post_stats,
    get_tiktok_accounts,
    list_tiktok_posts,
    missing_tiktok_publish_scopes,
    publish_tiktok_video,
    tiktok_can_publish,
)

load_dotenv()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', os.path.join(BASE_DIR, 'uploads'))

# Create the Flask app with explicit template folder
app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dfe8216ff1440e9e8137744e5087c537')
# Only require secure cookies in production (HTTPS), not in development (HTTP localhost)
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV') == 'production'
server_name = os.getenv('SERVER_NAME')
if server_name:
    app.config['SERVER_NAME'] = server_name

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size

# Sendgrid configuration
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = os.getenv('SENDGRID_FROM_EMAIL', 'postly_co@outlook.com.vn')

# Database configuration
database_url = os.getenv('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url or f"sqlite:///{os.path.join(BASE_DIR, 'fb_tokens.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


@app.before_request
def load_current_user():
    """Ensure session user still exists and expose it via flask.g."""
    g.current_user = None

    user_id = session.get('user_id')
    if not user_id:
        return None

    user = User.query.get(user_id)
    if user:
        g.current_user = user
        return None

    # Stale session: clear it and respond based on request type
    session.clear()
    message = 'Your session has expired. Please log in again.'

    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'error': message, 'code': 'SESSION_EXPIRED'}), 401

    endpoint = (request.endpoint or '').split('.')[-1]
    if endpoint in {'login', 'signup', 'serve_upload', 'static'}:
        flash(message, 'warning')
        return None

    flash(message, 'warning')
    return redirect(url_for('login'))

# Setup logger for analytics
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ENABLE_TIKTOK_DEMO = os.getenv('ENABLE_TIKTOK_DEMO', 'false').lower() == 'true'
PAGE_HISTORY_IMPORT_LIMIT = int(os.getenv('PAGE_HISTORY_IMPORT_LIMIT', '200'))
ANALYTICS_REFRESH_DEFAULT_BATCH = int(os.getenv('ANALYTICS_REFRESH_DEFAULT_BATCH', '25'))
ANALYTICS_REFRESH_AUTO_THRESHOLD = int(os.getenv('ANALYTICS_REFRESH_AUTO_THRESHOLD', '1'))
SELF_CRON_ENABLED = os.getenv('ENABLE_SELF_CRON', 'true').lower() == 'true'
SELF_CRON_INTERVAL = int(os.getenv('SELF_CRON_INTERVAL_SECONDS', '60'))
self_cron_runner = None
_self_cron_started = False


# ======================== DATABASE MODELS ========================

class User(db.Model):
    """User model for authentication"""
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    username = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    first_name = db.Column(db.String(255), nullable=True)
    last_name = db.Column(db.String(255), nullable=True)
    is_verified = db.Column(db.Boolean, default=False)
    verification_code = db.Column(db.String(255), nullable=True, index=True)
    verification_code_expires = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime, nullable=True)
    
    # Relationships - use foreign_keys to disambiguate when there are multiple FK to same table
    connected_pages = db.relationship('ConnectedPage', foreign_keys='ConnectedPage.user_id', backref='user', lazy=True, cascade='all, delete-orphan')
    oauth_tokens = db.relationship('OAuthToken', backref='user', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<User {self.email}>'


class OAuthToken(db.Model):
    """OAuth token storage for social media platforms"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    platform = db.Column(db.String(50), nullable=False)  # 'facebook', 'instagram', 'tiktok'
    access_token = db.Column(db.Text, nullable=False)
    refresh_token = db.Column(db.Text, nullable=True)
    token_expires_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('user_id', 'platform', name='unique_user_platform'),
    )
    
    def __repr__(self):
        return f'<OAuthToken {self.platform} for user {self.user_id}>'


class ConnectedPage(db.Model):
    """Connected social media pages for a user"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=True, index=True)  # Team if team-owned
    platform = db.Column(db.String(50), nullable=False)  # 'facebook', 'instagram', 'tiktok'
    platform_page_id = db.Column(db.String(255), nullable=False)
    page_name = db.Column(db.String(255), nullable=False)
    page_username = db.Column(db.String(255), nullable=True)
    page_profile_pic = db.Column(db.String(255), nullable=True)
    page_access_token = db.Column(db.Text, nullable=True)  # Page-specific access token for posting
    is_active = db.Column(db.Boolean, default=True)
    is_team_owned = db.Column(db.Boolean, default=False)  # True if connected by admin, False if connected by owner
    connected_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)  # User who connected this channel
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('user_id', 'platform', 'platform_page_id', name='unique_page_per_user'),
    )
    
    def __repr__(self):
        return f'<ConnectedPage {self.page_name} ({self.platform})>'


class Post(db.Model):
    """Social media posts scheduled or sent by users"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    title = db.Column(db.String(500), nullable=True)
    content = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(50), default='draft', index=True)  # 'draft', 'scheduled', 'publishing', 'sent', 'failed' - indexed for filtering
    scheduled_time = db.Column(db.DateTime, nullable=True, index=True)  # Indexed for date range queries
    sent_time = db.Column(db.DateTime, nullable=True, index=True)  # Indexed for date range queries and sorting
    caption = db.Column(db.Text, nullable=True)
    post_icon = db.Column(db.String(255), nullable=True)  # emoji or icon name
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Approval workflow fields
    submitted_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)  # User who submitted for approval
    approved_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)  # User who approved/rejected
    approval_status = db.Column(db.String(50), nullable=True)  # 'pending', 'approved', 'rejected'
    approval_notes = db.Column(db.Text, nullable=True)  # Admin's feedback on approval/rejection
    approval_requested_at = db.Column(db.DateTime, nullable=True)  # When approval was requested
    approval_responded_at = db.Column(db.DateTime, nullable=True)  # When admin approved/rejected
    
    # Relationships
    media = db.relationship('PostMedia', backref='post', lazy=True, cascade='all, delete-orphan')
    page_associations = db.relationship('PostPageAssociation', backref='post', lazy=True, cascade='all, delete-orphan')
    submitted_by = db.relationship('User', foreign_keys=[submitted_by_user_id])
    approved_by = db.relationship('User', foreign_keys=[approved_by_user_id])
    
    # Add composite index for common analyze page query
    __table_args__ = (
        db.Index('idx_post_status_sent_time', 'status', 'sent_time'),  # For filtering and sorting published posts
    )
    
    def __repr__(self):
        return f'<Post {self.id} ({self.status})>'


class PostMedia(db.Model):
    """Media files associated with posts (images, videos)"""
    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey('post.id'), nullable=False, index=True)
    media_url = db.Column(db.String(500), nullable=False)
    media_type = db.Column(db.String(50), nullable=False)  # 'image', 'video', 'gif'
    file_size = db.Column(db.Integer, nullable=True)
    duration = db.Column(db.Integer, nullable=True)  # in seconds for videos
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<PostMedia {self.media_type} for post {self.post_id}>'


class PostPageAssociation(db.Model):
    """Association between posts and connected pages (many-to-many)"""
    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey('post.id'), nullable=False, index=True)
    page_id = db.Column(db.Integer, db.ForeignKey('connected_page.id'), nullable=False, index=True)
    platform_post_id = db.Column(db.String(255), nullable=True)  # ID from the platform after posting
    status = db.Column(db.String(50), default='pending')  # 'pending', 'sent', 'failed'
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    connected_page = db.relationship('ConnectedPage', backref='post_associations')
    
    __table_args__ = (
        db.UniqueConstraint('post_id', 'page_id', name='unique_post_page'),
    )
    
    def __repr__(self):
        return f'<PostPageAssociation post={self.post_id} page={self.page_id}>'


class PostAnalytics(db.Model):
    """Real analytics data for posts from social media platforms"""
    id = db.Column(db.Integer, primary_key=True)
    post_page_association_id = db.Column(db.Integer, db.ForeignKey('post_page_association.id'), nullable=False, index=True)
    
    # Engagement metrics (actual values from Facebook)
    impressions = db.Column(db.Integer, default=0)  # Total views
    reach = db.Column(db.Integer, default=0)  # Unique people who saw it
    clicks = db.Column(db.Integer, default=0)  # Link clicks
    likes = db.Column(db.Integer, default=0)
    comments = db.Column(db.Integer, default=0)
    shares = db.Column(db.Integer, default=0)
    saves = db.Column(db.Integer, default=0)  # Instagram/TikTok saves
    engagement = db.Column(db.Float, default=0.0)  # Engagement rate percentage
    video_views = db.Column(db.Integer, default=0)  # Video views for video posts
    
    # Time data
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    post_page_association = db.relationship('PostPageAssociation', backref=db.backref('analytics', uselist=False, lazy='joined'))
    
    def __repr__(self):
        return f'<PostAnalytics post_page={self.post_page_association_id}>'


class DailyAnalyticsSummary(db.Model):
    """Pre-calculated daily analytics summaries for fast loading (Buffer-style Tier 2 data)"""
    __tablename__ = 'daily_analytics_summary'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    page_id = db.Column(db.Integer, db.ForeignKey('connected_page.id'), nullable=True, index=True)  # NULL for user totals
    date = db.Column(db.Date, nullable=False, index=True)
    
    # Aggregated metrics
    total_posts = db.Column(db.Integer, default=0)
    total_impressions = db.Column(db.Integer, default=0)
    total_reach = db.Column(db.Integer, default=0)
    total_clicks = db.Column(db.Integer, default=0)
    total_likes = db.Column(db.Integer, default=0)
    total_comments = db.Column(db.Integer, default=0)
    total_shares = db.Column(db.Integer, default=0)
    total_video_views = db.Column(db.Integer, default=0)
    avg_engagement_rate = db.Column(db.Float, default=0.0)
    
    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('user_id', 'page_id', 'date', name='unique_user_page_date'),
        db.Index('idx_user_date', 'user_id', 'date'),
        db.Index('idx_page_date', 'page_id', 'date'),
    )
    
    def __repr__(self):
        return f'<DailyAnalyticsSummary user={self.user_id} page={self.page_id} date={self.date}>'


# ======================== TEAM MANAGEMENT MODELS ========================

class Team(db.Model):
    """
    Team/Organization model representing a team with owner, members, and channels.
    One Owner per team. Multiple Team Members with roles (Admin, Member).
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = db.relationship('User', backref='owned_teams', foreign_keys=[owner_id])
    members = db.relationship('TeamMember', backref='team', lazy=True, cascade='all, delete-orphan')
    channel_access = db.relationship('ChannelAccess', backref='team', lazy=True, cascade='all, delete-orphan')
    invitations = db.relationship('TeamInvitation', backref='team', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Team {self.name} (Owner: {self.owner_id})>'
    
    def get_owner(self):
        """Get the Owner of this team"""
        return self.owner
    
    def get_admins(self):
        """Get all Admin members of this team"""
        return [m for m in self.members if m.role == 'admin']
    
    def get_members(self):
        """Get all regular Member members of this team"""
        return [m for m in self.members if m.role == 'member']
    
    def has_member(self, user_id):
        """Check if user is a member of this team"""
        return any(m.user_id == user_id for m in self.members)
    
    def get_member(self, user_id):
        """Get a specific team member by user_id"""
        for m in self.members:
            if m.user_id == user_id:
                return m
        return None


class TeamMember(db.Model):
    """
    Represents a team member with their role.
    Role: 'admin' or 'member' (Owner is managed separately)
    """
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    role = db.Column(db.String(50), default='member')  # 'admin' or 'member'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship('User', backref='team_memberships')
    
    __table_args__ = (
        db.UniqueConstraint('team_id', 'user_id', name='unique_team_member'),
    )
    
    def __repr__(self):
        return f'<TeamMember user={self.user_id} team={self.team_id} role={self.role}>'
    
    def is_admin(self):
        """Check if this member is an admin"""
        return self.role == 'admin'
    
    def is_member(self):
        """Check if this member is a regular member"""
        return self.role == 'member'


class ChannelAccess(db.Model):
    """
    Defines channel-level access for team members.
    access_level: 'full_posting' (can post directly), 'approval_required' (drafts need approval), or 'none' (no access)
    """
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False, index=True)
    team_member_id = db.Column(db.Integer, db.ForeignKey('team_member.id'), nullable=True, index=True)  # NULL for owner
    channel_id = db.Column(db.Integer, db.ForeignKey('connected_page.id'), nullable=False, index=True)
    access_level = db.Column(db.String(50), default='none')  # 'full_posting', 'approval_required', 'none'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    channel = db.relationship('ConnectedPage', backref='team_access')
    team_member = db.relationship('TeamMember', backref='channel_access')
    
    __table_args__ = (
        db.UniqueConstraint('team_id', 'team_member_id', 'channel_id', name='unique_team_channel_access'),
    )
    
    def __repr__(self):
        return f'<ChannelAccess team={self.team_id} member={self.team_member_id} channel={self.channel_id} level={self.access_level}>'
    
    def has_full_posting(self):
        """Check if this access level is full posting"""
        return self.access_level == 'full_posting'
    
    def requires_approval(self):
        """Check if this access level requires approval"""
        return self.access_level == 'approval_required'
    
    def has_no_access(self):
        """Check if this access level is none"""
        return self.access_level == 'none'


class TeamInvitation(db.Model):
    """
    Represents pending invitations to join a team.
    """
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False, index=True)
    invited_email = db.Column(db.String(255), nullable=False)
    invited_name = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(50), default='member')  # 'admin' or 'member'
    invitation_token = db.Column(db.String(255), unique=True, nullable=False, index=True)
    status = db.Column(db.String(50), default='pending')  # 'pending', 'accepted', 'declined'
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, default=lambda: datetime.utcnow() + timedelta(days=7))
    accepted_at = db.Column(db.DateTime, nullable=True)
    
    # Relationships
    created_by = db.relationship('User', backref='sent_team_invitations', foreign_keys=[created_by_user_id])
    
    def __repr__(self):
        return f'<TeamInvitation {self.invited_email} to team={self.team_id} status={self.status}>'
    
    def is_pending(self):
        """Check if invitation is still pending"""
        return self.status == 'pending'
    
    def is_expired(self):
        """Check if invitation has expired"""
        return datetime.utcnow() > self.expires_at
    
    def is_valid(self):
        """Check if invitation is valid (pending and not expired)"""
        return self.is_pending() and not self.is_expired()


class PageImportJob(db.Model):
    """Tracks asynchronous history imports for connected pages."""
    __tablename__ = 'page_import_jobs'

    id = db.Column(db.Integer, primary_key=True)
    page_id = db.Column(db.Integer, db.ForeignKey('connected_page.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    platform = db.Column(db.String(50), nullable=False)
    scope = db.Column(db.String(50), default='auto')
    status = db.Column(db.String(20), default='pending')
    max_posts = db.Column(db.Integer, nullable=True)
    total_posts_found = db.Column(db.Integer, default=0)
    posts_imported = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'page_id': self.page_id,
            'user_id': self.user_id,
            'platform': self.platform,
            'scope': self.scope,
            'status': self.status,
            'max_posts': self.max_posts,
            'total_posts_found': self.total_posts_found,
            'posts_imported': self.posts_imported,
            'error_message': self.error_message,
            'created_at': self.created_at.isoformat() + 'Z' if self.created_at else None,
            'started_at': self.started_at.isoformat() + 'Z' if self.started_at else None,
            'finished_at': self.finished_at.isoformat() + 'Z' if self.finished_at else None,
        }


class AnalyticsRefreshJob(db.Model):
    """Background job for refreshing analytics in manageable batches."""
    __tablename__ = 'analytics_refresh_jobs'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    page_id = db.Column(db.Integer, db.ForeignKey('connected_page.id'), nullable=True, index=True)
    scope = db.Column(db.String(50), default='manual')
    status = db.Column(db.String(20), default='pending')
    batch_size = db.Column(db.Integer, default=ANALYTICS_REFRESH_DEFAULT_BATCH)
    total_posts = db.Column(db.Integer, default=0)
    processed = db.Column(db.Integer, default=0)
    failed = db.Column(db.Integer, default=0)
    skipped = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
    last_progress_at = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'page_id': self.page_id,
            'scope': self.scope,
            'status': self.status,
            'batch_size': self.batch_size,
            'total_posts': self.total_posts,
            'processed': self.processed,
            'failed': self.failed,
            'skipped': self.skipped,
            'error_message': self.error_message,
            'created_at': self.created_at.isoformat() + 'Z' if self.created_at else None,
            'started_at': self.started_at.isoformat() + 'Z' if self.started_at else None,
            'finished_at': self.finished_at.isoformat() + 'Z' if self.finished_at else None,
            'last_progress_at': self.last_progress_at.isoformat() + 'Z' if self.last_progress_at else None,
        }


# ======================== PERMISSION HELPER FUNCTIONS ========================

def check_owner_access(team_id, user_id):
    """Check if user is the owner of a team"""
    team = Team.query.get(team_id)
    if not team:
        return False
    return team.owner_id == user_id


def check_admin_access(team_id, user_id):
    """Check if user is owner or admin of a team"""
    team = Team.query.get(team_id)
    if not team:
        return False
    
    # Owner always has admin access
    if team.owner_id == user_id:
        return True
    
    # Check if user is an admin member
    member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first()
    return member and member.is_admin()


def check_team_member_access(team_id, user_id):
    """Check if user is a member (any role) of a team"""
    team = Team.query.get(team_id)
    if not team:
        return False
    
    # Owner is always a member
    if team.owner_id == user_id:
        return True
    
    # Check if user is a team member
    member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first()
    return member is not None


def get_user_channel_access(user_id, channel_id):
    """
    Get the access level for a user on a specific channel.
    Returns: 'owner' (automatic full access), 'full_posting', 'approval_required', or 'none'
    """
    # Check if user owns the channel
    page = ConnectedPage.query.get(channel_id)
    if page and page.user_id == user_id:
        return 'owner'
    
    # Check team access
    access = ChannelAccess.query.filter(
        ChannelAccess.channel_id == channel_id,
        db.or_(
            ChannelAccess.team_member_id.in_(
                db.session.query(TeamMember.id).filter_by(user_id=user_id)
            ),
            ChannelAccess.team_id.in_(
                db.session.query(Team.id).filter_by(owner_id=user_id)
            )
        )
    ).first()
    
    if access:
        return access.access_level
    
    return 'none'


def can_publish_to_channel(user_id, channel_id):
    """Check if user has full posting or owner access to a channel"""
    access = get_user_channel_access(user_id, channel_id)
    return access in ['owner', 'full_posting']


def can_request_approval_on_channel(user_id, channel_id):
    """Check if user has approval_required access to a channel"""
    access = get_user_channel_access(user_id, channel_id)
    return access == 'approval_required'


def can_approve_posts_on_channel(user_id, channel_id):
    """Check if user has authority to approve posts on a channel"""
    access = get_user_channel_access(user_id, channel_id)
    return access in ['owner', 'full_posting']


def get_accessible_team_channels(user_id):
    """
    Get all channels (ConnectedPages) that a user has access to through team membership.
    Returns list of ConnectedPage objects with access level info.
    
    User has access to:
    1. Channels they own (ConnectedPage.user_id == user_id)
    2. Channels in teams they're a member of (with ChannelAccess permission)
    """
    # Get all teams user is a member of
    team_memberships = TeamMember.query.filter_by(user_id=user_id).all()
    team_member_ids = [m.id for m in team_memberships]
    
    if not team_member_ids:
        # User is not a member of any teams, only their own pages
        return []
    
    # Get all channel access records for this user
    channel_accesses = ChannelAccess.query.filter(
        ChannelAccess.team_member_id.in_(team_member_ids)
    ).all()
    
    if not channel_accesses:
        return []
    
    # Get the connected page IDs from channel access records
    channel_ids = [ca.channel_id for ca in channel_accesses]
    
    # Get the actual connected pages
    pages = ConnectedPage.query.filter(
        ConnectedPage.id.in_(channel_ids),
        ConnectedPage.is_active == True
    ).all()
    
    # Attach access level info to each page
    access_map = {ca.channel_id: ca.access_level for ca in channel_accesses}
    for page in pages:
        page._team_access_level = access_map.get(page.id, 'none')
    
    return pages


def user_can_access_page(user_id, page):
    """Return True if the user owns the page or has team access."""

    if not page:
        return False
    if page.user_id == user_id:
        return True
    team_pages = get_accessible_team_channels(user_id)
    return any(team_page.id == page.id for team_page in team_pages)


# ======================== PAGE IMPORT HELPERS ========================


def get_latest_import_job_for_page(page_id):
    if not page_id:
        return None
    return PageImportJob.query.filter_by(page_id=page_id).order_by(PageImportJob.created_at.desc()).first()


def build_import_status_meta(job):
    if not job:
        return {
            'state': 'idle',
            'label': 'History not imported',
            'detail': 'Import recent posts to backfill your calendar.',
            'posts_imported': 0,
            'last_run': None
        }

    state = (job.status or 'pending').lower()
    label_map = {
        'pending': 'Queued for import',
        'running': 'Import in progress',
        'completed': 'History synced',
        'failed': 'Import failed'
    }
    detail_map = {
        'pending': 'We will start pulling posts shortly.',
        'running': 'Fetching historical posts in the background.',
        'completed': f"Imported {job.posts_imported} post(s).",
        'failed': job.error_message or 'Retry the import to try again.'
    }

    return {
        'state': state,
        'label': label_map.get(state, state.title()),
        'detail': detail_map.get(state, ''),
        'posts_imported': job.posts_imported,
        'last_run': job.finished_at.isoformat() + 'Z' if job.finished_at else None
    }


def enqueue_page_import_job(page, user_id, scope='auto-connect', max_posts=None, auto_start=True):
    if not page or not page.page_access_token:
        return None

    job = PageImportJob(
        page_id=page.id,
        user_id=user_id,
        platform=page.platform,
        scope=scope,
        status='pending',
        max_posts=max_posts or PAGE_HISTORY_IMPORT_LIMIT
    )
    db.session.add(job)
    db.session.commit()

    if auto_start:
        start_page_import_job(job.id)

    return job


def start_page_import_job(job_id):
    if not job_id:
        return
    threading.Thread(target=_run_page_import_job, args=(job_id,), daemon=True).start()


def _run_page_import_job(job_id):
    with app.app_context():
        job = PageImportJob.query.get(job_id)
        if not job:
            return
        if job.status == 'running':
            return

        job.status = 'running'
        job.started_at = datetime.utcnow()
        job.error_message = None
        db.session.commit()

        page = ConnectedPage.query.get(job.page_id)
        if not page:
            job.status = 'failed'
            job.error_message = 'Connected page not found'
            job.finished_at = datetime.utcnow()
            db.session.commit()
            return

        if not page.page_access_token:
            job.status = 'failed'
            job.error_message = 'Page access token missing'
            job.finished_at = datetime.utcnow()
            db.session.commit()
            return

        try:
            platform_name = (page.platform or '').lower()
            max_posts = job.max_posts or PAGE_HISTORY_IMPORT_LIMIT
            if platform_name == 'facebook':
                posts_data = get_facebook_page_posts(page.platform_page_id, page.page_access_token, max_posts=max_posts)
                job.total_posts_found = len(posts_data)
                posts_imported = store_facebook_posts_to_db(job.user_id, page, posts_data)
            elif platform_name == 'tiktok':
                posts_data = list_tiktok_posts(page.platform_page_id, page.page_access_token, max_pages=1)
                job.total_posts_found = len(posts_data)
                posts_imported = store_tiktok_posts_to_db(job.user_id, page, posts_data)
            else:
                raise ValueError(f"Platform '{platform_name}' not supported for history import")

            job.posts_imported = posts_imported
            job.status = 'completed'
        except Exception as exc:
            job.status = 'failed'
            job.error_message = str(exc)
        finally:
            job.finished_at = datetime.utcnow()
            db.session.commit()


# ======================== ANALYTICS REFRESH HELPERS ========================


def _safe_count_posts_pending_refresh(user_id, page_id=None):
    try:
        from tasks import count_posts_needing_refresh as _count_posts
        return _count_posts(user_id, page_id)
    except Exception as exc:
        logger.warning(f"Unable to count posts needing refresh: {exc}")
        return 0


def get_latest_analytics_job(user_id, page_id=None):
    if not user_id:
        return None
    query = AnalyticsRefreshJob.query.filter(AnalyticsRefreshJob.user_id == user_id)
    if page_id:
        query = query.filter(AnalyticsRefreshJob.page_id == page_id)
    else:
        query = query.filter(AnalyticsRefreshJob.page_id.is_(None))
    return query.order_by(AnalyticsRefreshJob.created_at.desc()).first()


def enqueue_analytics_refresh_job(user_id, page_id=None, scope='manual', batch_size=None, auto_start=True):
    if not user_id:
        return None

    job = AnalyticsRefreshJob(
        user_id=user_id,
        page_id=page_id,
        scope=scope,
        status='pending',
        batch_size=batch_size or ANALYTICS_REFRESH_DEFAULT_BATCH,
        total_posts=0,
        processed=0,
        failed=0,
        skipped=0
    )
    db.session.add(job)
    db.session.commit()

    if auto_start:
        start_analytics_refresh_job(job.id)

    return job


def start_analytics_refresh_job(job_id):
    if not job_id:
        return
    threading.Thread(target=_run_analytics_refresh_job, args=(job_id,), daemon=True).start()


def _run_analytics_refresh_job(job_id):
    with app.app_context():
        job = AnalyticsRefreshJob.query.get(job_id)
        if not job:
            return
        if job.status == 'running':
            return

        job.status = 'running'
        job.started_at = datetime.utcnow()
        job.error_message = None
        job.last_progress_at = datetime.utcnow()
        db.session.commit()

        try:
            from tasks import refresh_all_post_analytics

            total_posts = _safe_count_posts_pending_refresh(job.user_id, job.page_id)
            job.total_posts = total_posts
            job.processed = 0
            job.failed = 0
            job.skipped = 0
            db.session.commit()

            if total_posts == 0:
                job.status = 'completed'
                job.finished_at = datetime.utcnow()
                db.session.commit()
                return

            remaining = total_posts
            batch_size = job.batch_size or ANALYTICS_REFRESH_DEFAULT_BATCH

            while remaining > 0:
                result = refresh_all_post_analytics(job.user_id, limit=batch_size, page_id=job.page_id) or {}
                processed = result.get('processed', 0)
                job.processed += processed
                job.failed += result.get('failed', 0)
                job.skipped += result.get('skipped', 0)
                job.last_progress_at = datetime.utcnow()
                remaining = max(0, remaining - processed)
                db.session.commit()

                if processed == 0:
                    break

            final_remaining = _safe_count_posts_pending_refresh(job.user_id, job.page_id)
            job.status = 'completed' if final_remaining == 0 else 'partial'
        except Exception as exc:
            job.status = 'failed'
            job.error_message = str(exc)
        finally:
            job.finished_at = datetime.utcnow()
            db.session.commit()


# ======================== APPLICATION INITIALIZATION ========================

# OAuth Configuration
FACEBOOK_APP_ID = os.getenv('FACEBOOK_APP_ID', '')
FACEBOOK_APP_SECRET = os.getenv('FACEBOOK_APP_SECRET', '')
FACEBOOK_OAUTH_REDIRECT_URI = os.getenv('FACEBOOK_OAUTH_REDIRECT_URI', 'http://localhost:5000/oauth/facebook/callback')

# OAuth URLs
FACEBOOK_OAUTH_AUTH_URL = "https://www.facebook.com/v18.0/dialog/oauth"
FACEBOOK_OAUTH_TOKEN_URL = "https://graph.facebook.com/v18.0/oauth/access_token"
FACEBOOK_PAGES_API_URL = "https://graph.facebook.com/v18.0/me/accounts"


# ======================== HELPER FUNCTIONS ========================

def get_facebook_oauth_url(state=None):
    """Generate Facebook OAuth URL for user authentication"""
    if not state:
        state = secrets.token_urlsafe(32)
    
    params = {
        'client_id': FACEBOOK_APP_ID,
        'redirect_uri': FACEBOOK_OAUTH_REDIRECT_URI,
        'scope': 'pages_manage_posts,pages_read_engagement,pages_read_user_content,pages_manage_metadata,read_insights,pages_show_list',
        'state': state,
        'response_type': 'code'
    }
    
    query_string = '&'.join([f'{k}={v}' for k, v in params.items()])
    return f"{FACEBOOK_OAUTH_AUTH_URL}?{query_string}", state


def exchange_facebook_code_for_token(code):
    """Exchange authorization code for access token"""
    print("[OAUTH] ====== exchange_facebook_code_for_token START ======")
    print(f"[OAUTH] Code: {code[:15]}...")
    data = {
        'client_id': FACEBOOK_APP_ID,
        'client_secret': FACEBOOK_APP_SECRET,
        'redirect_uri': FACEBOOK_OAUTH_REDIRECT_URI,
        'code': code
    }
    print(f"[OAUTH] Using redirect_uri: {FACEBOOK_OAUTH_REDIRECT_URI}")
    
    try:
        print(f"[OAUTH] Posting to: {FACEBOOK_OAUTH_TOKEN_URL}")
        response = requests.post(FACEBOOK_OAUTH_TOKEN_URL, data=data, timeout=10)
        print(f"[OAUTH] Response status: {response.status_code}")
        response.raise_for_status()
        result = response.json()
        print(f"[OAUTH] Token response: {result}")
        print("[OAUTH] ====== exchange_facebook_code_for_token END ======")
        return result
    except requests.RequestException as e:
        print(f"[OAUTH] Error exchanging code for token: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[OAUTH] Response text: {e.response.text}")
        print("[OAUTH] ====== exchange_facebook_code_for_token END (ERROR) ======")
        return None


def get_facebook_pages(access_token):
    """Fetch user's Facebook pages using the access token"""
    print("[OAUTH] ====== get_facebook_pages START ======")
    print(f"[OAUTH] Token: {access_token[:20]}...")
    try:
        print(f"[OAUTH] Requesting from: {FACEBOOK_PAGES_API_URL}")
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        # Request specific fields including access_token for each page
        params = {
            'fields': 'id,name,username,access_token,picture'
        }
        response = requests.get(FACEBOOK_PAGES_API_URL, headers=headers, params=params, timeout=10)
        print(f"[OAUTH] Response status: {response.status_code}")
        response.raise_for_status()
        result = response.json()
        print(f"[OAUTH] Pages response: {result}")
        print("[OAUTH] ====== get_facebook_pages END ======")
        return result
    except requests.RequestException as e:
        print(f"[OAUTH] Error fetching Facebook pages: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[OAUTH] Response text: {e.response.text}")
        print("[OAUTH] ====== get_facebook_pages END (ERROR) ======")
        return None


def build_absolute_url(path):
    """Return an absolute URL for a given relative/static path."""

    if not path:
        return ''
    if path.startswith('http://') or path.startswith('https://'):
        return path

    base_url = os.getenv('APP_URL') or request.host_url.rstrip('/')
    if not path.startswith('/'):
        path = f'/{path}'
    return f"{base_url}{path}"


def build_platform_view_url(page, platform_post_id):
    """Best-effort permalink for a post on a connected page."""

    if not page or not platform_post_id:
        return None

    platform_post_id = str(platform_post_id)
    if platform_post_id.startswith('http://') or platform_post_id.startswith('https://'):
        return platform_post_id

    platform_name = (page.platform or '').lower()
    username = (page.page_username or '').lstrip('@') if page.page_username else None
    page_identifier = username or page.platform_page_id

    if platform_name == 'facebook':
        parts = platform_post_id.split('_', 1)
        post_id = parts[1] if len(parts) == 2 else platform_post_id
        page_part = page_identifier or (parts[0] if len(parts) == 2 else None)
        if page_part and post_id:
            return f"https://www.facebook.com/{page_part}/posts/{post_id}"
        return f"https://www.facebook.com/{platform_post_id}"

    if platform_name == 'instagram':
        # Graph API media IDs are not shortcodes, but some imports may already include valid slugs
        if len(platform_post_id) <= 15 and platform_post_id.isalnum():
            return f"https://www.instagram.com/p/{platform_post_id}/"
        # Fallback to profile link if we cannot construct a permalink
        if username:
            return f"https://www.instagram.com/{username}/"

    if platform_name == 'tiktok' and page_identifier:
        handle = str(page_identifier).lstrip('@')
        return f"https://www.tiktok.com/@{handle}/video/{platform_post_id}"

    return None


def normalize_post_status_value(status):
    """Map legacy/internal statuses to the variants our UI understands."""
    normalized = (status or 'draft').lower()
    if normalized == 'publishing':
        return 'scheduled'
    if normalized == 'published':
        return 'sent'
    return normalized


def get_facebook_page_posts(page_id, access_token, max_posts=None):
    """Fetch historical posts from a Facebook page with optional limit."""
    print(f"[POSTS] Fetching ALL posts for page {page_id}")
    all_posts = []
    try:
        # Try different field combinations, prioritizing useful data
        # Start with minimal, but ensure we get at least created_time for useful posts
        field_sets = [
            'id,created_time',
            'id,message,created_time'
        ]
        
        endpoints = [
            f"https://graph.facebook.com/v18.0/{page_id}/feed",
            f"https://graph.facebook.com/v18.0/{page_id}/posts"
        ]
        
        url = None
        working_fields = None
        
        # Try combinations
        for endpoint_url in endpoints:
            for fields in field_sets:
                print(f"[POSTS] Testing {endpoint_url} with fields: {fields}")
                params = {
                    'fields': fields,
                    'access_token': access_token,
                    'limit': 1  # Just test one to see if it works
                }
                
                try:
                    test_response = requests.get(endpoint_url, params=params, timeout=15)
                    if test_response.status_code == 200:
                        print(f"[POSTS] [OK] Success with endpoint: {endpoint_url}")
                        print(f"[POSTS] [OK] Using fields: {fields}")
                        url = endpoint_url
                        working_fields = fields
                        break
                    else:
                        error_msg = "Unknown error"
                        try:
                            error_data = test_response.json()
                            if 'error' in error_data:
                                error_msg = error_data['error'].get('message', str(error_data['error']))
                                print(f"[POSTS] API Error: {error_data['error']}")
                        except:
                            error_msg = test_response.text[:500] if test_response.text else "No error details"
                        print(f"[POSTS] [FAILED] Failed with {test_response.status_code}: {error_msg}")
                except Exception as e:
                    print(f"[POSTS] [FAILED] Exception: {e}")
                    continue
            
            if url:
                break
        
        if not url or not working_fields:
            print(f"[POSTS] Could not find working endpoint/fields combination for page {page_id}")
            print(f"[POSTS] Trying simple endpoint without fields parameter...")
            # Last resort: try without specifying fields
            try:
                simple_url = f"https://graph.facebook.com/v18.0/{page_id}/posts"
                params = {
                    'access_token': access_token,
                    'limit': 1
                }
                test_response = requests.get(simple_url, params=params, timeout=15)
                if test_response.status_code == 200:
                    print(f"[POSTS] [OK] Success with simple endpoint (no fields specified)")
                    result = test_response.json()
                    print(f"[POSTS] Default fields returned: {list(result.get('data', [{}])[0].keys()) if result.get('data') else 'N/A'}")
                    url = simple_url
                    working_fields = None  # Let API return default fields
                else:
                    error_data = test_response.json()
                    error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                    print(f"[POSTS] [FAILED] Even simple endpoint failed: {error_msg}")
                    print(f"[POSTS] Full error: {error_data.get('error', {})}")
                    return all_posts
            except Exception as e:
                print(f"[POSTS] [FAILED] Exception on simple endpoint: {e}")
                return all_posts
        
        # Now fetch all posts with the working combination
        print(f"[POSTS] Fetching all posts with working fields: {working_fields}")
        params = {
            'access_token': access_token,
            'limit': 100
        }
        if working_fields:
            params['fields'] = working_fields
        
        first_request = True
        while url:
            if first_request:
                response = requests.get(url, params=params, timeout=15)
                first_request = False
            else:
                response = requests.get(url, timeout=15)
            
            if response.status_code != 200:
                error_msg = response.text
                try:
                    error_data = response.json()
                    if 'error' in error_data:
                        error_dict = error_data['error']
                        error_msg = error_dict.get('message', error_msg)
                        print(f"[POSTS] Full error details: {error_dict}")
                except:
                    pass
                print(f"[POSTS] Error ({response.status_code}): {error_msg}")
                break
            
            result = response.json()
            posts_data = result.get('data', [])
            all_posts.extend(posts_data)
            print(f"[POSTS] Retrieved {len(posts_data)} posts in this batch (total: {len(all_posts)})")

            if max_posts and len(all_posts) >= max_posts:
                all_posts = all_posts[:max_posts]
                print(f"[POSTS] Reached max_posts limit ({max_posts}), stopping pagination")
                break
            
            paging = result.get('paging', {})
            url = paging.get('next')
        
        print(f"[POSTS] Total posts retrieved: {len(all_posts)}")
        return all_posts
        
    except Exception as e:
        print(f"[POSTS] Error: {e}")
        import traceback
        traceback.print_exc()
        return []


FACEBOOK_ATTACHMENT_FIELD_BLOCK = 'attachments{media_type,media,url,subattachments{media,url},target}'


def _strip_facebook_attachment_fields(field_str):
    cleaned = field_str.replace(f",{FACEBOOK_ATTACHMENT_FIELD_BLOCK}", '')
    cleaned = cleaned.replace(f"{FACEBOOK_ATTACHMENT_FIELD_BLOCK},", '')
    cleaned = cleaned.replace(FACEBOOK_ATTACHMENT_FIELD_BLOCK, '')
    cleaned = cleaned.strip(', ')
    if not cleaned:
        return ''
    parts = [segment.strip() for segment in cleaned.split(',') if segment.strip()]
    return ','.join(parts)


def _is_attachment_deprecation_error(error_json):
    error_block = (error_json or {}).get('error') or {}
    message = (error_block.get('message') or '').lower()
    return error_block.get('code') == 12 and 'deprecate_post_aggregated_fields_for_attachement' in message


def _fetch_facebook_post_fields(post_id, access_token, fields):
    """Fetch specific fields for a Facebook post ID, returning JSON dict."""
    if not post_id or not access_token or not fields:
        return {}

    url = f"https://graph.facebook.com/v18.0/{post_id}"
    attempted_without_attachments = False
    current_fields = fields

    while current_fields:
        params = {
            'fields': current_fields,
            'access_token': access_token
        }

        try:
            response = requests.get(url, params=params, timeout=10)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[POSTS] Error fetching post fields for {post_id}: {exc}")
            return {}

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                return {}

        try:
            error_json = response.json()
        except ValueError:
            error_json = {'message': response.text}

        print(f"[POSTS] Error fetching post fields for {post_id}: {error_json}")

        if (not attempted_without_attachments and 'attachments{' in current_fields \
                and _is_attachment_deprecation_error(error_json)):
            attempted_without_attachments = True
            current_fields = _strip_facebook_attachment_fields(current_fields)
            print("[POSTS] Facebook deprecated aggregated attachments; retrying without attachment fields")
            continue

        return {}

    return {}


def _extract_media_from_facebook_post(post_data):
    """Return a list of media payloads extracted from a Facebook post response."""
    media_items = []
    if not post_data:
        return media_items

    attachments = ((post_data.get('attachments') or {}).get('data')) or []
    seen_urls = set()

    def handle_attachment(attachment):
        if not attachment:
            return

        subattachments = (attachment.get('subattachments') or {}).get('data') or []
        for sub in subattachments:
            handle_attachment(sub)

        media_type = (attachment.get('media_type') or attachment.get('type') or '').lower()
        media_block = attachment.get('media') or {}
        image_block = media_block.get('image') or {}

        candidates = [
            media_block.get('source'),
            image_block.get('src'),
            image_block.get('url'),
            attachment.get('media_url'),
            attachment.get('url'),
            (attachment.get('target') or {}).get('url')
        ]
        media_url = next((candidate for candidate in candidates if candidate), None)
        if media_url and media_url not in seen_urls:
            normalized_type = 'video' if 'video' in media_type else 'image'
            media_items.append({'url': media_url, 'type': normalized_type})
            seen_urls.add(media_url)

    for attachment in attachments:
        handle_attachment(attachment)

    if not media_items:
        full_picture = post_data.get('full_picture')
        if full_picture and full_picture not in seen_urls:
            media_items.append({'url': full_picture, 'type': 'image'})

    return media_items


def _attach_media_to_post(post, media_items):
    """Create PostMedia rows for the provided Post, skipping duplicates."""
    if not post or not media_items:
        return 0

    existing_urls = {media.media_url for media in getattr(post, 'media', []) or []}
    attached = 0

    for media in media_items:
        media_url = media.get('url')
        if not media_url or media_url in existing_urls:
            continue
        db.session.add(PostMedia(
            post_id=post.id,
            media_url=media_url,
            media_type=media.get('type') or 'image'
        ))
        existing_urls.add(media_url)
        attached += 1

    return attached


def store_facebook_posts_to_db(user_id, connected_page, posts_data):
    """Store Facebook posts to database, avoiding duplicates"""
    print(f"[POSTS] Storing {len(posts_data)} posts to database for page {connected_page.page_name}")
    posts_added = 0
    
    for post_data in posts_data:
        post_id = post_data.get('id', '')
        print(f"[POSTS] Processing post: {post_id}")
        print(f"[POSTS] Post data keys: {list(post_data.keys())}")

        access_token = connected_page.page_access_token
        detail_data = {}
        if post_id and access_token:
            detail_fields = (
                'message,caption,permalink_url,full_picture,'
                'attachments{media_type,media,url,subattachments{media,url},target}'
            )
            detail_data = _fetch_facebook_post_fields(post_id, access_token, detail_fields)
            for key in ['message', 'caption', 'permalink_url', 'full_picture', 'attachments']:
                if not post_data.get(key) and detail_data.get(key):
                    post_data[key] = detail_data[key]

        message = post_data.get('message') or post_data.get('caption')
        if not message and detail_data.get('message'):
            message = detail_data.get('message')
            print(f"[POSTS] Fetched message separately: {message[:50]}...")

        media_payload = _extract_media_from_facebook_post(post_data)
        
        # Check if post already exists by checking both Facebook ID and user
        existing_post = Post.query.filter_by(
            user_id=user_id,
            title=post_id
        ).first()
        
        if existing_post:
            print(f"[POSTS] → Post already exists in DB (Post ID: {existing_post.id}, Caption: '{existing_post.caption[:30] if existing_post.caption else 'No caption'}...', Status: {existing_post.status})")
            # Check if association exists, if not create it
            existing_assoc = PostPageAssociation.query.filter_by(
                post_id=existing_post.id,
                page_id=connected_page.id
            ).first()
            if not existing_assoc:
                print(f"[POSTS] → Creating missing association for existing post")
                association = PostPageAssociation(
                    post_id=existing_post.id,
                    page_id=connected_page.id,
                    platform_post_id=post_id,
                    status='sent'
                )
                db.session.add(association)
                posts_added += 1
            else:
                print(f"[POSTS] → Association already exists (Assoc ID: {existing_assoc.id}, Platform Post ID: {existing_assoc.platform_post_id})")

            attached = _attach_media_to_post(existing_post, media_payload)
            if attached:
                print(f"[POSTS] → Added {attached} media attachment(s) to existing post")
            continue
        
        try:
            # Parse Facebook's created_time
            created_time = post_data.get('created_time', '')
            if created_time:
                sent_time = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            else:
                sent_time = datetime.utcnow()
            
            # Create post from Facebook data
            # Use fetched message or fallback to generic text
            content = message or f'Posted on {connected_page.page_name}'
            post = Post(
                user_id=user_id,
                title=post_id,
                content=content,
                caption=message or '',
                status='sent',
                sent_time=sent_time
            )
            db.session.add(post)
            db.session.flush()
            
            # Create page association
            association = PostPageAssociation(
                post_id=post.id,
                page_id=connected_page.id,
                platform_post_id=post_id,
                status='sent'
            )
            db.session.add(association)
            db.session.flush()  # Ensure association is added before continuing

            attached = _attach_media_to_post(post, media_payload)
            if attached:
                print(f"[POSTS] → Captured {attached} media attachment(s)")
            posts_added += 1
            print(f"[POSTS] → ✓ Added to DB with association (sent_time: {sent_time})")
        except Exception as e:
            print(f"[POSTS] → ✗ Error storing post: {e}")
            import traceback
            traceback.print_exc()
            db.session.rollback()  # Rollback on error to prevent partial commits
            continue
    
    if posts_added > 0:
        try:
            db.session.commit()
            print(f"[POSTS] Successfully committed {posts_added} new posts/associations to database")
        except Exception as e:
            print(f"[POSTS] → ✗ Error committing to database: {e}")
            db.session.rollback()
            return 0
    else:
        print(f"[POSTS] No new posts added (all duplicates or errors)")
    
    return posts_added


def normalize_oauth_accounts(platform, raw_accounts, default_access_token=None):
    """Normalize provider-specific account payloads for the select-accounts UI."""
    normalized = []
    for account in raw_accounts or []:
        if platform == 'tiktok':
            account_id = account.get('id') or account.get('open_id')
            if not account_id:
                continue
            normalized.append({
                'id': account_id,
                'name': account.get('display_name') or account.get('username') or 'TikTok Account',
                'username': account.get('username') or account.get('display_name'),
                'access_token': account.get('access_token') or default_access_token,
                'avatar_url': account.get('avatar_url') or account.get('avatar_url_100'),
            })
        else:
            # Facebook/Instagram payload already matches what the template expects
            normalized.append(account)
    return normalized


def store_tiktok_posts_to_db(user_id, connected_page, posts_data):
    """Store TikTok posts to database, mirroring the Facebook helper."""
    print(f"[TIKTOK] Storing {len(posts_data)} posts for {connected_page.page_name}")
    posts_added = 0

    for post_data in posts_data:
        platform_post_id = post_data.get('id') or post_data.get('video_id')
        if not platform_post_id:
            continue

        existing_post = Post.query.filter_by(user_id=user_id, title=platform_post_id).first()
        if existing_post:
            assoc = PostPageAssociation.query.filter_by(
                post_id=existing_post.id,
                page_id=connected_page.id
            ).first()
            if not assoc:
                db.session.add(PostPageAssociation(
                    post_id=existing_post.id,
                    page_id=connected_page.id,
                    platform_post_id=platform_post_id,
                    status='sent'
                ))
                posts_added += 1
            continue

        created_time = post_data.get('create_time') or post_data.get('publish_time')
        if isinstance(created_time, (int, float)):
            sent_time = datetime.utcfromtimestamp(created_time)
        elif isinstance(created_time, str):
            try:
                sent_time = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            except ValueError:
                sent_time = datetime.utcnow()
        else:
            sent_time = datetime.utcnow()

        caption = post_data.get('description') or post_data.get('caption')
        if isinstance(caption, dict):
            caption = caption.get('text')

        post = Post(
            user_id=user_id,
            title=platform_post_id,
            content=caption or f'TikTok video from {connected_page.page_name}',
            caption=caption or '',
            status='sent',
            sent_time=sent_time
        )
        db.session.add(post)
        db.session.flush()

        db.session.add(PostPageAssociation(
            post_id=post.id,
            page_id=connected_page.id,
            platform_post_id=platform_post_id,
            status='sent'
        ))
        posts_added += 1

    if posts_added:
        try:
            db.session.commit()
        except Exception as exc:
            print(f"[TIKTOK] Error committing posts: {exc}")
            db.session.rollback()
            return 0
    else:
        print("[TIKTOK] No new posts stored (duplicates)")

    return posts_added


def init_db():
    """Initialize database"""
    try:
        with app.app_context():
            # Drop all tables first to ensure schema updates are applied
            # db.drop_all()
            # Create all tables with updated schema
            db.create_all()
            print("[DB] Database tables created successfully")
    except OSError as e:
        print(f"[DB] Error creating database tables: {e}")


def send_invitation_email(recipient_email, recipient_name, sender_name, organization_name, invitation_token, app_url):
    """Send invitation email using Sendgrid"""
    try:
        if not SENDGRID_API_KEY:
            print(f"[EMAIL] Sendgrid API key not configured. Skipping email to {recipient_email}")
            return False
        
        # Create invitation accept link
        accept_link = f"{app_url}/team/accept-invite/{invitation_token}"
        
        subject = f"{sender_name} invited you to {organization_name} on Postly"
        
        html_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background-color: #1877f2; padding: 20px; text-align: center; border-radius: 8px 8px 0 0;">
                <h1 style="color: white; margin: 0;">Postly</h1>
            </div>
            <div style="background-color: #f8f9fa; padding: 40px; border-radius: 0 0 8px 8px;">
                <p style="color: #050505; font-size: 16px;">Hi {recipient_name},</p>
                
                <p style="color: #65676b; font-size: 14px; line-height: 1.6;">
                    <strong>{sender_name}</strong> has invited you to join <strong>{organization_name}</strong> on Postly, 
                    a comprehensive social media management platform.
                </p>
                
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{accept_link}" style="background-color: #1877f2; color: white; padding: 12px 30px; 
                       text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
                        Accept Invitation
                    </a>
                </div>
                
                <p style="color: #65676b; font-size: 12px; line-height: 1.6;">
                    Or copy and paste this link in your browser:<br>
                    <a href="{accept_link}" style="color: #1877f2; text-decoration: none; word-break: break-all;">
                        {accept_link}
                    </a>
                </p>
                
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 30px 0;">
                
                <p style="color: #99a0a8; font-size: 12px;">
                    This invitation will expire in 7 days.
                </p>
            </div>
        </div>
        """
        
        message = Mail(
            from_email=SENDGRID_FROM_EMAIL,
            to_emails=recipient_email,
            subject=subject,
            html_content=html_content
        )
        
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        
        print(f"[EMAIL] Invitation sent to {recipient_email}")
        return True
        
    except Exception as e:
        print(f"[EMAIL] Error sending invitation to {recipient_email}: {str(e)}")
        return False


# ======================== ROUTES ========================

@app.route('/', methods=['GET'])
def index():
    """Landing page"""
    return render_template('index.html')


@app.route('/uploads/<path:filename>')
def serve_upload(filename):
    """Serve uploaded media files"""
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    """User signup"""
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        first_name = request.form.get('first_name', '').strip()
        last_name = request.form.get('last_name', '').strip()
        
        # Validation
        if not email or not username or not password:
            flash('Email, username, and password are required', 'danger')
            return redirect(url_for('signup'))
        
        if password != confirm_password:
            flash('Passwords do not match', 'danger')
            return redirect(url_for('signup'))
        
        if len(password) < 8:
            flash('Password must be at least 8 characters', 'danger')
            return redirect(url_for('signup'))
        
        # Check if user exists
        if User.query.filter_by(email=email).first():
            flash('Email already registered', 'danger')
            return redirect(url_for('signup'))
        
        if User.query.filter_by(username=username).first():
            flash('Username already taken', 'danger')
            return redirect(url_for('signup'))
        
        try:
            # Create user
            # TEMPORARILY: All email verification commented for testing
            # verification_code = secrets.token_urlsafe(32)
            user = User(
                email=email,
                username=username,
                password_hash=PasswordHelper.hash_password(password),
                first_name=first_name,
                last_name=last_name,
                # verification_code=verification_code,
                # verification_code_expires=datetime.utcnow() + timedelta(days=1),
                is_verified=True  # Auto-verify for testing
            )
            db.session.add(user)
            db.session.flush()  # Get user.id without committing
            
            # Create default personal organization for user
            default_team = Team(
                name=f"{username}'s Organization",
                owner_id=user.id
            )
            db.session.add(default_team)
            db.session.commit()
            
            # Send verification email
            # TEMPORARILY COMMENTED FOR TESTING
            # app_url = os.getenv('APP_URL', 'http://localhost:5000')
            # EmailService.send_verification_email(email, verification_code, app_url)
            
            flash('Signup successful! You can now log in.', 'success')
            return redirect(url_for('login'))
        
        except OSError as e:
            db.session.rollback()
            print(f"[SIGNUP] Error: {e}")
            flash('An error occurred during signup', 'danger')
            return redirect(url_for('signup'))
    
    return render_template('auth/signup.html')


@app.route('/verify-email/<verification_code>', methods=['GET'])
def verify_email(_):
    """Verify email address - TEMPORARILY DISABLED FOR TESTING"""
    # TEMPORARILY: All email verification commented out
    flash('Email verification is disabled for testing', 'info')
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    if request.method == 'POST':
        email_or_username = request.form.get('email_or_username', '').strip()
        password = request.form.get('password', '')
        
        if not email_or_username or not password:
            flash('Email/Username and password are required', 'danger')
            return redirect(url_for('login'))
        
        # Find user by email or username
        user = User.query.filter(
            (User.email == email_or_username) | (User.username == email_or_username)
        ).first()
        
        if not user:
            flash('Invalid email/username or password', 'danger')
            return redirect(url_for('login'))
        
        # TEMPORARILY: Email verification check disabled for testing
        # if not user.is_verified:
        #     flash('Please verify your email before logging in', 'warning')
        #     return redirect(url_for('login'))
        
        if not PasswordHelper.verify_password(password, user.password_hash):
            flash('Invalid email/username or password', 'danger')
            return redirect(url_for('login'))
        
        # Update last login
        user.last_login = datetime.utcnow()
        db.session.commit()
        
        # Set session
        session['user_id'] = user.id
        session['username'] = user.username
        session['email'] = user.email
        session.permanent = True
        app.permanent_session_lifetime = timedelta(days=30)
        
        flash(f'Welcome back, {user.first_name or user.username}!', 'success')
        return redirect(url_for('index'))
    
    return render_template('auth/login.html')


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    """User logout"""
    session.clear()
    flash('You have been logged out', 'success')
    return redirect(url_for('index'))


# ======================== DASHBOARD AND CALENDAR ROUTES ========================

@app.route('/publish', methods=['GET'])
@login_required
def publish():
    """Calendar/publish dashboard for logged-in users"""
    user_id = session.get('user_id')
    
    # Get user's own connected pages
    owned_pages = ConnectedPage.query.filter_by(user_id=user_id, is_active=True).all()
    
    # Get team pages user has access to
    team_pages = get_accessible_team_channels(user_id)
    
    # Combine all pages (no duplicates)
    all_pages = owned_pages + [p for p in team_pages if p.id not in set(p.id for p in owned_pages)]

    # Attach latest import metadata for UI badges
    page_ids = [p.id for p in all_pages]
    page_import_status = {}
    latest_lookup = {}
    if page_ids:
        has_import_table = False
        try:
            inspector = inspect(db.engine)
            has_import_table = inspector.has_table('page_import_jobs')
        except Exception as exc:
            print(f"[PUBLISH] Unable to check page_import_jobs table existence: {exc}")

        if has_import_table:
            latest_jobs_subquery = db.session.query(
                PageImportJob.page_id,
                func.max(PageImportJob.created_at).label('latest_created')
            ).filter(PageImportJob.page_id.in_(page_ids)).group_by(PageImportJob.page_id).subquery()

            latest_jobs = db.session.query(PageImportJob).join(
                latest_jobs_subquery,
                (PageImportJob.page_id == latest_jobs_subquery.c.page_id) &
                (PageImportJob.created_at == latest_jobs_subquery.c.latest_created)
            ).all()
            latest_lookup = {job.page_id: job for job in latest_jobs}
        else:
            print("[PUBLISH] Skipping page import metadata lookup because table is missing")

    for page in all_pages:
        meta = build_import_status_meta(latest_lookup.get(page.id))
        page._import_meta = meta
        page_import_status[page.id] = meta
    
    # Group pages by platform for template compatibility
    pages_by_platform = {}
    for page in all_pages:
        if page.platform not in pages_by_platform:
            pages_by_platform[page.platform] = []
        pages_by_platform[page.platform].append(page)
    
    # Group owned pages by platform (for "My Pages" section)
    owned_by_platform = {}
    for page in owned_pages:
        if page.platform not in owned_by_platform:
            owned_by_platform[page.platform] = []
        owned_by_platform[page.platform].append(page)
    
    # Group team pages by platform (for "Team Pages" section)
    team_by_platform = {}
    for page in team_pages:
        if page.platform not in team_by_platform:
            team_by_platform[page.platform] = []
        team_by_platform[page.platform].append(page)
    
    # Get all posts published to pages the user has access to (not just their own)
    # This allows team members to see all posts on shared pages they have access to
    accessible_page_ids = [p.id for p in all_pages]
    
    # Get all posts that were published to any of these pages
    all_accessible_posts = db.session.query(Post).join(
        PostPageAssociation, Post.id == PostPageAssociation.post_id
    ).filter(
        PostPageAssociation.page_id.in_(accessible_page_ids),
        Post.status != 'draft'  # Don't show drafts in calendar (except own drafts if needed)
    ).order_by(Post.scheduled_time.desc()).distinct().all()
    
    return render_template('dashboard/publish.html', 
                         pages_by_platform=pages_by_platform,
                         owned_by_platform=owned_by_platform,
                         team_by_platform=team_by_platform,
                         connected_pages=all_pages,
                         owned_pages=owned_pages,
                         team_pages=team_pages,
                         user_posts=all_accessible_posts,
                         current_user_id=user_id,
                         page_import_status=page_import_status)


@app.route('/drafts', methods=['GET'])
@login_required
def drafts():
    """Draft approval queue for admins/owners"""
    user_id = session.get('user_id')
    team_id = request.args.get('team_id', type=int)
    can_approve_drafts = bool(_collect_page_ids_for_approval(user_id))
    
    # If team_id is provided, verify user is admin/owner
    if team_id:
        team = Team.query.get(team_id)
        if not team:
            flash('Team not found', 'danger')
            return redirect(url_for('publish'))
        
        if team.owner_id != user_id:
            team_member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first()
            if not team_member or team_member.role != 'admin':
                flash('You don\'t have permission to view draft approval queue', 'danger')
                return redirect(url_for('publish'))
    
    return render_template('dashboard/drafts.html', team_id=team_id, can_approve_drafts=can_approve_drafts)


@app.route('/analyze', methods=['GET'])
@login_required
@monitor_performance('analyze_page')
def analyze():
    """Analytics dashboard for user - Buffer-style with date filtering and pagination"""
    user_id = session.get('user_id')
    user = User.query.get(user_id)
    
    # If user doesn't exist in database (e.g., after db reset), redirect to login
    if not user:
        flash('Session expired. Please log in again.', 'warning')
        return redirect(url_for('login'))
    
    # Get query parameters for filtering (Buffer-style)
    days = request.args.get('days', 30, type=int)  # Default to 30 days
    current_page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)  # 20 posts per page
    
    # Limit days to reasonable values
    if days not in [7, 30, 90]:
        days = 30
    
    # Get user's own connected pages
    owned_pages = ConnectedPage.query.filter_by(user_id=user_id, is_active=True).all()
    
    # Get team pages user has access to
    team_pages = get_accessible_team_channels(user_id)
    
    # Combine all pages
    all_pages = owned_pages + [p for p in team_pages if p.id not in set(p.id for p in owned_pages)]
    
    # Group pages by platform
    pages_by_platform = {}
    for page in all_pages:
        if page.platform not in pages_by_platform:
            pages_by_platform[page.platform] = []
        pages_by_platform[page.platform].append(page)
    
    # Get all posts published to pages the user has access to (not just their own)
    # This allows team members to see all posts on shared pages they have access to
    accessible_page_ids = [p.id for p in all_pages]
    
    # Calculate date range for filtering (Buffer-style)
    from datetime import datetime, timedelta
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)
    
    # Get posts within date range with pagination
    # Use eager loading to reduce database queries
    from sqlalchemy.orm import joinedload
    
    # Get total count for pagination (without loading all data)
    total_posts_count = db.session.query(Post).join(
        PostPageAssociation, Post.id == PostPageAssociation.post_id
    ).filter(
        PostPageAssociation.page_id.in_(accessible_page_ids),
        Post.status.in_(['sent', 'published']),
        Post.sent_time >= start_date,
        Post.sent_time <= end_date
    ).distinct().count()
    
    with PerformanceTimer('query_posts_with_analytics'):
        accessible_posts = db.session.query(Post).join(
            PostPageAssociation, Post.id == PostPageAssociation.post_id
        ).options(
            joinedload(Post.media),
            joinedload(Post.page_associations).joinedload(PostPageAssociation.connected_page),
            joinedload(Post.page_associations).joinedload(PostPageAssociation.analytics)
        ).filter(
            PostPageAssociation.page_id.in_(accessible_page_ids),
            Post.status.in_(['sent', 'published']),
            Post.sent_time >= start_date,  # Date filtering
            Post.sent_time <= end_date
        ).order_by(Post.sent_time.desc()).distinct().limit(per_page).offset((current_page - 1) * per_page).all()

    
    # Convert posts to JSON-serializable format with their page associations and analytics (if available)
    posts_data = []
    for post in accessible_posts:
        media_list = []
        for media in post.media:
            media_list.append({
                'id': media.id,
                'url': media.media_url,
                'type': media.media_type,
                'size': media.file_size,
                'duration': media.duration
            })
        
        # Only show page associations that the user has access to
        page_analytics = []
        for assoc in post.page_associations:
            if assoc.page_id in accessible_page_ids:
                # Use eager loaded analytics data (already fetched via joinedload)
                analytics = assoc.analytics if hasattr(assoc, 'analytics') else None
                
                analytics_data = {}
                if analytics:
                    analytics_data = {
                        'impressions': analytics.impressions or 0,
                        'reach': analytics.reach or 0,
                        'clicks': analytics.clicks or 0,
                        'likes': analytics.likes or 0,
                        'comments': analytics.comments or 0,
                        'shares': analytics.shares or 0,
                        'saves': analytics.saves or 0,
                        'engagement': analytics.engagement or 0,
                        'video_views': analytics.video_views or 0
                    }
                else:
                    # Show post with empty analytics (for historical posts without metrics yet)
                    analytics_data = {
                        'impressions': 0,
                        'reach': 0,
                        'clicks': 0,
                        'likes': 0,
                        'comments': 0,
                        'shares': 0,
                        'saves': 0,
                        'engagement': 0,
                        'video_views': 0
                    }
                
                # Always include this page (whether analytics exists or not)
                page_analytics.append({
                    'id': assoc.id,
                    'page_id': assoc.connected_page.id,
                    'page_name': assoc.connected_page.page_name,
                    'platform': assoc.connected_page.platform,
                    'platform_post_id': assoc.platform_post_id,
                    'status': assoc.status,
                    'analytics': analytics_data,
                    'has_analytics': analytics is not None
                })
        
        # Include post even if it doesn't have analytics (show "No analytics yet" state)
        if page_analytics:
            # Use existing caption/content - no synchronous API calls to avoid blocking page load
            display_caption = post.caption if post.caption and post.caption.strip() else post.content
                
            normalized_status = 'scheduled' if post.status == 'publishing' else post.status
            post_dict = {
                'id': post.id,
                'caption': display_caption,
                'content': post.content,
                'scheduled_time': post.scheduled_time.isoformat() + 'Z' if post.scheduled_time else None,
                'sent_time': post.sent_time.isoformat() + 'Z' if post.sent_time else None,
                'status': normalized_status,
                'media': media_list,
                'pages': page_analytics
            }
            posts_data.append(post_dict)
    
    # Prepare analytics job metadata and auto-queue background refresh if needed
    analytics_job_meta = {
        'stale_post_count': 0,
        'job': None
    }
    stale_post_count = _safe_count_posts_pending_refresh(user_id)
    analytics_job_meta['stale_post_count'] = stale_post_count

    latest_job = get_latest_analytics_job(user_id)
    auto_threshold = max(1, ANALYTICS_REFRESH_AUTO_THRESHOLD)
    if stale_post_count >= auto_threshold and (not latest_job or latest_job.status not in ['pending', 'running']):
        latest_job = enqueue_analytics_refresh_job(
            user_id=user_id,
            scope='auto-on-analyze',
            auto_start=True
        ) or latest_job

    if latest_job:
        analytics_job_meta['job'] = latest_job.to_dict()

    # Calculate page comparison metrics for Overview tab
    page_metrics = {}
    for page in all_pages:
        page_metrics[page.id] = {
            'page_id': page.id,
            'page_name': page.page_name,
            'platform': page.platform,
            'total_posts': 0,
            'total_reach': 0,
            'total_engagement': 0,
            'total_clicks': 0,
            'total_video_views': 0,
            'total_reactions': 0,  # likes + loves + etc
            'avg_engagement_rate': 0,
            'avg_reach_per_post': 0,
            'best_post': None,
            'posts_with_video': 0
        }
    
    # Aggregate analytics by page
    for post in posts_data:
        for page_assoc in post['pages']:
            page_id = page_assoc['page_id']
            if page_id in page_metrics:
                analytics = page_assoc['analytics']
                metrics = page_metrics[page_id]
                
                metrics['total_posts'] += 1
                metrics['total_reach'] += analytics.get('reach', 0)
                metrics['total_engagement'] += analytics.get('engagement', 0)
                metrics['total_clicks'] += analytics.get('clicks', 0)
                metrics['total_video_views'] += analytics.get('video_views', 0)
                metrics['total_reactions'] += (analytics.get('likes', 0) + 
                                              analytics.get('comments', 0) + 
                                              analytics.get('shares', 0))
                
                if analytics.get('video_views', 0) > 0:
                    metrics['posts_with_video'] += 1
                
                # Track best performing post
                engagement_rate = analytics.get('engagement', 0)
                if metrics['best_post'] is None or engagement_rate > metrics['best_post'].get('engagement_rate', 0):
                    metrics['best_post'] = {
                        'post_id': post['id'],
                        'caption': post['caption'][:100] if post['caption'] else 'No caption',
                        'engagement_rate': engagement_rate,
                        'reach': analytics.get('reach', 0),
                        'sent_time': post['sent_time']
                    }
    
    # Calculate averages
    for page_id, metrics in page_metrics.items():
        if metrics['total_posts'] > 0:
            metrics['avg_engagement_rate'] = round(metrics['total_engagement'] / metrics['total_posts'], 2)
            metrics['avg_reach_per_post'] = round(metrics['total_reach'] / metrics['total_posts'])
    
    # Sort pages by performance (engagement rate)
    sorted_pages = sorted(page_metrics.values(), key=lambda x: x['avg_engagement_rate'], reverse=True)
    
    # Calculate platform comparison
    platform_metrics = {}
    for page_metric in page_metrics.values():
        platform = page_metric['platform']
        if platform not in platform_metrics:
            platform_metrics[platform] = {
                'platform': platform,
                'total_posts': 0,
                'total_reach': 0,
                'total_engagement': 0,
                'total_clicks': 0,
                'page_count': 0,
                'avg_engagement_rate': 0
            }
        
        plat_metrics = platform_metrics[platform]
        plat_metrics['total_posts'] += page_metric['total_posts']
        plat_metrics['total_reach'] += page_metric['total_reach']
        plat_metrics['total_engagement'] += page_metric['total_engagement']
        plat_metrics['total_clicks'] += page_metric['total_clicks']
        plat_metrics['page_count'] += 1
    
    # Calculate platform averages
    for platform, metrics in platform_metrics.items():
        if metrics['total_posts'] > 0:
            metrics['avg_engagement_rate'] = round(metrics['total_engagement'] / metrics['total_posts'], 2)
    
    # Buffer-style: Pass pagination and filter info to template
    return render_template('dashboard/analyze.html', 
                         pages_by_platform=pages_by_platform, 
                         user_posts=posts_data,
                         page_metrics=sorted_pages,
                         platform_metrics=list(platform_metrics.values()),
                         current_page=current_page,
                         per_page=per_page,
                         selected_days=days,
                         total_posts_count=total_posts_count,
                         analytics_job_meta=analytics_job_meta)


@app.route('/tiktok/demo', methods=['GET'])
@login_required
def tiktok_demo():
    """Guided hub used to record the TikTok review demo."""

    if not ENABLE_TIKTOK_DEMO:
        abort(404)

    scope_string = os.getenv('TIKTOK_OAUTH_SCOPE', '')
    redirect_uri = os.getenv('TIKTOK_OAUTH_REDIRECT_URI', '')
    redirect_host = redirect_uri.split('/oauth')[0] if redirect_uri else request.host_url.rstrip('/')
    # Static list used by the Jinja template so the reviewer can map each UI card
    # to the corresponding TikTok product the demo intends to highlight.
    product_cards = [
        {
            'name': 'Login Kit',
            'icon': 'fa-plug',
            'description': 'Demonstrates OAuth with PKCE and the tester login flow using the TikTok Login Kit.',
        },
        {
            'name': 'Display API',
            'icon': 'fa-film',
            'description': 'Shows how Postly pulls the creator\'s videos via video.list and renders them in-app.',
        },
        {
            'name': 'Share Kit',
            'icon': 'fa-share-from-square',
            'description': 'Provides a user-initiated Share to TikTok action that opens TikTok\'s upload UI with the selected media.',
        },
        {
            'name': 'Content Posting API',
            'icon': 'fa-upload',
            'description': 'Covers server-to-server publishing through the Content Posting API path already wired into Publish.',
        },
    ]

    return render_template(
        'tiktok/demo.html',
        scope_string=scope_string,
        redirect_host=redirect_host,
        can_publish=tiktok_can_publish(),
        product_cards=product_cards,
        connect_url=url_for('connect_social_platform', platform='tiktok'),
        publish_url=url_for('publish'),
    )


def _get_user_tiktok_pages(user_id):
    """Return TikTok pages the user owns or can access via team membership."""

    owned = ConnectedPage.query.filter_by(
        user_id=user_id,
        platform='tiktok',
        is_active=True,
    ).all()
    team_access = [p for p in get_accessible_team_channels(user_id) if p.platform == 'tiktok']
    pages = {page.id: page for page in owned}
    for page in team_access:
        pages[page.id] = page
    return list(pages.values())


def _build_tiktok_permalink(page, video_id):
    """Construct a TikTok permalink when username + video ID are available."""

    if not video_id or not page.page_username:
        return None
    username = page.page_username.lstrip('@') or page.page_username
    return f"https://www.tiktok.com/@{username}/video/{video_id}"


def _fetch_historical_tiktok_media(user_id, limit_per_page=5):
    """Fetch recent TikTok videos per connected page for demo/history views."""

    media_items = []
    pages = _get_user_tiktok_pages(user_id)
    for page in pages:
        if not page.page_access_token:
            continue

        try:
            # Pull a small batch of posts directly from TikTok so we can mirror
            # the Facebook "history import" behavior inside the demo hub.
            videos = list_tiktok_posts(page.platform_page_id, page.page_access_token, max_pages=1)
        except TikTokApiError as exc:
            print(f"[TIKTOK][HISTORY] Failed pulling videos for page {page.id}: {exc}")
            continue

        if not videos:
            continue

        try:
            stored = store_tiktok_posts_to_db(page.user_id, page, videos)
            if stored:
                print(f"[TIKTOK][HISTORY] Stored {stored} videos for page {page.id}")
        except Exception as exc:  # noqa: BLE001 - best-effort cache
            print(f"[TIKTOK][HISTORY] Failed storing TikTok videos for page {page.id}: {exc}")

        for video in videos[:limit_per_page]:
            video_id = video.get('id') or video.get('video_id')
            caption = video.get('description') or video.get('caption') or ''
            asset_url = (
                video.get('download_url')
                or video.get('video_url')
                or video.get('play_url')
                or ''
            )
            fallback_url = video.get('share_url') or _build_tiktok_permalink(page, video_id)
            media_items.append({
                'post_id': f"{page.id}:{video_id}" if video_id else f"{page.id}:history",
                'caption': caption,
                'media_url': asset_url or fallback_url or '',
                'absolute_url': asset_url or fallback_url or '',
                'permalink': fallback_url,
                'page_name': page.page_name,
                'is_historical': True,
            })

    return media_items


@app.route('/api/tiktok/demo/status', methods=['GET'])
@login_required
def api_tiktok_demo_status():
    if not ENABLE_TIKTOK_DEMO:
        abort(404)
    user_id = session.get('user_id')
    pages = _get_user_tiktok_pages(user_id)

    serialized = []
    for page in pages:
        access_level = 'owner' if page.user_id == user_id else getattr(page, '_team_access_level', 'team')
        serialized.append({
            'id': page.id,
            'page_name': page.page_name,
            'platform_page_id': page.platform_page_id,
            'username': page.page_username,
            'access_level': access_level,
            'connected_at': page.created_at.isoformat() if page.created_at else None,
        })

    return {
        'success': True,
        'pages': serialized,
        'scope': os.getenv('TIKTOK_OAUTH_SCOPE', ''),
        'redirect_host': os.getenv('TIKTOK_OAUTH_REDIRECT_URI', ''),
        'can_publish': tiktok_can_publish(),
    }


@app.route('/api/tiktok/demo/pages/<int:page_id>/posts', methods=['GET'])
@login_required
def api_tiktok_demo_posts(page_id):
    if not ENABLE_TIKTOK_DEMO:
        abort(404)
    user_id = session.get('user_id')
    page = ConnectedPage.query.filter_by(id=page_id, platform='tiktok').first()
    if not page or not user_can_access_page(user_id, page):
        return {'success': False, 'error': 'Page not found or not accessible'}, 404

    if not page.page_access_token:
        return {'success': False, 'error': 'TikTok access token missing. Reconnect the channel first.'}, 400

    try:
        # Always hit TikTok live so reviewers see the fresh API response, then
        # persist what we found so later Share Kit calls can reuse the data.
        videos = list_tiktok_posts(page.platform_page_id, page.page_access_token, max_pages=1)
        if videos:
            store_tiktok_posts_to_db(page.user_id, page, videos)
    except TikTokApiError as exc:
        return {'success': False, 'error': str(exc)}, 400

    serialized = []
    for video in videos[:12]:
        video_id = video.get('id') or video.get('video_id')
        cover = video.get('cover_image_url') or video.get('share_url') or ''
        created_time = video.get('create_time') or video.get('publish_time')
        permalink = _build_tiktok_permalink(page, video_id) or video.get('share_url')
        serialized.append({
            'id': video_id,
            'description': video.get('description') or video.get('caption') or '',
            'cover': cover,
            'created_time': created_time,
            'permalink': permalink,
        })

    return {'success': True, 'videos': serialized}


@app.route('/api/tiktok/demo/local-media', methods=['GET'])
@login_required
def api_tiktok_demo_media():
    if not ENABLE_TIKTOK_DEMO:
        abort(404)
    user_id = session.get('user_id')
    pages = _get_user_tiktok_pages(user_id)
    page_ids = [page.id for page in pages]

    media = []
    if page_ids:
        # Find every Postly-hosted video that was scheduled to one of the
        # connected TikTok channels so the Share Kit UI can reuse the asset.
        media_rows = (
            db.session.query(PostMedia, Post, ConnectedPage)
            .join(Post, PostMedia.post_id == Post.id)
            .join(PostPageAssociation, PostPageAssociation.post_id == Post.id)
            .join(ConnectedPage, ConnectedPage.id == PostPageAssociation.page_id)
            .filter(
                ConnectedPage.id.in_(page_ids),
                ConnectedPage.platform == 'tiktok',
                func.lower(PostMedia.media_type).like('video%')  # legacy uploads stored as VIDEO or video/mp4
            )
            .order_by(Post.created_at.desc())
            .limit(10)
            .all()
        )

        for media_row, post, page in media_rows:
            media.append({
                'post_id': post.id,
                'caption': post.caption or post.content,
                'media_url': media_row.media_url,
                'absolute_url': build_absolute_url(media_row.media_url),
                'page_name': page.page_name,
                'is_historical': False,
            })

    historical_media = _fetch_historical_tiktok_media(user_id)
    media.extend(historical_media)

    return {'success': True, 'media': media}


@app.route('/api/tiktok/demo/share-link', methods=['POST'])
@login_required
def api_tiktok_demo_share_link():
    if not ENABLE_TIKTOK_DEMO:
        abort(404)
    data = request.get_json() or {}
    media_url = data.get('media_url')
    caption = data.get('caption', 'Postly share demo')

    if not media_url:
        return {'success': False, 'error': 'media_url is required'}, 400

    absolute_media = build_absolute_url(media_url)
    if not absolute_media:
        return {'success': False, 'error': 'Unable to build a public media URL'}, 400

    # TikTok Share Kit expects a public media URL (source) plus an optional
    # title; we keep the parameters human-readable for the reviewer.
    title = caption[:60] if caption else 'Postly share demo'
    share_url = (
        'https://www.tiktok.com/upload?'
        f'app_name=Postly&refer=postly_sharekit_demo&source={quote(absolute_media)}&title={quote(title)}'
    )

    return {'success': True, 'share_url': share_url}


@app.route('/api/analytics-summary', methods=['GET'])
@login_required
@monitor_performance('analytics_summary')
def get_analytics_summary():
    """Get pre-calculated analytics summary (Buffer-style Tier 1: fast overview data)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    try:
        days = request.args.get('days', 30, type=int)
        if days not in [7, 30, 90]:
            days = 30
        
        from datetime import timedelta
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)
        
        # Try to get cached summaries from DailyAnalyticsSummary table
        try:
            with PerformanceTimer('query_daily_summaries'):
                summaries = DailyAnalyticsSummary.query.filter(
                    DailyAnalyticsSummary.user_id == user_id,
                    DailyAnalyticsSummary.date >= start_date,
                    DailyAnalyticsSummary.date <= end_date,
                    DailyAnalyticsSummary.page_id.is_(None)  # Get user-level summaries
                ).all()
            
            # If we have cached data, use it
            if summaries:
                log_cache_hit('analytics_summary', True)
                
                # Aggregate the cached summaries
                total_posts = sum(s.total_posts for s in summaries)
                total_reach = sum(s.total_reach for s in summaries)
                total_clicks = sum(s.total_clicks for s in summaries)
                total_video_views = sum(s.total_video_views for s in summaries)
                total_likes = sum(s.total_likes for s in summaries)
                total_comments = sum(s.total_comments for s in summaries)
                total_shares = sum(s.total_shares for s in summaries)
                total_engagement = total_likes + total_comments + total_shares
                
                # Calculate average engagement rate
                valid_rates = [s.avg_engagement_rate for s in summaries if s.avg_engagement_rate > 0]
                avg_engagement_rate = sum(valid_rates) / len(valid_rates) if valid_rates else 0
                
                last_updated = max(s.updated_at for s in summaries) if summaries else None
                
                return jsonify({
                    'success': True,
                    'cached': True,
                    'summary': {
                        'total_posts': total_posts,
                        'total_reach': total_reach,
                        'total_clicks': total_clicks,
                        'total_video_views': total_video_views,
                        'total_engagement': total_engagement,
                        'avg_engagement_rate': round(avg_engagement_rate, 2)
                    },
                    'last_updated': last_updated.isoformat() if last_updated else None,
                    'date_range': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
                    'days': days
                })
        
        except Exception as cache_error:
            # Table doesn't exist or query error - gracefully fallback
            error_msg = str(cache_error).split('\n')[0]  # Get first line only, skip SQLAlchemy docs link
            if 'does not exist' in error_msg or 'no such table' in error_msg:
                logger.info("Cache table not available (run migration to enable caching)")
            else:
                logger.info(f"Cache query failed: {error_msg}")
        
        # No cached data available - return cache miss response
        log_cache_hit('analytics_summary', False)
        return jsonify({
            'success': True,
            'cached': False,
            'message': 'No cached data available. Run migration: python migrations/create_daily_analytics_summary.py',
            'last_updated': None,
            'date_range': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
            'days': days
        })
        
    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        return jsonify({'success': False, 'error': 'Analytics summary temporarily unavailable'}), 500


@app.route('/api/analytics/jobs', methods=['POST'])
@login_required
def create_analytics_refresh_job():
    user_id = session.get('user_id')
    if not user_id:
        return {'success': False, 'error': 'Not authenticated'}, 401

    data = request.get_json() or {}
    page_id = data.get('page_id')
    scope = data.get('scope', 'manual')
    auto_start = data.get('auto_start', True)
    reuse_existing = data.get('reuse_existing', True)
    batch_size = data.get('batch_size')

    if page_id:
        page = ConnectedPage.query.get(page_id)
        if not page or not user_can_access_page(user_id, page):
            return {'success': False, 'error': 'Page not found or inaccessible'}, 404
    else:
        page = None

    try:
        if batch_size:
            batch_size = max(1, int(batch_size))
        else:
            batch_size = None
    except (TypeError, ValueError):
        batch_size = None

    running_query = AnalyticsRefreshJob.query.filter(
        AnalyticsRefreshJob.user_id == user_id,
        AnalyticsRefreshJob.status.in_(['pending', 'running'])
    )
    if page_id:
        running_query = running_query.filter(AnalyticsRefreshJob.page_id == page_id)
    else:
        running_query = running_query.filter(AnalyticsRefreshJob.page_id.is_(None))

    existing = running_query.order_by(AnalyticsRefreshJob.created_at.desc()).first()
    if existing and reuse_existing:
        remaining = _safe_count_posts_pending_refresh(user_id, page_id)
        return {
            'success': True,
            'job': existing.to_dict(),
            'reused': True,
            'remaining': remaining
        }

    job = enqueue_analytics_refresh_job(
        user_id=user_id,
        page_id=page_id,
        scope=scope,
        batch_size=batch_size,
        auto_start=auto_start
    )

    if not job:
        return {'success': False, 'error': 'Unable to create analytics job'}, 500

    remaining = _safe_count_posts_pending_refresh(user_id, page_id)
    return {
        'success': True,
        'job': job.to_dict(),
        'remaining': remaining
    }


@app.route('/api/analytics/jobs/latest', methods=['GET'])
@login_required
def get_latest_analytics_refresh_job():
    user_id = session.get('user_id')
    if not user_id:
        return {'success': False, 'error': 'Not authenticated'}, 401

    page_id = request.args.get('page_id', type=int)
    job = get_latest_analytics_job(user_id, page_id)
    remaining = _safe_count_posts_pending_refresh(user_id, page_id)

    return {
        'success': True,
        'job': job.to_dict() if job else None,
        'remaining': remaining
    }


@app.route('/api/analytics/jobs/<int:job_id>', methods=['GET'])
@login_required
def get_analytics_refresh_job(job_id):
    user_id = session.get('user_id')
    if not user_id:
        return {'success': False, 'error': 'Not authenticated'}, 401

    job = AnalyticsRefreshJob.query.get(job_id)
    if not job or job.user_id != user_id:
        return {'success': False, 'error': 'Job not found'}, 404

    remaining = _safe_count_posts_pending_refresh(user_id, job.page_id)
    return {
        'success': True,
        'job': job.to_dict(),
        'remaining': remaining
    }


@app.route('/api/refresh-analytics', methods=['POST'])
@login_required
def refresh_analytics():
    """Refresh analytics for user's published posts (batch of 5 to avoid timeout)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    try:
        # Import inside the function to ensure app context is available
        from tasks import refresh_all_post_analytics, count_posts_needing_refresh
        
        # Get total count of posts that need refresh
        total_posts = count_posts_needing_refresh(user_id)
        
        # Call the refresh function with user_id and batch limit to prevent worker timeout
        # Process only 5 posts per request (each post can take ~10-15s with API calls)
        # This keeps total response time under 25 seconds (well within Gunicorn's 30s timeout)
        result = refresh_all_post_analytics(user_id, limit=5)

        if not result or result.get('error'):
            error_message = result.get('error', 'Failed to refresh analytics') if result else 'Failed to refresh analytics'
            return jsonify({
                'success': False,
                'error': error_message,
                'success_count': result.get('success', 0) if result else 0,
                'failed_count': result.get('failed', 0) if result else 0,
                'skipped_count': result.get('skipped', 0) if result else 0,
                'total_posts': total_posts,
                'remaining': max(0, total_posts - result.get('processed', 0))
            }), 500
        
        success_count = result.get('success', 0)
        processed = result.get('processed', 0)
        remaining = max(0, total_posts - processed)
        
        return jsonify({
            'success': True,
            'message': f"Refreshed {success_count} posts",
            'success_count': success_count,
            'failed_count': result.get('failed', 0),
            'skipped_count': result.get('skipped', 0),
            'processed': processed,
            'total_posts': total_posts,
            'remaining': remaining,
            'has_more': remaining > 0
        }), 200
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print("Error refreshing analytics: {}".format(e))
        print("Traceback: {}".format(error_trace))
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/refresh-analytics-progress', methods=['GET'])
@login_required
def refresh_analytics_progress():
    """Get progress of analytics refresh for user"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    try:
        from tasks import count_posts_needing_refresh
        
        total_posts = count_posts_needing_refresh(user_id)
        
        return jsonify({
            'success': True,
            'total_posts': total_posts
        }), 200
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ======================== TEAM MANAGEMENT ROUTES ========================

# Register team routes
from team_routes import register_team_routes
register_team_routes(app, db, User, Team, TeamMember, ChannelAccess, TeamInvitation, ConnectedPage,
                     check_owner_access, check_admin_access, check_team_member_access)


# ======================== DATABASE AUTO-INITIALIZATION ========================

# Global flag to track database initialization
_db_initialized = False

@app.before_request
def initialize_database():
    """Auto-initialize database tables on first request if they don't exist"""
    global _db_initialized
    
    # Only run once
    if _db_initialized:
        if not _self_cron_started:
            _ensure_self_cron_started()
        return
    
    try:
        # Check if tables exist using inspector
        from sqlalchemy import inspect
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        
        if 'user' in tables:
            _db_initialized = True
            print("[DB] ✓ Database tables already exist")
            _ensure_self_cron_started()
            return
        
        # Tables don't exist, create them
        print("[DB] Database tables not found, creating...")
        db.create_all()
        
        # Verify tables were created
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        print(f"[DB] ✓ Created {len(tables)} tables: {', '.join(tables)}")
        _db_initialized = True
        _ensure_self_cron_started()
        
    except Exception as e:
        print(f"[DB] ✗ Failed to initialize database: {e}")
        import traceback
        traceback.print_exc()
        # Don't mark as initialized so it will try again
        # But don't crash the app either


# ======================== TEMPLATE CONTEXT PROCESSOR ========================

def _collect_page_ids_for_approval(user_id):
    """Return connected page IDs where the user can approve drafts."""
    page_ids = set(
        pid for (pid,) in db.session.query(ConnectedPage.id).filter_by(user_id=user_id).all()
    )

    admin_team_ids = [
        team_id for (team_id,) in db.session.query(TeamMember.team_id)
        .filter_by(user_id=user_id, role='admin').all()
    ]

    if admin_team_ids:
        owner_ids = [owner_id for (owner_id,) in db.session.query(Team.owner_id)
                     .filter(Team.id.in_(admin_team_ids)).all()]
        for owner_id in owner_ids:
            owner_page_ids = db.session.query(ConnectedPage.id).filter_by(user_id=owner_id).all()
            page_ids.update(pid for (pid,) in owner_page_ids)

    return page_ids


def _get_pending_approval_count(user_id):
    page_ids = _collect_page_ids_for_approval(user_id)
    if not page_ids:
        return 0

    return db.session.query(Post).join(
        PostPageAssociation, Post.id == PostPageAssociation.post_id
    ).filter(
        PostPageAssociation.page_id.in_(page_ids),
        Post.approval_status == 'pending'
    ).distinct().count()


@app.context_processor
def inject_pending_invitations_count():
    """Make pending invitations and approval counts available to all templates"""
    default_context = {
        'pending_invitations_count': 0,
        'pending_approval_count': 0,
        'tiktok_demo_enabled': ENABLE_TIKTOK_DEMO,
    }

    if not _db_initialized:
        return default_context
    
    if 'user_id' in session:
        user_id = session.get('user_id')
        try:
            user = User.query.get(user_id)
            if user:
                # Count pending invitations for this user's email
                count = TeamInvitation.query.filter_by(
                    invited_email=user.email,
                    status='pending'
                ).filter(
                    TeamInvitation.expires_at > datetime.utcnow()
                ).count()
                default_context['pending_invitations_count'] = count
                default_context['pending_approval_count'] = _get_pending_approval_count(user_id)
                return default_context
        except Exception as e:
            print(f"[DB] Error in inject_pending_invitations_count: {e}")
            # Return defaults if database isn't ready yet
            pass
    return default_context


# ======================== OAUTH ROUTES ========================

@app.route('/connect/<platform>', methods=['GET'])
@login_required
def connect_social_platform(platform):
    """Initiate OAuth connection with social media platform"""
    platform = platform.lower()
    
    if platform == 'facebook':
        oauth_url, state = get_facebook_oauth_url()
        session['oauth_state'] = state
        session['oauth_platform'] = platform
        return redirect(oauth_url)
    elif platform == 'instagram':
        # Instagram uses Facebook OAuth
        oauth_url, state = get_facebook_oauth_url()
        session['oauth_state'] = state
        session['oauth_platform'] = platform
        return redirect(oauth_url)
    elif platform == 'tiktok':
        try:
            oauth_url, state, code_verifier = build_tiktok_oauth_url()
        except TikTokApiError as exc:
            flash(f'TikTok OAuth unavailable: {exc}', 'danger')
            return redirect(url_for('publish'))
        session['oauth_state'] = state
        session['oauth_platform'] = platform
        session['oauth_code_verifier'] = code_verifier
        return redirect(oauth_url)
    else:
        flash('Invalid platform', 'danger')
        return redirect(url_for('publish'))


@app.route('/oauth/facebook/callback', methods=['GET'])
@login_required
def oauth_facebook_callback():
    """Handle Facebook OAuth callback"""
    print("\n[OAUTH] ========== FACEBOOK CALLBACK START ==========")
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error')
    print(f"[OAUTH] Received: code={code[:10] if code else None}..., state={state}, error={error}")
    
    # Verify state parameter
    if error:
        print(f"[OAUTH] Authorization error: {error}")
        flash(f'OAuth error: {error}', 'danger')
        return redirect(url_for('publish'))
    
    if not code:
        print("[OAUTH] No authorization code received")
        flash('No authorization code received', 'danger')
        return redirect(url_for('publish'))
    
    if state != session.get('oauth_state'):
        print(f"[OAUTH] State mismatch! Received: {state}, Expected: {session.get('oauth_state')}")
        flash('State mismatch - possible CSRF attack', 'danger')
        session.pop('oauth_state', None)
        return redirect(url_for('publish'))
    
    print("[OAUTH] State verification passed")
    
    # Exchange code for access token
    print("[OAUTH] Exchanging code for access token...")
    token_data = exchange_facebook_code_for_token(code)
    print(f"[OAUTH] Token exchange result: {token_data}")
    if not token_data or 'access_token' not in token_data:
        print("[OAUTH] Failed to get access token from token_data")
        flash('Failed to obtain access token', 'danger')
        return redirect(url_for('publish'))
    
    access_token = token_data['access_token']
    user_id = session.get('user_id')
    platform = session.get('oauth_platform', 'facebook')
    print(f"[OAUTH] Got access token. user_id={user_id}, platform={platform}")
    
    # Verify user exists in database (handle stale sessions)
    if not user_id:
        print("[OAUTH] ERROR: No user_id in session")
        flash('Please log in to connect your account.', 'warning')
        session.clear()
        return redirect(url_for('login'))
    
    user = User.query.get(user_id)
    if not user:
        print(f"[OAUTH] ERROR: User {user_id} not found in database. Session is stale.")
        flash('Your session has expired. Please log in again.', 'warning')
        session.clear()
        return redirect(url_for('login'))
    
    # Store OAuth token
    print("[OAUTH] Storing OAuth token in database...")
    oauth_token = OAuthToken.query.filter_by(user_id=user_id, platform=platform).first()
    if not oauth_token:
        oauth_token = OAuthToken(user_id=user_id, platform=platform)
        print("[OAUTH] Created new OAuthToken record")
    else:
        print("[OAUTH] Updating existing OAuthToken record")
    
    oauth_token.access_token = access_token
    if 'expires_in' in token_data:
        oauth_token.token_expires_at = datetime.utcnow() + timedelta(seconds=token_data['expires_in'])
    
    db.session.add(oauth_token)
    db.session.commit()
    print("[OAUTH] OAuthToken saved successfully")
    
    # Fetch and display user's pages
    print("[OAUTH] Fetching Facebook pages...")
    pages_data = get_facebook_pages(access_token)
    print(f"[OAUTH] Pages data response: {pages_data}")
    if not pages_data or 'data' not in pages_data:
        print("[OAUTH] Failed to fetch pages or no 'data' key in response")
        flash('Failed to fetch your accounts', 'danger')
        return redirect(url_for('publish'))
    
    print(f"[OAUTH] Got {len(pages_data['data'])} pages")
    normalized_pages = normalize_oauth_accounts(platform, pages_data.get('data', []), access_token)
    if not normalized_pages:
        flash('No accounts were returned for this platform', 'danger')
        return redirect(url_for('publish'))
    
    # Store pages data in session for selection page
    session['oauth_pages'] = normalized_pages
    session['oauth_access_token'] = access_token
    print(f"[OAUTH] Pages stored in session")
    
    # Clean up temporary session data
    session.pop('oauth_state', None)
    
    print("[OAUTH] ========== FACEBOOK CALLBACK END ==========\n")
    return redirect(url_for('select_oauth_accounts', platform=platform))


@app.route('/oauth/tiktok/callback', methods=['GET'])
@login_required
def oauth_tiktok_callback():
    """Handle TikTok OAuth callback."""
    print("\n[OAUTH] ========== TIKTOK CALLBACK START ==========")
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error_description') or request.args.get('error')

    if error:
        flash(f'TikTok OAuth error: {error}', 'danger')
        return redirect(url_for('publish'))

    if not code or state != session.get('oauth_state'):
        flash('Invalid TikTok OAuth response', 'danger')
        session.pop('oauth_state', None)
        return redirect(url_for('publish'))

    code_verifier = session.get('oauth_code_verifier')
    if not code_verifier:
        flash('Missing TikTok PKCE verifier in session. Please try connecting again.', 'danger')
        return redirect(url_for('publish'))

    try:
        token_data = exchange_tiktok_code_for_token(code, code_verifier)
    except TikTokApiError as exc:
        flash(f'TikTok token exchange failed: {exc}', 'danger')
        return redirect(url_for('publish'))

    access_token = token_data.get('access_token')
    refresh_token = token_data.get('refresh_token')
    expires_in = token_data.get('expires_in')

    if not access_token:
        flash('TikTok did not return an access token', 'danger')
        return redirect(url_for('publish'))

    user_id = session.get('user_id')
    platform = 'tiktok'
    session['oauth_platform'] = platform
    
    # Verify user exists in database (handle stale sessions)
    if not user_id:
        print("[OAUTH] ERROR: No user_id in session")
        flash('Please log in to connect your account.', 'warning')
        session.clear()
        return redirect(url_for('login'))
    
    user = User.query.get(user_id)
    if not user:
        print(f"[OAUTH] ERROR: User {user_id} not found in database. Session is stale.")
        flash('Your session has expired. Please log in again.', 'warning')
        session.clear()
        return redirect(url_for('login'))

    oauth_token = OAuthToken.query.filter_by(user_id=user_id, platform=platform).first()
    if not oauth_token:
        oauth_token = OAuthToken(user_id=user_id, platform=platform)
    oauth_token.access_token = access_token
    oauth_token.refresh_token = refresh_token
    if expires_in:
        oauth_token.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
    db.session.add(oauth_token)
    db.session.commit()

    fallback_accounts = []
    open_id = token_data.get('open_id')
    if open_id:
        fallback_accounts.append({
            'id': open_id,
            'open_id': open_id,
            'display_name': token_data.get('display_name') or token_data.get('username') or 'TikTok Account',
            'username': token_data.get('username'),
            'avatar_url': token_data.get('avatar_url'),
            'access_token': access_token,
        })

    try:
        accounts = get_tiktok_accounts(access_token)
        normalized = normalize_oauth_accounts(platform, accounts, access_token)
    except TikTokApiError as exc:
        if fallback_accounts:
            normalized = normalize_oauth_accounts(platform, fallback_accounts, access_token)
            flash('TikTok connected, but sandbox scopes blocked profile lookup. Using basic account info instead.', 'warning')
        else:
            flash(f'Failed to fetch TikTok account info: {exc}', 'danger')
            return redirect(url_for('publish'))

    if not normalized and fallback_accounts:
        normalized = normalize_oauth_accounts(platform, fallback_accounts, access_token)

    if not normalized:
        flash('No TikTok accounts were returned. Please ensure the required scopes are approved.', 'danger')
        return redirect(url_for('publish'))

    session['oauth_pages'] = normalized
    session['oauth_access_token'] = access_token
    session.pop('oauth_state', None)
    session.pop('oauth_code_verifier', None)

    print("[OAUTH] ========== TIKTOK CALLBACK END ==========\n")
    return redirect(url_for('select_oauth_accounts', platform=platform))


@app.route('/select-accounts/<platform>', methods=['GET', 'POST'])
@login_required
def select_oauth_accounts(platform):
    """Allow user to select which accounts to add to Postly"""
    print(f"\n[SELECT-ACCOUNTS] ========== ROUTE START (method={request.method}) ==========")
    platform = platform.lower()
    user_id = session.get('user_id')
    pages = session.get('oauth_pages', [])
    
    print(f"[SELECT-ACCOUNTS] user_id={user_id}, platform={platform}, pages_count={len(pages)}")
    
    if not pages:
        print(f"[SELECT-ACCOUNTS] No pages in session!")
        flash('No accounts found', 'danger')
        return redirect(url_for('publish'))

    def annotate_pages(pages_list):
        """Attach connection metadata for UI rendering without mutating the session payload."""
        if not pages_list:
            return []

        existing_ids = {
            str(row.platform_page_id)
            for row in ConnectedPage.query.with_entities(ConnectedPage.platform_page_id)
            .filter_by(user_id=user_id, platform=platform)
        }

        annotated = []
        for page in pages_list:
            if isinstance(page, dict):
                page_dict = page.copy()
            else:
                try:
                    page_dict = dict(page)
                except (TypeError, ValueError):
                    page_dict = getattr(page, '__dict__', {}).copy()

            page_id = str(page_dict.get('id') or page_dict.get('page_id') or page_dict.get('open_id') or '')
            page_dict['is_connected'] = bool(page_id and page_id in existing_ids)
            annotated.append(page_dict)
        return annotated

    annotated_pages = annotate_pages(pages)
    
    if request.method == 'POST':
        print("[SELECT-ACCOUNTS] Processing POST request")
        selected_page_ids = [str(pid) for pid in request.form.getlist('selected_pages')]
        print(f"[SELECT-ACCOUNTS] Selected page IDs: {selected_page_ids}")
        
        if not selected_page_ids:
            print("[SELECT-ACCOUNTS] No pages selected")
            flash('Please select at least one account', 'warning')
            return render_template('dashboard/select_accounts.html', pages=annotated_pages, platform=platform)
        
        # Add selected pages to ConnectedPage and enqueue history imports
        saved_count = 0
        access_token = session.get('oauth_access_token')
        new_pages_for_import = []

        for page_id in selected_page_ids:
            # Find page in pages list
            page_data = next((p for p in pages if str(p.get('id')) == page_id), None)
            if not page_data:
                print(f"[SELECT-ACCOUNTS] Page {page_id} not found in pages list")
                continue
            
            # Check if page already connected
            existing = ConnectedPage.query.filter_by(
                user_id=user_id,
                platform=platform,
                platform_page_id=page_id
            ).first()
            
            if existing:
                print(f"[SELECT-ACCOUNTS] Page already connected: {page_id} - {existing.page_name}")
                # Update the access token if it's different
                new_token = page_data.get('access_token') or access_token
                if new_token and existing.page_access_token != new_token:
                    existing.page_access_token = new_token
                    existing.updated_at = datetime.utcnow()
                    print(f"[SELECT-ACCOUNTS] Updated access token for {page_id}")
                continue
            
            # Create new connected page
            try:
                connected_page = ConnectedPage(
                    user_id=user_id,
                    platform=platform,
                    platform_page_id=page_id,
                    page_name=page_data.get('name', 'Unnamed Page'),
                    page_username=page_data.get('username', ''),
                    page_access_token=page_data.get('access_token') or access_token,
                    is_active=True
                )
                db.session.add(connected_page)
                db.session.flush()
                saved_count += 1
                print(f"[SELECT-ACCOUNTS] Added: {page_data.get('name', 'Unnamed')} (ID: {page_id})")
                print(f"[SELECT-ACCOUNTS] Stored page access token for {page_id}")
                
                if platform in ['facebook', 'tiktok']:
                    new_pages_for_import.append(connected_page)
            except Exception as e:
                print(f"[SELECT-ACCOUNTS] Error adding page {page_id}: {str(e)}")
                db.session.rollback()
                continue
        
        print(f"[SELECT-ACCOUNTS] About to commit {saved_count} new pages...")
        db.session.commit()
        print(f"[SELECT-ACCOUNTS] Committed successfully")

        jobs_created = []
        for page in new_pages_for_import:
            job = enqueue_page_import_job(page, user_id, scope='auto-connect')
            if job:
                jobs_created.append(job)
                print(f"[SELECT-ACCOUNTS] Queued import job {job.id} for page {page.page_name}")
        
        # Clean up session data
        session.pop('oauth_pages', None)
        session.pop('oauth_access_token', None)
        
        if jobs_created:
            flash(f'Successfully added {saved_count} account(s). Importing history in the background.', 'success')
        else:
            flash(f'Successfully added {saved_count} account(s) to Postly!', 'success')
        print("[SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
        return redirect(url_for('publish'))
    
    print("[SELECT-ACCOUNTS] Rendering GET request")
    print("[SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
    return render_template('dashboard/select_accounts.html', pages=annotated_pages, platform=platform)


# ======================== ADMIN CHANNEL MANAGEMENT ========================

@app.route('/api/admin/connect-account', methods=['POST'])
@login_required
def admin_connect_account():
    """Initiate OAuth connection as an admin to the team's social platform accounts"""
    try:
        data = request.get_json() or {}
        team_id = data.get('team_id')
        platform = (data.get('platform') or 'facebook').lower()
        
        user_id = session.get('user_id')
        
        # Verify user is admin or owner of this team
        if not check_admin_access(team_id, user_id):
            return {'success': False, 'error': 'You must be a team admin to connect accounts'}, 403
        
        print(f"[ADMIN-OAUTH] Admin {user_id} initiating {platform} OAuth for team {team_id}")
        
        if platform == 'facebook':
            oauth_url, state = get_facebook_oauth_url()
            session['admin_oauth_state'] = state
            session['admin_oauth_platform'] = platform
            session['admin_oauth_team_id'] = team_id
            session['admin_oauth_user_id'] = user_id
            
            print(f"[ADMIN-OAUTH] Generated OAuth URL with state {state}")
            return {'success': True, 'oauth_url': oauth_url, 'state': state}
        
        elif platform == 'instagram':
            # Instagram uses Facebook OAuth
            oauth_url, state = get_facebook_oauth_url()
            session['admin_oauth_state'] = state
            session['admin_oauth_platform'] = platform
            session['admin_oauth_team_id'] = team_id
            session['admin_oauth_user_id'] = user_id
            
            print(f"[ADMIN-OAUTH] Generated Instagram OAuth URL (via Facebook) with state {state}")
            return {'success': True, 'oauth_url': oauth_url, 'state': state}
        
        elif platform == 'tiktok':
            try:
                oauth_url, state, code_verifier = build_tiktok_oauth_url()
            except TikTokApiError as exc:
                return {'success': False, 'error': f'TikTok OAuth unavailable: {exc}'}, 500
            session['admin_oauth_state'] = state
            session['admin_oauth_platform'] = platform
            session['admin_oauth_team_id'] = team_id
            session['admin_oauth_user_id'] = user_id
            session['admin_oauth_code_verifier'] = code_verifier
            return {'success': True, 'oauth_url': oauth_url, 'state': state}
        
        else:
            return {'success': False, 'error': 'Invalid platform'}, 400
    
    except Exception as e:
        print(f"[ADMIN-OAUTH] Error: {str(e)}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/oauth/facebook/admin-callback', methods=['GET'])
@login_required
def oauth_facebook_admin_callback():
    """Handle Facebook OAuth callback for admin account connection"""
    print("\n[ADMIN-OAUTH-CALLBACK] ========== FACEBOOK ADMIN CALLBACK START ==========")
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error')
    
    user_id = session.get('user_id')
    team_id = session.get('admin_oauth_team_id')
    platform = session.get('admin_oauth_platform', 'facebook')
    
    print(f"[ADMIN-OAUTH-CALLBACK] user_id={user_id}, team_id={team_id}, code={code[:10] if code else None}..., error={error}")
    
    if error:
        print(f"[ADMIN-OAUTH-CALLBACK] Authorization error: {error}")
        return {'success': False, 'error': f'OAuth error: {error}'}, 400
    
    if not code:
        print("[ADMIN-OAUTH-CALLBACK] No authorization code received")
        return {'success': False, 'error': 'No authorization code received'}, 400
    
    if not user_id or not team_id:
        print(f"[ADMIN-OAUTH-CALLBACK] Invalid session state: user_id={user_id}, team_id={team_id}")
        return {'success': False, 'error': 'Invalid session state'}, 400
    
    if state != session.get('admin_oauth_state'):
        print(f"[ADMIN-OAUTH-CALLBACK] State mismatch! Received: {state}, Expected: {session.get('admin_oauth_state')}")
        return {'success': False, 'error': 'State mismatch - possible CSRF attack'}, 400
    
    # Verify admin access
    if not check_admin_access(team_id, user_id):
        print(f"[ADMIN-OAUTH-CALLBACK] User {user_id} is not admin of team {team_id}")
        return {'success': False, 'error': 'Insufficient permissions'}, 403
    
    print("[ADMIN-OAUTH-CALLBACK] State verification and admin check passed")
    
    # Exchange code for access token
    print("[ADMIN-OAUTH-CALLBACK] Exchanging code for access token...")
    token_data = exchange_facebook_code_for_token(code)
    print(f"[ADMIN-OAUTH-CALLBACK] Token exchange result: {token_data}")
    
    if not token_data or 'access_token' not in token_data:
        print("[ADMIN-OAUTH-CALLBACK] Failed to get access token from token_data")
        return {'success': False, 'error': 'Failed to obtain access token'}, 400
    
    access_token = token_data['access_token']
    
    # Store OAuth token (per user, not team-global)
    print("[ADMIN-OAUTH-CALLBACK] Storing OAuth token in database...")
    oauth_token = OAuthToken.query.filter_by(user_id=user_id, platform=platform).first()
    if not oauth_token:
        oauth_token = OAuthToken(user_id=user_id, platform=platform)
        print("[ADMIN-OAUTH-CALLBACK] Created new OAuthToken record")
    else:
        print("[ADMIN-OAUTH-CALLBACK] Updating existing OAuthToken record")
    
    oauth_token.access_token = access_token
    if 'expires_in' in token_data:
        oauth_token.token_expires_at = datetime.utcnow() + timedelta(seconds=token_data['expires_in'])
    
    db.session.add(oauth_token)
    db.session.commit()
    print("[ADMIN-OAUTH-CALLBACK] OAuthToken saved successfully")
    
    # Fetch user's pages
    print("[ADMIN-OAUTH-CALLBACK] Fetching Facebook pages...")
    pages_data = get_facebook_pages(access_token)
    print(f"[ADMIN-OAUTH-CALLBACK] Pages data response: {pages_data}")
    
    if not pages_data or 'data' not in pages_data:
        print("[ADMIN-OAUTH-CALLBACK] Failed to fetch pages or no 'data' key in response")
        return {'success': False, 'error': 'Failed to fetch your accounts'}, 400
    
    print(f"[ADMIN-OAUTH-CALLBACK] Got {len(pages_data['data'])} pages")
    normalized_pages = normalize_oauth_accounts(platform, pages_data.get('data', []), access_token)
    if not normalized_pages:
        return {'success': False, 'error': 'No accounts returned from platform'}, 400
    
    # Store pages data in session for selection page
    session['admin_oauth_pages'] = normalized_pages
    session['admin_oauth_access_token'] = access_token
    print(f"[ADMIN-OAUTH-CALLBACK] Pages stored in session")
    
    # Clean up temporary session data
    session.pop('admin_oauth_state', None)
    
    print("[ADMIN-OAUTH-CALLBACK] ========== FACEBOOK ADMIN CALLBACK END ==========\n")
    
    # Redirect to admin account selection page
    return redirect(url_for('admin_select_oauth_accounts', team_id=team_id, platform=platform))


@app.route('/oauth/tiktok/admin-callback', methods=['GET'])
@login_required
def oauth_tiktok_admin_callback():
    """Handle TikTok OAuth callback for admins."""
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error_description') or request.args.get('error')

    user_id = session.get('user_id')
    team_id = session.get('admin_oauth_team_id')
    platform = session.get('admin_oauth_platform', 'tiktok')

    if error:
        return {'success': False, 'error': f'OAuth error: {error}'}, 400
    if not code or not user_id or not team_id:
        return {'success': False, 'error': 'Invalid session state'}, 400
    if state != session.get('admin_oauth_state'):
        return {'success': False, 'error': 'State mismatch - possible CSRF attack'}, 400
    if not check_admin_access(team_id, user_id):
        return {'success': False, 'error': 'Insufficient permissions'}, 403

    code_verifier = session.get('admin_oauth_code_verifier')
    if not code_verifier:
        return {'success': False, 'error': 'Missing PKCE verifier. Please restart OAuth.'}, 400

    try:
        token_data = exchange_tiktok_code_for_token(code, code_verifier)
    except TikTokApiError as exc:
        return {'success': False, 'error': f'TikTok token exchange failed: {exc}'}, 400

    access_token = token_data.get('access_token')
    refresh_token = token_data.get('refresh_token')
    expires_in = token_data.get('expires_in')
    if not access_token:
        return {'success': False, 'error': 'TikTok access token missing'}, 400

    oauth_token = OAuthToken.query.filter_by(user_id=user_id, platform=platform).first()
    if not oauth_token:
        oauth_token = OAuthToken(user_id=user_id, platform=platform)
    oauth_token.access_token = access_token
    oauth_token.refresh_token = refresh_token
    if expires_in:
        oauth_token.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
    db.session.add(oauth_token)
    db.session.commit()

    try:
        accounts = get_tiktok_accounts(access_token)
        normalized_pages = normalize_oauth_accounts(platform, accounts, access_token)
    except TikTokApiError as exc:
        return {'success': False, 'error': f'Failed to fetch TikTok accounts: {exc}'}, 400

    if not normalized_pages:
        return {'success': False, 'error': 'No TikTok accounts returned'}, 400

    session['admin_oauth_pages'] = normalized_pages
    session['admin_oauth_access_token'] = access_token
    session.pop('admin_oauth_state', None)
    session.pop('admin_oauth_code_verifier', None)

    return redirect(url_for('admin_select_oauth_accounts', team_id=team_id, platform=platform))


@app.route('/admin/select-accounts/<int:team_id>/<platform>', methods=['GET', 'POST'])
@login_required
def admin_select_oauth_accounts(team_id, platform):
    """Allow admin to select which accounts to add to the team"""
    print(f"\n[ADMIN-SELECT-ACCOUNTS] ========== ROUTE START (method={request.method}) ==========")
    platform = platform.lower()
    user_id = session.get('user_id')
    pages = session.get('admin_oauth_pages', [])
    
    print(f"[ADMIN-SELECT-ACCOUNTS] user_id={user_id}, team_id={team_id}, platform={platform}, pages_count={len(pages)}")
    
    # Verify admin access
    if not check_admin_access(team_id, user_id):
        print(f"[ADMIN-SELECT-ACCOUNTS] User {user_id} is not admin of team {team_id}")
        flash('You must be a team admin to perform this action', 'danger')
        return redirect(url_for('team_dashboard'))
    
    if not pages:
        print(f"[ADMIN-SELECT-ACCOUNTS] No pages in session!")
        flash('No accounts found', 'danger')
        return redirect(url_for('team_dashboard'))
    
    if request.method == 'POST':
        print("[ADMIN-SELECT-ACCOUNTS] Processing POST request")
        selected_page_ids = [str(pid) for pid in request.form.getlist('selected_pages')]
        print(f"[ADMIN-SELECT-ACCOUNTS] Selected page IDs: {selected_page_ids}")
        
        if not selected_page_ids:
            print("[ADMIN-SELECT-ACCOUNTS] No pages selected")
            flash('Please select at least one account', 'warning')
            return render_template('dashboard/admin_select_accounts.html', pages=pages, platform=platform, team_id=team_id)
        
        # Get team for validation
        team = Team.query.get(team_id)
        if not team:
            flash('Team not found', 'danger')
            return redirect(url_for('team_dashboard'))
        
        # Add selected pages to ConnectedPage
        saved_count = 0
        access_token = session.get('admin_oauth_access_token')
        
        for page_id in selected_page_ids:
            # Find page in pages list
            page_data = next((p for p in pages if str(p.get('id')) == page_id), None)
            if not page_data:
                print(f"[ADMIN-SELECT-ACCOUNTS] Page {page_id} not found in pages list")
                continue
            
            # Check if page already connected
            existing = ConnectedPage.query.filter_by(
                platform=platform,
                platform_page_id=page_id
            ).filter(
                db.or_(
                    ConnectedPage.user_id == team.owner_id,  # Owner's connection
                    db.and_(
                        ConnectedPage.team_id == team_id,  # Team's connection
                        ConnectedPage.is_team_owned == True
                    )
                )
            ).first()
            
            if existing:
                print(f"[ADMIN-SELECT-ACCOUNTS] Page already connected: {page_id} - {existing.page_name}")
                # Update the access token if it's different
                new_token = page_data.get('access_token') or access_token
                if new_token and existing.page_access_token != new_token:
                    existing.page_access_token = new_token
                    existing.updated_at = datetime.utcnow()
                    print(f"[ADMIN-SELECT-ACCOUNTS] Updated access token for {page_id}")
                continue
            
            # Create new connected page
            try:
                connected_page = ConnectedPage(
                    user_id=user_id,  # Admin user who connected it
                    team_id=team_id,  # For team context
                    platform=platform,
                    platform_page_id=page_id,
                    page_name=page_data.get('name', 'Unnamed Page'),
                    page_username=page_data.get('username', ''),
                    page_access_token=page_data.get('access_token') or access_token,
                    is_active=True,
                    is_team_owned=True,  # Mark as team-owned
                    connected_by_user_id=user_id  # Track who connected
                )
                db.session.add(connected_page)
                db.session.flush()
                saved_count += 1
                print(f"[ADMIN-SELECT-ACCOUNTS] Added: {page_data.get('name', 'Unnamed')} (ID: {page_id})")
                
                if platform == 'tiktok' and access_token:
                    try:
                        posts_data = list_tiktok_posts(page_id, access_token)
                        stored = store_tiktok_posts_to_db(user_id, connected_page, posts_data)
                        print(f"[ADMIN-SELECT-ACCOUNTS] Stored {stored} TikTok posts for new channel")
                    except TikTokApiError as exc:
                        print(f"[ADMIN-SELECT-ACCOUNTS] TikTok sync failed: {exc}")
                
                # Add admin as full_posting member on this channel
                team_member = TeamMember.query.filter_by(user_id=user_id, team_id=team_id).first()
                if team_member:
                    existing_access = ChannelAccess.query.filter_by(
                        team_id=team_id,
                        team_member_id=team_member.id,
                        channel_id=connected_page.id
                    ).first()
                    
                    if not existing_access:
                        channel_access = ChannelAccess(
                            team_id=team_id,
                            team_member_id=team_member.id,
                            channel_id=connected_page.id,
                            access_level='full_posting'
                        )
                        db.session.add(channel_access)
                        print(f"[ADMIN-SELECT-ACCOUNTS] Added admin {user_id} to channel {connected_page.id} with full_posting")
            except Exception as e:
                print(f"[ADMIN-SELECT-ACCOUNTS] Error adding page {page_id}: {str(e)}")
                db.session.rollback()
                continue
        
        db.session.commit()
        flash(f'Successfully added {saved_count} account(s) to the team', 'success')
        print(f"[ADMIN-SELECT-ACCOUNTS] Saved {saved_count} accounts")
        print(f"[ADMIN-SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
        return redirect(url_for('admin_channels', team_id=team_id))
    
    print("[ADMIN-SELECT-ACCOUNTS] Rendering GET request")
    print("[ADMIN-SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
    return render_template('dashboard/admin_select_accounts.html', pages=pages, platform=platform, team_id=team_id)


@app.route('/admin-connect/<platform>/<int:team_id>')
@login_required
def admin_connect_redirect(platform, team_id):
    """Redirect to initiate admin OAuth connection"""
    user_id = session.get('user_id')
    
    # Verify user is admin of team
    if not check_admin_access(team_id, user_id):
        return {'success': False, 'error': 'Unauthorized'}, 403
    
    # Redirect to OAuth initialization endpoint
    return redirect(url_for('admin_connect_account', platform=platform, team_id=team_id))


@app.route('/admin/channels/<int:team_id>', methods=['GET'])
@login_required
def admin_channels(team_id):
    """Admin dashboard for managing team channels"""
    print(f"\n[ADMIN-CHANNELS] ========== GET /admin/channels/{team_id} ==========")
    user_id = session.get('user_id')
    
    # Verify admin access
    if not check_admin_access(team_id, user_id):
        print(f"[ADMIN-CHANNELS] User {user_id} is not admin of team {team_id}")
        flash('You must be a team admin to access this page', 'danger')
        return redirect(url_for('team_dashboard'))
    
    team = Team.query.get(team_id)
    if not team:
        flash('Team not found', 'danger')
        return redirect(url_for('team_dashboard'))
    
    # Get all team channels (both owner-owned and team-owned)
    channels = ConnectedPage.query.filter(
        db.or_(
            db.and_(ConnectedPage.team_id == team_id, ConnectedPage.is_team_owned == True),
            ConnectedPage.user_id == team.owner_id
        )
    ).all()
    
    print(f"[ADMIN-CHANNELS] Found {len(channels)} channels for team {team_id}")
    
    # For each channel, get members with access
    channel_data = []
    for channel in channels:
        members = db.session.query(
            TeamMember.id,
            TeamMember.user_id,
            User.username,
            ChannelAccess.access_level
        ).join(
            ChannelAccess, ChannelAccess.team_member_id == TeamMember.id
        ).join(
            User, User.id == TeamMember.user_id
        ).filter(
            ChannelAccess.channel_id == channel.id,
            ChannelAccess.team_id == team_id
        ).all()
        
        print(f"[ADMIN-CHANNELS] Channel {channel.id} ({channel.page_name}) has {len(members)} members")
        
        channel_data.append({
            'id': channel.id,
            'name': channel.page_name,
            'platform': channel.platform,
            'platform_page_id': channel.platform_page_id,
            'added_by_user_id': channel.connected_by_user_id or channel.user_id,
            'is_team_owned': channel.is_team_owned,
            'members': [
                {
                    'user_id': m[1],
                    'username': m[2],
                    'access_level': m[3]
                } for m in members
            ]
        })
    
    print(f"[ADMIN-CHANNELS] ========== ROUTE END ==========\n")
    return render_template('dashboard/admin_channels.html', team=team, channels=channel_data)


@app.route('/api/admin/disconnect-channel', methods=['POST'])
@login_required
def api_admin_disconnect_channel():
    """Disconnect a channel from the team"""
    try:
        data = request.get_json() or {}
        team_id = data.get('team_id')
        channel_id = data.get('channel_id')
        
        user_id = session.get('user_id')
        
        # Verify admin access
        if not check_admin_access(team_id, user_id):
            return {'success': False, 'error': 'You must be a team admin to perform this action'}, 403
        
        print(f"[ADMIN-DISCONNECT] Admin {user_id} disconnecting channel {channel_id} from team {team_id}")
        
        channel = ConnectedPage.query.get(channel_id)
        if not channel:
            return {'success': False, 'error': 'Channel not found'}, 404
        
        # Verify channel belongs to this team
        if channel.team_id != team_id and channel.user_id != Team.query.get(team_id).owner_id:
            return {'success': False, 'error': 'Channel does not belong to this team'}, 403
        
        # Remove all ChannelAccess entries for this channel
        accesses = ChannelAccess.query.filter_by(channel_id=channel_id, team_id=team_id).all()
        for access in accesses:
            db.session.delete(access)
            print(f"[ADMIN-DISCONNECT] Removed access for TeamMember {access.team_member_id}")
        
        # Mark channel as inactive (soft delete)
        channel.is_active = False
        db.session.commit()
        
        print(f"[ADMIN-DISCONNECT] Channel {channel_id} disconnected successfully")
        return {'success': True, 'message': 'Channel disconnected from team'}
    
    except Exception as e:
        print(f"[ADMIN-DISCONNECT] Error: {str(e)}")
        db.session.rollback()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/admin/assign-to-channel', methods=['POST'])
@login_required
def api_admin_assign_to_channel():
    """Assign admin or team member to a channel"""
    try:
        data = request.get_json() or {}
        team_id = data.get('team_id')
        channel_id = data.get('channel_id')
        target_user_id = data.get('user_id')  # User to assign (or self)
        access_level = data.get('access_level', 'full_posting')
        
        user_id = session.get('user_id')
        
        # Verify admin access
        if not check_admin_access(team_id, user_id):
            return {'success': False, 'error': 'You must be a team admin to perform this action'}, 403
        
        print(f"[ADMIN-ASSIGN] Admin {user_id} assigning user {target_user_id} to channel {channel_id} with {access_level}")
        
        # Verify channel exists and belongs to team
        channel = ConnectedPage.query.get(channel_id)
        if not channel:
            return {'success': False, 'error': 'Channel not found'}, 404
        
        if channel.team_id != team_id and channel.user_id != Team.query.get(team_id).owner_id:
            return {'success': False, 'error': 'Channel does not belong to this team'}, 403
        
        # Verify access level is valid
        if access_level not in ['full_posting', 'approval_required', 'none']:
            return {'success': False, 'error': 'Invalid access level'}, 400
        
        # Get target user's TeamMember record
        team_member = TeamMember.query.filter_by(user_id=target_user_id, team_id=team_id).first()
        if not team_member:
            return {'success': False, 'error': 'User is not a member of this team'}, 404
        
        # Check if assignment already exists
        existing_access = ChannelAccess.query.filter_by(
            team_id=team_id,
            team_member_id=team_member.id,
            channel_id=channel_id
        ).first()
        
        if existing_access:
            # Update existing access
            existing_access.access_level = access_level
            db.session.commit()
            print(f"[ADMIN-ASSIGN] Updated access level to {access_level}")
            return {'success': True, 'message': 'Member access updated'}
        else:
            # Create new access
            channel_access = ChannelAccess(
                team_id=team_id,
                team_member_id=team_member.id,
                channel_id=channel_id,
                access_level=access_level
            )
            db.session.add(channel_access)
            db.session.commit()
            print(f"[ADMIN-ASSIGN] Created new access for user {target_user_id}")
            return {'success': True, 'message': 'Member assigned to channel'}
    
    except Exception as e:
        print(f"[ADMIN-ASSIGN] Error: {str(e)}")
        db.session.rollback()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/admin/unassign-from-channel', methods=['POST'])
@login_required
def api_admin_unassign_from_channel():
    """Remove a team member from a channel"""
    try:
        data = request.get_json() or {}
        team_id = data.get('team_id')
        channel_id = data.get('channel_id')
        target_user_id = data.get('user_id')
        
        user_id = session.get('user_id')
        
        # Verify admin access
        if not check_admin_access(team_id, user_id):
            return {'success': False, 'error': 'You must be a team admin to perform this action'}, 403
        
        print(f"[ADMIN-UNASSIGN] Admin {user_id} removing user {target_user_id} from channel {channel_id}")
        
        # Get target user's TeamMember record
        team_member = TeamMember.query.filter_by(user_id=target_user_id, team_id=team_id).first()
        if not team_member:
            return {'success': False, 'error': 'User is not a member of this team'}, 404
        
        # Find and delete access
        channel_access = ChannelAccess.query.filter_by(
            team_id=team_id,
            team_member_id=team_member.id,
            channel_id=channel_id
        ).first()
        
        if not channel_access:
            return {'success': False, 'error': 'User does not have access to this channel'}, 404
        
        db.session.delete(channel_access)
        db.session.commit()
        
        print(f"[ADMIN-UNASSIGN] Removed user {target_user_id} from channel {channel_id}")
        return {'success': True, 'message': 'Member removed from channel'}
    
    except Exception as e:
        print(f"[ADMIN-UNASSIGN] Error: {str(e)}")
        db.session.rollback()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/admin/channels', methods=['GET'])
@login_required
def api_admin_channels_list():
    """Get all team channels with member assignments (JSON API)"""
    try:
        team_id = request.args.get('team_id', type=int)
        user_id = session.get('user_id')
        
        if not team_id:
            return {'success': False, 'error': 'team_id required'}, 400
        
        # Verify admin access
        if not check_admin_access(team_id, user_id):
            return {'success': False, 'error': 'Insufficient permissions'}, 403
        
        team = Team.query.get(team_id)
        if not team:
            return {'success': False, 'error': 'Team not found'}, 404
        
        # Get all team channels
        channels = ConnectedPage.query.filter(
            db.or_(
                db.and_(ConnectedPage.team_id == team_id, ConnectedPage.is_team_owned == True),
                ConnectedPage.user_id == team.owner_id
            ),
            ConnectedPage.is_active == True
        ).all()
        
        channel_data = []
        for channel in channels:
            # Get members with access
            members = db.session.query(
                TeamMember.id,
                TeamMember.user_id,
                User.username,
                ChannelAccess.access_level
            ).join(
                ChannelAccess, ChannelAccess.team_member_id == TeamMember.id
            ).join(
                User, User.id == TeamMember.user_id
            ).filter(
                ChannelAccess.channel_id == channel.id,
                ChannelAccess.team_id == team_id
            ).all()
            
            channel_data.append({
                'id': channel.id,
                'name': channel.page_name,
                'platform': channel.platform,
                'platform_page_id': channel.platform_page_id,
                'is_team_owned': channel.is_team_owned,
                'members': [
                    {
                        'user_id': m[1],
                        'username': m[2],
                        'access_level': m[3]
                    } for m in members
                ]
            })
        
        return {'success': True, 'channels': channel_data}
    
    except Exception as e:
        print(f"[API-ADMIN-CHANNELS] Error: {str(e)}")
        return {'success': False, 'error': str(e)}, 500


# ======================== CONNECTED PAGES MANAGEMENT ========================

@app.route('/api/connected-pages/<int:page_id>', methods=['DELETE'])
@login_required
def delete_connected_page(page_id):
    """Delete a connected page and all associated posts, analytics, and data"""
    try:
        user_id = session.get('user_id')
        page = ConnectedPage.query.filter_by(id=page_id, user_id=user_id).first()
        
        if not page:
            return {'success': False, 'error': 'Page not found'}, 404
        
        page_name = page.page_name
        print(f"[DELETE PAGE] Starting deletion of page {page_id} ({page_name})")

        # Remove background jobs that still reference this page to avoid FK violations
        import_jobs = PageImportJob.query.filter_by(page_id=page_id).all()
        print(f"[DELETE PAGE] Found {len(import_jobs)} import jobs")
        for job in import_jobs:
            db.session.delete(job)
            print(f"[DELETE PAGE] Deleted import job {job.id}")

        refresh_jobs = AnalyticsRefreshJob.query.filter_by(page_id=page_id).all()
        print(f"[DELETE PAGE] Found {len(refresh_jobs)} analytics refresh jobs")
        for job in refresh_jobs:
            db.session.delete(job)
            print(f"[DELETE PAGE] Deleted analytics refresh job {job.id}")
        
        # Delete channel access records for this page
        channel_access_records = ChannelAccess.query.filter_by(channel_id=page_id).all()
        for access_record in channel_access_records:
            db.session.delete(access_record)
            print(f"[DELETE PAGE] Deleted channel access record {access_record.id}")
        print(f"[DELETE PAGE] Deleted {len(channel_access_records)} channel access records")
        
        # Get all post-page associations for this page
        associations = PostPageAssociation.query.filter_by(page_id=page_id).all()
        print(f"[DELETE PAGE] Found {len(associations)} post associations")
        
        # For each association, delete related analytics and the association itself
        for assoc in associations:
            # Delete analytics for this association
            analytics = PostAnalytics.query.filter_by(post_page_association_id=assoc.id).all()
            for analytics_record in analytics:
                db.session.delete(analytics_record)
                print(f"[DELETE PAGE] Deleted analytics record {analytics_record.id}")
            
            # Delete the association
            db.session.delete(assoc)
            print(f"[DELETE PAGE] Deleted association {assoc.id}")
        
        # Check if any posts are ONLY associated with this page
        # If so, delete the entire post since it has no other channels
        print(f"[DELETE PAGE] Checking for orphaned posts...")
        for assoc in associations:
            post = Post.query.get(assoc.post_id)
            if post:
                # Count how many active associations this post has
                remaining_assocs = PostPageAssociation.query.filter_by(post_id=post.id).filter(
                    PostPageAssociation.page_id != page_id
                ).count()
                
                if remaining_assocs == 0:
                    # This post is only on the deleted page, delete the entire post
                    print(f"[DELETE PAGE] Deleting orphaned post {post.id}")
                    
                    # Delete media associated with this post
                    media_files = PostMedia.query.filter_by(post_id=post.id).all()
                    for media in media_files:
                        db.session.delete(media)
                    
                    # Delete the post itself
                    db.session.delete(post)
        
        # Finally, delete the connected page
        db.session.delete(page)
        db.session.commit()
        
        print(f"[DELETE PAGE] Successfully deleted page {page_id} ({page_name}) for user {user_id}")
        return {'success': True, 'message': f'Disconnected {page_name} and removed all associated data'}
    except Exception as e:
        db.session.rollback()
        print(f"[DELETE PAGE] Error deleting page: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


# ======================== POST MANAGEMENT ROUTES ========================

@app.route('/api/posts', methods=['POST'])
@login_required
def create_post():
    """Create a new post with media and schedule it for connected pages"""
    try:
        print("\n[POST] ========== CREATE POST START ==========")
        user_id = session.get('user_id')
        
        # Handle both form and JSON data
        if request.is_json:
            data = request.get_json()
            caption = data.get('caption', '').strip()
            pages_data = data.get('pages', [])  # Get as array directly
            post_icon = data.get('icon', 'fas fa-pen')
            publish_type = data.get('publish_type', 'now')
            scheduled_time_str = data.get('scheduled_time', '')
            submit_for_approval = data.get('submit_for_approval', False)  # NEW: approval workflow
        else:
            # Get form data
            caption = request.form.get('caption', '').strip()
            pages_json = request.form.get('pages', '[]')
            post_icon = request.form.get('icon', 'fas fa-pen')  # Default icon if not provided
            publish_type = request.form.get('publish_type', 'now')
            scheduled_time_str = request.form.get('scheduled_time', '')
            submit_for_approval = request.form.get('submit_for_approval', 'false').lower() == 'true'  # NEW: approval workflow
            
            # Parse pages from form
            try:
                import json
                pages_data = json.loads(pages_json)
            except:
                return {'success': False, 'error': 'Invalid pages data'}, 400
        
        print(f"[POST] User: {user_id}, Caption length: {len(caption)}, Publish type: {publish_type}, Submit for approval: {submit_for_approval}")
        print(f"[POST] Scheduled time string: '{scheduled_time_str}'")
        print(f"[POST] Pages data type: {type(pages_data)}, Pages data: {pages_data}")
        
        # Validate input
        if not caption:
            print("[POST] ERROR: Caption is empty")
            return {'success': False, 'error': 'Caption is required'}, 400
        
        # Validate pages
        if not pages_data:
            print("[POST] ERROR: No pages selected")
            return {'success': False, 'error': 'At least one page must be selected'}, 400
        
        print(f"[POST] Validation passed - creating post")
        
        # CHECK PAGE ACCESS LEVELS BEFORE CREATING POST
        # This is crucial - we need to know if ANY page requires approval
        print(f"[POST] Checking access levels for {len(pages_data)} page(s)")
        pages_to_post = []  # Store validated pages
        any_requires_approval = False
        tiktok_targets = []
        
        for page_info in pages_data:
            # Handle both object format {id, name, ...} and simple id format
            if isinstance(page_info, dict):
                page_id = page_info.get('id')
            else:
                page_id = page_info
            
            # Convert string ID to integer if needed
            try:
                page_id = int(page_id)
            except (ValueError, TypeError):
                print(f"[POST] Warning: Could not convert page ID to int: {page_id}")
                continue
            
            if not page_id:
                print(f"[POST] Warning: Could not extract page ID from {page_info}")
                continue
            
            # Verify user has access to this page
            page = ConnectedPage.query.get(page_id)
            if not page:
                print(f"[POST] Page {page_id} not found")
                continue
            
            print(f"[POST] Processing page {page_id}: {page.page_name} (user_id={page.user_id})")
            
            # Check if user owns the page
            page_belongs_to_user = page.user_id == user_id
            print(f"[POST]   - User owns page: {page_belongs_to_user}")
            
            # Check user's access level for this page
            has_team_access = False
            page_access_level = 'none'
            
            if page_belongs_to_user:
                page_access_level = 'owner'
                print(f"[POST]   - Access level: owner")
            else:
                # User doesn't own it, check team access
                print(f"[POST]   - Checking team access...")
                team_pages = get_accessible_team_channels(user_id)
                print(f"[POST]   - User has {len(team_pages)} accessible team pages")
                has_team_access = any(tp.id == page_id for tp in team_pages)
                print(f"[POST]   - Has team access to this page: {has_team_access}")
                
                if has_team_access:
                    page_access_level = get_user_channel_access(user_id, page_id)
                    print(f"[POST]   - Access level: {page_access_level}")
            
            if not page_belongs_to_user and not has_team_access:
                print(f"[POST] User {user_id} does not have access to page {page_id}")
                continue
            
            # Check if this page requires approval
            if page_access_level == 'approval_required':
                any_requires_approval = True
                print(f"[POST] Page {page_id} requires approval (access_level={page_access_level})")
            
            # Store valid page with its access level
            pages_to_post.append({
                'page_id': page_id,
                'page': page,
                'access_level': page_access_level
            })

            if page.platform.lower() == 'tiktok':
                tiktok_targets.append(page)
        
        if not pages_to_post:
            print("[POST] ERROR: No valid pages to post to")
            return {'success': False, 'error': 'No valid pages selected'}, 400

        if tiktok_targets and not tiktok_can_publish():
            missing_scopes = missing_tiktok_publish_scopes()
            missing_list = ', '.join(sorted(missing_scopes))
            print(f"[POST] TikTok scopes missing: {missing_list}")
            return {
                'success': False,
                'error': (
                    'TikTok cannot publish yet because the sandbox app is missing '
                    f"these scopes: {missing_list}. Enable them in the TikTok developer portal "
                    'or switch to a production app before adding TikTok to a post.'
                )
            }, 400
        
        # FORCE APPROVAL if any page requires approval
        if any_requires_approval:
            submit_for_approval = True
            print(f"[POST] At least one page requires approval - forcing submit_for_approval=True")
        
        # Create post object
        print(f"[POST] Creating post: publish_type='{publish_type}', scheduled_time_str='{scheduled_time_str}', submit_for_approval={submit_for_approval}")
        
        # Determine initial status based on submit_for_approval
        if submit_for_approval:
            initial_status = 'draft'
            approval_status = 'pending'
        else:
            initial_status = 'sent' if publish_type == 'now' else 'scheduled'
            approval_status = None
        
        post = Post(
            user_id=user_id,
            content=caption,
            caption=caption,
            post_icon=post_icon,
            status=initial_status,
            sent_time=datetime.utcnow() if (publish_type == 'now' and not submit_for_approval) else None,
            submitted_by_user_id=user_id if submit_for_approval else None,
            approval_status=approval_status,
            approval_requested_at=datetime.utcnow() if submit_for_approval else None
        )
        
        # Set scheduled time if needed
        if publish_type == 'scheduled' and scheduled_time_str:
            try:
                from datetime import datetime as dt_class
                # Parse time as Vietnam timezone (UTC+7)
                scheduled_dt = dt_class.strptime(scheduled_time_str, '%Y-%m-%d %H:%M')
                print(f"[POST] Parsed datetime from string: {scheduled_dt}")
                # Convert to UTC for storage
                scheduled_dt_utc = scheduled_dt - timedelta(hours=7)
                post.scheduled_time = scheduled_dt_utc
                if not submit_for_approval:
                    post.status = 'scheduled'
                print(f"[POST] Scheduled time set (UTC): {scheduled_dt_utc}")
            except Exception as e:
                print(f"[POST] Error parsing scheduled time: {e}")
                return {'success': False, 'error': 'Invalid scheduled time format'}, 400
        else:
            print(f"[POST] NOT setting scheduled time (publish_type={publish_type})")
        
        print(f"[POST] Post object before flush - sent_time: {post.sent_time}, scheduled_time: {post.scheduled_time}, status: {post.status}, approval_status: {post.approval_status}")
        db.session.add(post)
        db.session.flush()  # Get the post ID
        
        print(f"[POST] Post created with ID: {post.id}, status: {post.status}, approval_status: {post.approval_status}")
        print(f"[POST] Actual DB values - sent_time: {post.sent_time}, scheduled_time: {post.scheduled_time}")
        
        # Handle media uploads
        print(f"[POST] DEBUG: request.files keys: {list(request.files.keys())}")
        print(f"[POST] DEBUG: request.files: {request.files}")
        media_files = []
        saved_media_paths = []
        has_video_media = False
        for key in request.files:
            print(f"[POST] DEBUG: Checking key '{key}', startswith('media_'): {key.startswith('media_')}")
            if key.startswith('media_'):
                media_files.append(request.files[key])
                print(f"[POST] DEBUG: Added file from key '{key}'")
        
        print(f"[POST] Processing {len(media_files)} media files...")
        media_count = 0
        for media_file in media_files:
            print(f"[POST] Media file object: {media_file}")
            print(f"[POST] Media filename: {media_file.filename if media_file else 'None'}")
            print(f"[POST] Media content_type: {media_file.content_type if media_file else 'None'}")
            
            if media_file:
                # Handle case where filename might be empty - generate one from content type
                filename = media_file.filename
                if not filename or filename.strip() == '':
                    # Generate filename from content type
                    ext = 'dat'
                    if media_file.content_type:
                        if media_file.content_type.startswith('image/'):
                            ext = media_file.content_type.split('/')[1].split(';')[0]
                        elif media_file.content_type.startswith('video/'):
                            ext = media_file.content_type.split('/')[1].split(';')[0]
                    filename = f"upload_{secrets.token_hex(4)}.{ext}"
                    print(f"[POST] Generated filename: {filename}")
                
                # Save file to uploads folder
                safe_filename = f"post_{post.id}_{secrets.token_hex(8)}_{filename}"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
                print(f"[POST] Saving to: {filepath}")
                media_file.save(filepath)
                saved_media_paths.append(filepath)
                
                # Determine media type
                if media_file.content_type and media_file.content_type.startswith('image/'):
                    media_type = 'image'
                elif media_file.content_type and media_file.content_type.startswith('video/'):
                    media_type = 'video'
                    has_video_media = True
                else:
                    media_type = 'file'
                
                # Create PostMedia record
                post_media = PostMedia(
                    post_id=post.id,
                    media_url=f'/uploads/{safe_filename}',
                    media_type=media_type,
                    file_size=os.path.getsize(filepath) if os.path.exists(filepath) else 0
                )
                db.session.add(post_media)
                media_count += 1
                print(f"[POST] Media saved successfully: {safe_filename} ({media_type}, {post_media.file_size} bytes)")
            else:
                print(f"[POST] WARNING: Media file object is None or empty")

        if tiktok_targets and not has_video_media:
            print("[POST] TikTok requires a video attachment - aborting")
            for path in saved_media_paths:
                try:
                    os.remove(path)
                except OSError:
                    pass
            db.session.rollback()
            return {'success': False, 'error': 'TikTok posts must include a video file. Please attach an MP4 before selecting TikTok.'}, 400
        
        # Create associations with connected pages (already validated above)
        page_count = 0
        for page_data in pages_to_post:
            page_id = page_data['page_id']
            page = page_data['page']
            page_access_level = page_data['access_level']
            
            # Create association
            association = PostPageAssociation(
                post_id=post.id,
                page_id=page_id,
                status='sent' if publish_type == 'now' else 'pending'
            )
            db.session.add(association)
            page_count += 1
            print(f"[POST] Associated with page: {page.page_name} ({page.platform}) [access_level={page_access_level}]")
        
        # Commit all changes
        db.session.commit()
        
        publish_now_success = 0
        publish_now_errors = []

        # If "Post Now" is selected AND not submitting for approval, actually publish to Facebook/TikTok immediately
        if publish_type == 'now' and not submit_for_approval:
            print(f"[POST] Publishing immediately to {page_count} page(s)")
            for page_info in pages_data:
                if isinstance(page_info, dict):
                    page_id = page_info.get('id')
                else:
                    page_id = page_info
                
                # Get page and verify it exists (no need to check ownership here since we already did in association)
                page = ConnectedPage.query.get(page_id)
                if not page:
                    print(f"[POST] Page {page_id} not found when publishing")
                    continue
                
                # Use the page access token (not user token) to publish to the page
                if not page.page_access_token:
                    print(f"[POST] No page access token for page {page_id} - user may need to reconnect")
                    publish_now_errors.append(f"{page.page_name}: missing page access token")
                    continue
                
                print(f"[POST] Using page access token for page {page_id}")
                platform_name = page.platform.lower()
                if platform_name in ['facebook', 'instagram']:
                    platform_post_id = publish_to_facebook(page.platform_page_id, post, page.page_access_token)
                elif platform_name == 'tiktok':
                    missing_scopes = missing_tiktok_publish_scopes()
                    if missing_scopes:
                        readable_scopes = ', '.join(sorted(missing_scopes))
                        msg = (
                            f"{page.page_name}: TikTok sandbox missing scopes ({readable_scopes}) so video upload is disabled"
                        )
                        print(f"[POST] {msg}")
                        publish_now_errors.append(msg)
                        continue
                    platform_post_id = publish_to_tiktok(page.platform_page_id, post, page.page_access_token)
                else:
                    platform_post_id = None
                if platform_post_id:
                    # Update association with platform_post_id
                    assoc = PostPageAssociation.query.filter_by(post_id=post.id, page_id=page_id).first()
                    if assoc:
                        assoc.platform_post_id = platform_post_id
                        assoc.status = 'sent'
                    db.session.commit()
                    print(f"[POST] Published to {platform_name}, ID: {platform_post_id}")
                    publish_now_success += 1
                else:
                    print(f"[POST] Failed to publish to {platform_name}")
                    publish_now_errors.append(f"{page.page_name}: publish failed")
        elif submit_for_approval:
            print(f"[POST] NOT publishing - post submitted for approval (status={post.status}, approval_status={post.approval_status})")
        
        # Verify what was stored
        post_reload = Post.query.get(post.id)
        print(f"[POST] Post saved successfully. Media: {media_count}, Pages: {page_count}")
        print(f"[POST] DB verification - status: {post_reload.status}, approval_status: {post_reload.approval_status}, sent_time: {post_reload.sent_time}, scheduled_time: {post_reload.scheduled_time}")
        print("[POST] ========== CREATE POST END ==========\n")
        
        # Build success message
        media_info = f" with {media_count} media file(s)" if media_count > 0 else ""
        
        if submit_for_approval:
            message = f'Post submitted for approval{media_info}'
        elif publish_type == 'now':
            if publish_now_success and not publish_now_errors:
                message = f'Post published to {publish_now_success} channel(s){media_info}'
            elif publish_now_success and publish_now_errors:
                error_text = ' | '.join(publish_now_errors)
                message = f'Post published to {publish_now_success} channel(s){media_info}, but some failed: {error_text}'
            else:
                error_text = ' | '.join(publish_now_errors) if publish_now_errors else 'No channels accepted the publish request'
                message = f'Post saved{media_info} but not published: {error_text}'
        else:
            message = f'Post scheduled for {page_count} page(s){media_info}'
        response = {'success': True, 'post_id': post.id, 'message': message}
        print(f"[POST] Returning response: {response}")
        return response
        
    except Exception as e:
        db.session.rollback()
        print(f"[POST] Error creating post: {e}")
        print("[POST] ========== CREATE POST END (ERROR) ==========\n")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts', methods=['GET'])
@login_required
def get_posts():
    """Fetch all posts for pages the user has access to (for calendar display)"""
    try:
        user_id = session.get('user_id')
        print(f"[GET /api/posts] Fetching posts for user {user_id}")
        
        # Get user's own connected pages
        owned_pages = ConnectedPage.query.filter_by(user_id=user_id, is_active=True).all()
        
        # Get team pages user has access to
        team_pages = get_accessible_team_channels(user_id)
        
        # Combine all pages
        all_pages = owned_pages + [p for p in team_pages if p.id not in set(p.id for p in owned_pages)]
        accessible_page_ids = [p.id for p in all_pages]
        
        if not accessible_page_ids:
            print(f"[GET /api/posts] User has no accessible pages")
            return {'success': True, 'posts': []}
        
        # Get all posts published to any of these pages (not just user's own posts)
        posts = db.session.query(Post).join(
            PostPageAssociation, Post.id == PostPageAssociation.post_id
        ).filter(
            PostPageAssociation.page_id.in_(accessible_page_ids),
            Post.status != 'draft'  # Don't show draft posts in calendar
        ).order_by(Post.scheduled_time.desc()).distinct().all()
        
        print(f"[GET /api/posts] Retrieved {len(posts)} total posts from database")
        
        # Sort by most recent date (scheduled_time if exists, otherwise sent_time)
        posts = sorted(posts, key=lambda p: (
            p.scheduled_time if p.scheduled_time else p.sent_time or datetime.min
        ), reverse=True)
        
        posts_data = []
        for post in posts:
            # Get associated pages
            page_associations = PostPageAssociation.query.filter_by(post_id=post.id).all()
            pages_info = []
            first_platform_post_id = None
            first_view_url = None
            for assoc in page_associations:
                page = assoc.connected_page
                if not first_platform_post_id and assoc.platform_post_id:
                    first_platform_post_id = assoc.platform_post_id
                view_url = build_platform_view_url(page, assoc.platform_post_id)
                if not first_view_url and view_url:
                    first_view_url = view_url
                pages_info.append({
                    'id': page.id,
                    'name': page.page_name,
                    'platform': page.platform,
                    'platform_page_id': page.platform_page_id,
                    'page_username': page.page_username,
                    'platform_post_id': assoc.platform_post_id,
                    'view_url': view_url
                })
            
            # Get media
            media_info = []
            for media in post.media:
                media_info.append({
                    'id': media.id,
                    'url': media.media_url,
                    'type': media.media_type,
                    'size': media.file_size
                })
            
            display_date = post.scheduled_time or post.sent_time or post.created_at
            print(f"[GET /api/posts] Post {post.id}: status={post.status}, date={display_date}, pages={len(pages_info)}")
            
            normalized_status = normalize_post_status_value(post.status)
            posts_data.append({
                'id': post.id,
                'caption': post.caption,
                'icon': post.post_icon,
                'status': normalized_status,
                'scheduled_time': post.scheduled_time.isoformat() + 'Z' if post.scheduled_time else None,
                'sent_time': post.sent_time.isoformat() + 'Z' if post.sent_time else None,
                'created_at': post.created_at.isoformat() + 'Z',
                'pages': pages_info,
                'media': media_info,
                'platform_post_id': first_platform_post_id,
                'view_url': first_view_url
            })
        
        print(f"[GET /api/posts] Returning {len(posts_data)} posts to client")
        return {'success': True, 'posts': posts_data}
    except Exception as e:
        print(f"[GET /api/posts] Error fetching posts: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts/<int:post_id>/analytics', methods=['GET'])
@login_required
def get_post_analytics(post_id):
    """Fetch analytics for a specific post"""
    try:
        user_id = session.get('user_id')
        
        # Get the post
        post = Post.query.filter_by(id=post_id, user_id=user_id).first()
        if not post:
            return {'success': False, 'error': 'Post not found'}, 404
        
        # Get analytics from all page associations
        associations = PostPageAssociation.query.filter_by(post_id=post_id).all()
        
        # Aggregate analytics from all associations
        total_likes = 0
        total_comments = 0
        total_shares = 0
        total_impressions = 0
        total_clicks = 0
        total_reach = 0
        total_saves = 0
        
        for assoc in associations:
            # Get the analytics for this association (first one if multiple)
            analytics = PostAnalytics.query.filter_by(post_page_association_id=assoc.id).first()
            if analytics:
                total_likes += analytics.likes or 0
                total_comments += analytics.comments or 0
                total_shares += analytics.shares or 0
                total_impressions += analytics.impressions or 0
                total_clicks += analytics.clicks or 0
                total_reach += analytics.reach or 0
                total_saves += analytics.saves or 0
        
        return {
            'success': True,
            'analytics': {
                'likes': total_likes,
                'comments': total_comments,
                'shares': total_shares,
                'impressions': total_impressions,
                'clicks': total_clicks,
                'reach': total_reach,
                'saves': total_saves
            }
        }
    except Exception as e:
        print(f"[GET /api/posts/{post_id}/analytics] Error: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/pages/<int:page_id>/import-history', methods=['POST'])
@login_required
def trigger_page_history_import(page_id):
    """Queue a background job to import a page's history."""
    try:
        user_id = session.get('user_id')
        page = ConnectedPage.query.get(page_id)
        if not page or not user_can_access_page(user_id, page):
            return {'success': False, 'error': 'Page not found or inaccessible'}, 404

        payload = request.get_json() or {}
        max_posts = payload.get('max_posts')
        if max_posts:
            try:
                max_posts = max(1, int(max_posts))
            except (TypeError, ValueError):
                max_posts = None

        job = enqueue_page_import_job(page, user_id, scope='manual-refresh', max_posts=max_posts)
        if not job:
            return {'success': False, 'error': 'Missing page access token. Reconnect this page first.'}, 400

        return {'success': True, 'job': job.to_dict(), 'page': {'id': page.id, 'name': page.page_name}}
    except Exception as e:
        print(f"[IMPORT] Error queueing job for page {page_id}: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/pages/<int:page_id>/import-history', methods=['GET'])
@login_required
def get_page_history_import_status(page_id):
    """Return latest job info for the requested page."""
    try:
        user_id = session.get('user_id')
        page = ConnectedPage.query.get(page_id)
        if not page or not user_can_access_page(user_id, page):
            return {'success': False, 'error': 'Page not found or inaccessible'}, 404

        job = get_latest_import_job_for_page(page_id)
        return {'success': True, 'page': {'id': page.id, 'name': page.page_name}, 'job': job.to_dict() if job else None}
    except Exception as e:
        print(f"[IMPORT] Error fetching job status for page {page_id}: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts/refresh/historical', methods=['POST'])
@login_required
def refresh_historical_posts():
    """Queue background import jobs for all connected pages."""
    try:
        user_id = session.get('user_id')
        pages = ConnectedPage.query.filter_by(user_id=user_id).all()

        if not pages:
            return {'success': False, 'error': 'No connected pages found'}, 400

        queued = []
        skipped = []
        for page in pages:
            platform_name = (page.platform or '').lower()
            if platform_name not in ['facebook', 'tiktok']:
                skipped.append({'page': page.page_name, 'reason': 'Platform not supported'})
                continue

            job = enqueue_page_import_job(page, user_id, scope='manual-refresh')
            if job:
                queued.append({'page': page.page_name, 'job': job.to_dict()})
                print(f"[REFRESH] Queued import job {job.id} for page {page.page_name}")
            else:
                skipped.append({'page': page.page_name, 'reason': 'Missing page access token'})

        return {
            'success': True,
            'jobs_created': len(queued),
            'queued': queued,
            'skipped': skipped
        }

    except Exception as e:
        print(f"[REFRESH] Error refreshing historical posts: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/analytics/refresh', methods=['POST'])
@login_required
def refresh_analytics_manually():
    """Manually trigger analytics refresh for all posts"""
    try:
        from tasks import refresh_all_post_analytics
        
        result = refresh_all_post_analytics()
        return {
            'success': True,
            'message': f"Refreshed analytics. Success: {result.get('success', 0)}, Failed: {result.get('failed', 0)}",
            'details': result
        }
    except ImportError:
        return {
            'success': False,
            'error': 'Tasks module not available. APScheduler may not be installed.'
        }, 500
    except Exception as e:
        print(f"[ANALYTICS] Error refreshing analytics: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/pages/diagnose/<int:page_id>', methods=['GET'])
@login_required
def diagnose_page(page_id):
    """Diagnose issues with a specific page's access token and permissions"""
    try:
        user_id = session.get('user_id')
        
        # Get the page
        page = ConnectedPage.query.filter_by(id=page_id, user_id=user_id).first()
        if not page:
            return {'success': False, 'error': 'Page not found'}, 404
        
        platform_name = page.platform.lower()
        
        if not page.page_access_token:
            return {'success': False, 'error': 'No page access token available'}, 400
        
        diagnosis = {
            'page_name': page.page_name,
            'platform_page_id': page.platform_page_id,
            'platform': platform_name,
            'tests': {}
        }
        
        if platform_name == 'facebook':
            try:
                url = f"https://graph.facebook.com/v18.0/{page.platform_page_id}"
                response = requests.get(url, params={
                    'fields': 'id,name',
                    'access_token': page.page_access_token
                }, timeout=10)
                if response.status_code == 200:
                    diagnosis['tests']['token_validity'] = 'valid'
                    diagnosis['tests']['token_info'] = response.json()
                else:
                    diagnosis['tests']['token_validity'] = f'invalid ({response.status_code})'
                    diagnosis['tests']['token_error'] = response.json().get('error', {}).get('message', response.text)
            except Exception as e:
                diagnosis['tests']['token_validity'] = f'error - {str(e)}'
            
            try:
                url = f"https://graph.facebook.com/v18.0/{page.platform_page_id}/posts"
                response = requests.get(url, params={
                    'fields': 'id',
                    'limit': 1,
                    'access_token': page.page_access_token
                }, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    diagnosis['tests']['posts_fetch'] = 'success'
                    diagnosis['tests']['posts_count'] = len(data.get('data', []))
                else:
                    diagnosis['tests']['posts_fetch'] = f'failed ({response.status_code})'
                    diagnosis['tests']['posts_error'] = response.json().get('error', {}).get('message', response.text)
            except Exception as e:
                diagnosis['tests']['posts_fetch'] = f'error - {str(e)}'
            
            try:
                url = f"https://graph.facebook.com/v18.0/{page.platform_page_id}/posts"
                response = requests.get(url, params={
                    'fields': 'id,message,caption,created_time,type,permalink_url',
                    'limit': 1,
                    'access_token': page.page_access_token
                }, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    diagnosis['tests']['posts_full_fetch'] = 'success'
                    diagnosis['tests']['posts_full_count'] = len(data.get('data', []))
                    if data.get('data'):
                        diagnosis['tests']['sample_post'] = data['data'][0]
                else:
                    diagnosis['tests']['posts_full_fetch'] = f'failed ({response.status_code})'
                    diagnosis['tests']['posts_full_error'] = response.json().get('error', {}).get('message', response.text)
            except Exception as e:
                diagnosis['tests']['posts_full_fetch'] = f'error - {str(e)}'
        else:
            try:
                get_tiktok_accounts(page.page_access_token)
                diagnosis['tests']['token_validity'] = 'valid'
            except TikTokApiError as exc:
                diagnosis['tests']['token_validity'] = f'error - {exc}'
            try:
                videos = list_tiktok_posts(page.platform_page_id, page.page_access_token, max_pages=1)
                diagnosis['tests']['posts_fetch'] = 'success'
                diagnosis['tests']['posts_count'] = len(videos)
                if videos:
                    diagnosis['tests']['sample_post'] = videos[0]
            except TikTokApiError as exc:
                diagnosis['tests']['posts_fetch'] = f'error - {exc}'
        
        return {'success': True, 'diagnosis': diagnosis}
        
    except Exception as e:
        print(f"[DIAGNOSE] Error diagnosing page {page_id}: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts/<int:post_id>', methods=['PUT'])
@login_required
def update_post(post_id):
    """Update an existing post"""
    try:
        print(f"\n[PUT] ========== UPDATE POST {post_id} START ==========")
        user_id = session.get('user_id')
        
        # Get the post - don't filter by user_id to allow admins/owners to edit team posts
        post = Post.query.filter_by(id=post_id).first()
        if not post:
            return {'success': False, 'error': 'Post not found'}, 404
        
        # Check authorization: must be post creator OR page owner OR team admin
        is_creator = (post.user_id == user_id)
        is_page_owner = False
        is_team_admin = False
        
        # Check if user is owner of any page this post is associated with
        assoc = PostPageAssociation.query.filter_by(post_id=post_id).first()
        if assoc:
            page = assoc.connected_page
            is_page_owner = (page.user_id == user_id)
            
            # Check if user is team admin
            if page.team_id:
                try:
                    is_team_admin = check_admin_access(page.team_id, user_id)
                except:
                    is_team_admin = False
        
        if not is_creator and not is_page_owner and not is_team_admin:
            return {'success': False, 'error': 'Unauthorized - only post creator, page owner, or team admin can edit'}, 403
        
        # Get data from JSON or form
        data = {}
        if request.is_json:
            data = request.get_json() or {}
        else:
            data = request.form.to_dict()
        
        # Extract fields
        caption = data.get('caption', '').strip()
        pages_data = data.get('pages', [])
        media_to_delete = data.get('media_to_delete', [])
        
        # Parse pages if it's a JSON string (from FormData)
        if isinstance(pages_data, str):
            try:
                import json
                pages_data = json.loads(pages_data)
            except:
                return {'success': False, 'error': 'Invalid pages data'}, 400
        
        if isinstance(media_to_delete, str):
            media_to_delete = media_to_delete.strip()
            if media_to_delete:
                try:
                    import json
                    media_to_delete = json.loads(media_to_delete)
                except Exception:
                    media_to_delete = [media_to_delete]
            else:
                media_to_delete = []
        elif media_to_delete is None:
            media_to_delete = []

        if not isinstance(media_to_delete, list):
            media_to_delete = [media_to_delete]

        publish_type = data.get('publish_type', 'now')
        status = data.get('status', '')
        scheduled_time_str = data.get('scheduled_time', '')
        
        print(f"[PUT] Updating post {post_id}: caption length={len(caption)}, pages count={len(pages_data) if pages_data else 0}")
        print(f"[PUT] Pages data: {pages_data}")
        print(f"[PUT] publish_type={publish_type}, status={status}, scheduled_time={scheduled_time_str}")
        
        # Validate input
        if not caption:
            return {'success': False, 'error': 'Caption is required'}, 400
        
        # Validate pages
        if not pages_data or len(pages_data) == 0:
            return {'success': False, 'error': 'At least one page must be selected'}, 400
        
        # Update post content
        post.caption = caption
        post.content = caption
        
        # Determine status based on publish_type or explicit status
        if publish_type == 'scheduled' or status in ['scheduled', 'publishing']:
            post.status = 'scheduled'
        elif publish_type == 'now':
            post.status = 'draft'  # Will be published immediately
        elif status:
            post.status = status
        # else: keep existing status when not specified
        
        print(f"[PUT] Post status set to: {post.status}")
        
        # Update scheduled time if needed
        if (publish_type == 'scheduled' or post.status in ['scheduled', 'publishing']) and scheduled_time_str:
            try:
                from datetime import datetime as dt_class
                # Parse time as Vietnam timezone (UTC+7)
                scheduled_dt = dt_class.strptime(scheduled_time_str, '%Y-%m-%d %H:%M')
                # Convert to UTC for storage
                scheduled_dt_utc = scheduled_dt - timedelta(hours=7)
                post.scheduled_time = scheduled_dt_utc
                post.status = 'scheduled'
                print(f"[PUT] Scheduled time updated: {scheduled_dt_utc}")
            except Exception as e:
                print(f"[PUT] Error parsing scheduled time: {e}")
                return {'success': False, 'error': 'Invalid scheduled time format'}, 400
        elif publish_type == 'now':
            # Only clear scheduled_time when explicitly posting now
            post.scheduled_time = None
        # else: keep existing scheduled_time when not specified
        
        print(f"[PUT] Final post status: {post.status}, scheduled_time: {post.scheduled_time}")

        # Process media deletions
        media_ids_to_delete = []
        for media_id in media_to_delete:
            try:
                media_ids_to_delete.append(int(media_id))
            except (TypeError, ValueError):
                print(f"[PUT] Skipping invalid media_to_delete entry: {media_id}")

        if media_ids_to_delete:
            removed_count = 0
            for media_id in media_ids_to_delete:
                media_obj = PostMedia.query.filter_by(id=media_id, post_id=post.id).first()
                if not media_obj:
                    print(f"[PUT] Media ID {media_id} not found or not attached to post {post.id}")
                    continue

                media_url_path = media_obj.media_url or ''
                if media_url_path:
                    filename = os.path.basename(media_url_path)
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            print(f"[PUT] Removed media file {filepath}")
                    except OSError as exc:
                        print(f"[PUT] Warning: Could not remove media file {filepath}: {exc}")

                db.session.delete(media_obj)
                removed_count += 1

            print(f"[PUT] Removed {removed_count} existing media item(s) from post {post.id}")
        
        # Handle media uploads (new media files)
        media_files = []
        for key in request.files:
            if key.startswith('media_'):
                media_files.append(request.files[key])
        
        print(f"[PUT] Processing {len(media_files)} new media files...")
        media_count = 0
        for media_file in media_files:
            if media_file:
                # Handle case where filename might be empty - generate one from content type
                filename = media_file.filename
                if not filename or filename.strip() == '':
                    # Generate filename from content type
                    ext = 'dat'
                    if media_file.content_type:
                        if media_file.content_type.startswith('image/'):
                            ext = media_file.content_type.split('/')[1].split(';')[0]
                        elif media_file.content_type.startswith('video/'):
                            ext = media_file.content_type.split('/')[1].split(';')[0]
                    filename = f"upload_{secrets.token_hex(4)}.{ext}"
                    print(f"[PUT] Generated filename: {filename}")
                
                # Save file to uploads folder
                safe_filename = f"post_{post.id}_{secrets.token_hex(8)}_{filename}"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
                print(f"[PUT] Saving to: {filepath}")
                media_file.save(filepath)
                
                # Determine media type
                if media_file.content_type and media_file.content_type.startswith('image/'):
                    media_type = 'image'
                elif media_file.content_type and media_file.content_type.startswith('video/'):
                    media_type = 'video'
                else:
                    media_type = 'file'
                
                # Create PostMedia record
                post_media = PostMedia(
                    post_id=post.id,
                    media_url=f'/uploads/{safe_filename}',
                    media_type=media_type,
                    file_size=os.path.getsize(filepath) if os.path.exists(filepath) else 0
                )
                db.session.add(post_media)
                media_count += 1
                print(f"[PUT] New media added: {safe_filename} ({media_type}, {post_media.file_size} bytes)")
        
        # Update page associations - delete old ones and create new ones
        PostPageAssociation.query.filter_by(post_id=post.id).delete()
        
        page_count = 0
        for page_info in pages_data:
            if isinstance(page_info, dict):
                page_id = page_info.get('id')
            else:
                page_id = page_info
            
            # Convert page_id to integer (frontend sends as string)
            try:
                page_id = int(page_id)
            except (ValueError, TypeError):
                print(f"[PUT] Invalid page_id: {page_id}")
                continue
            
            # Verify user has access to this page (either owns it or has team access)
            page = ConnectedPage.query.get(page_id)
            if not page:
                print(f"[PUT] Page {page_id} not found in database")
                continue
            
            # Check if user owns the page
            page_belongs_to_user = page.user_id == user_id
            
            # Check if user has team access to the page
            has_team_access = False
            if not page_belongs_to_user:
                # User doesn't own it, check team access
                team_pages = get_accessible_team_channels(user_id)
                has_team_access = any(tp.id == page_id for tp in team_pages)
            
            if not page_belongs_to_user and not has_team_access:
                print(f"[PUT] User {user_id} does not have access to page {page_id}")
                continue
            
            # Create association
            association = PostPageAssociation(
                post_id=post.id,
                page_id=page_id,
                status=post.status
            )
            db.session.add(association)
            page_count += 1
            print(f"[PUT] Associated with page: {page.page_name} ({page.platform})")
        
        # Commit all changes
        db.session.commit()
        print(f"[PUT] Post updated successfully. New media: {media_count}, Pages: {page_count}")
        print("[PUT] ========== UPDATE POST END ==========\n")
        
        return {'success': True, 'post_id': post.id, 'message': f'Post updated for {page_count} page(s)'}
        
    except Exception as e:
        db.session.rollback()
        print(f"[PUT] Error updating post: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts/<int:post_id>', methods=['DELETE'])
@login_required
def delete_post(post_id):
    """Delete a post"""
    try:
        user_id = session.get('user_id')
        post = Post.query.get(post_id)
        
        if not post:
            return {'success': False, 'error': 'Post not found'}, 404
        
        # Check authorization: must be post creator OR page owner OR team member
        is_creator = (post.user_id == user_id)
        is_page_owner = False
        is_team_member = False
        
        # Check if user is owner of any page this post is associated with
        assoc = PostPageAssociation.query.filter_by(post_id=post_id).first()
        if assoc:
            page = assoc.connected_page
            is_page_owner = (page.user_id == user_id)
            
            # Check if user is ANY member (owner, admin, or regular member) of the team
            if page.team_id:
                try:
                    is_team_member = check_team_member_access(page.team_id, user_id)
                except:
                    is_team_member = False
        
        # For scheduled posts: Allow page owners and ANY team members to delete
        # For other posts: Allow creator, page owner, or team members to delete
        if post.status in ['scheduled', 'publishing']:
            # Page owners and team members can delete scheduled posts
            if is_page_owner or is_team_member:
                print(f"[DELETE] User {user_id} authorized to delete scheduled post {post_id} (page_owner={is_page_owner}, team_member={is_team_member})")
            elif not is_creator:
                return {'success': False, 'error': 'Only page owners and team members can delete scheduled posts'}, 403
        
        if not is_creator and not is_page_owner and not is_team_member:
            return {'success': False, 'error': 'Unauthorized - only post creator, page owner, or team member can delete'}, 403
        
        db.session.delete(post)
        db.session.commit()
        
        return {'success': True, 'message': 'Post deleted successfully'}
    except Exception as e:
        db.session.rollback()
        print(f"[DELETE] Error deleting post: {e}")
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/posts/<int:post_id>/publish', methods=['POST'])
@login_required
def publish_post_now(post_id):
    """Publish a scheduled post immediately to Facebook/Instagram"""
    try:
        user_id = session.get('user_id')
        post = Post.query.filter_by(id=post_id, user_id=user_id).first()
        
        if not post:
            return {'success': False, 'error': 'Post not found'}, 404
        
        # Get post associations (pages to post to)
        associations = PostPageAssociation.query.filter_by(post_id=post.id).all()
        if not associations:
            return {'success': False, 'error': 'No pages associated with this post'}, 400
        
        # Publish to each page
        published_count = 0
        errors = []
        
        for assoc in associations:
            page = assoc.connected_page
            if not page:
                errors.append(f"Page {assoc.page_id} not found")
                continue
            
            # Use the page access token (not user token) to publish to the page
            if not page.page_access_token:
                errors.append(f"No page access token for {page.page_name} - user may need to reconnect")
                continue
            
            try:
                platform_name = page.platform.lower()
                platform_post_id = None
                if platform_name in ['facebook', 'instagram']:
                    print(f"[PUBLISH] Publishing post {post_id} to {platform_name} page {page.platform_page_id}")
                    platform_post_id = publish_to_facebook(page.platform_page_id, post, page.page_access_token)
                elif platform_name == 'tiktok':
                    missing_scopes = missing_tiktok_publish_scopes()
                    if missing_scopes:
                        readable_scopes = ', '.join(sorted(missing_scopes))
                        msg = (
                            f"{page.page_name}: TikTok sandbox missing scopes ({readable_scopes}) so video upload is disabled"
                        )
                        print(f"[PUBLISH] {msg}")
                        errors.append(msg)
                        continue
                    print(f"[PUBLISH] Publishing post {post_id} to TikTok account {page.platform_page_id}")
                    platform_post_id = publish_to_tiktok(page.platform_page_id, post, page.page_access_token)

                if platform_post_id:
                    assoc.platform_post_id = platform_post_id
                    assoc.status = 'sent'
                    published_count += 1
                    print(f"[PUBLISH] Post published to {platform_name}: {platform_post_id}")
                else:
                    errors.append(f"Failed to publish to {page.page_name}")
            except Exception as e:
                errors.append(f"Error publishing to {page.page_name}: {str(e)}")
                print(f"[PUBLISH] Error: {e}")
        
        # Update post status only if published to at least one page
        if published_count > 0:
            post.status = 'sent'
            post.sent_time = datetime.utcnow()
            # Clear any previously scheduled time so UI reflects actual publish time
            post.scheduled_time = None
            db.session.commit()
            
            message = f'Post published to {published_count} page(s)'
            if errors:
                message += f'. Errors: {" | ".join(errors)}'
            
            return {'success': True, 'message': message}
        else:
            db.session.rollback()
            return {'success': False, 'error': ' | '.join(errors) if errors else 'Failed to publish to any page'}, 400
    
    except Exception as e:
        db.session.rollback()
        print(f"[PUBLISH] Error publishing post: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


def publish_to_facebook(page_id, post, access_token):
    """Publish a post to Facebook/Instagram page with support for multiple images"""
    print(f"[PUBLISH] Publishing to page {page_id}")
    try:
        # Get media files
        media_files = PostMedia.query.filter_by(post_id=post.id).all()
        print(f"[PUBLISH] Found {len(media_files)} media files for post {post.id}")
        
        # Prepare post data
        data = {
            'message': post.caption or post.content,
            'access_token': access_token
        }
        
        # Handle media
        if media_files:
            # Separate images and videos
            images = [m for m in media_files if m.media_type == 'image']
            videos = [m for m in media_files if m.media_type == 'video']
            
            # Handle multiple images (carousel/album)
            if len(images) > 1:
                print(f"[PUBLISH] Uploading {len(images)} images as album/carousel")
                
                # Step 1: Upload all images and get their IDs
                attached_media = []
                for idx, media in enumerate(images):
                    media_url_path = media.media_url
                    if media_url_path.startswith('/uploads/'):
                        filename = media_url_path.replace('/uploads/', '')
                        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    else:
                        filepath = os.path.join(app.config['UPLOAD_FOLDER'], os.path.basename(media_url_path))
                    
                    if not os.path.exists(filepath):
                        print(f"[PUBLISH] WARNING: Image {idx+1} not found at {filepath}")
                        continue
                    
                    # Upload image (unpublished)
                    upload_url = f"https://graph.facebook.com/v18.0/{page_id}/photos"
                    upload_data = {
                        'published': 'false',
                        'access_token': access_token
                    }
                    
                    try:
                        with open(filepath, 'rb') as img_file:
                            files = {'source': img_file}
                            response = requests.post(upload_url, data=upload_data, files=files, timeout=30)
                            response.raise_for_status()
                            result = response.json()
                            
                            if 'id' in result:
                                attached_media.append({'media_fbid': result['id']})
                                print(f"[PUBLISH] Uploaded image {idx+1}: {result['id']}")
                            else:
                                print(f"[PUBLISH] No ID in image upload response: {result}")
                    except Exception as e:
                        print(f"[PUBLISH] Error uploading image {idx+1}: {e}")
                
                # Step 2: Create the post with all attached images
                if attached_media:
                    post_url = f"https://graph.facebook.com/v18.0/{page_id}/feed"
                    post_data = {
                        'message': data['message'],
                        'attached_media': str(attached_media).replace("'", '"'),
                        'access_token': access_token
                    }
                    
                    response = requests.post(post_url, data=post_data, timeout=30)
                    response.raise_for_status()
                    result = response.json()
                    
                    if 'id' in result:
                        print(f"[PUBLISH] Successfully published album with {len(attached_media)} images: {result['id']}")
                        return result['id']
                    else:
                        print(f"[PUBLISH] No ID in album post response: {result}")
                        return None
                else:
                    print(f"[PUBLISH] No images could be uploaded")
                    return None
            
            # Handle single image
            elif len(images) == 1:
                media = images[0]
                media_url_path = media.media_url
                
                if media_url_path.startswith('/uploads/'):
                    filename = media_url_path.replace('/uploads/', '')
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                else:
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], os.path.basename(media_url_path))
                
                print(f"[PUBLISH] Media file path: {filepath}")
                print(f"[PUBLISH] File exists: {os.path.exists(filepath)}")
                
                if os.path.exists(filepath):
                    print(f"[PUBLISH] Uploading single image directly to Facebook")
                    url = f"https://graph.facebook.com/v18.0/{page_id}/photos"
                    
                    with open(filepath, 'rb') as img_file:
                        files = {'source': img_file}
                        response = requests.post(url, data=data, files=files, timeout=30)
                        response.raise_for_status()
                        result = response.json()
                        
                        if 'id' in result or 'post_id' in result:
                            post_id = result.get('post_id') or result.get('id')
                            print(f"[PUBLISH] Successfully published image: {post_id}")
                            return post_id
                        else:
                            print(f"[PUBLISH] No ID in response: {result}")
                            return None
                else:
                    print(f"[PUBLISH] ERROR: Media file not found at {filepath}")
                    return None
            
            # Handle video (only first one)
            elif len(videos) > 0:
                media = videos[0]
                media_url_path = media.media_url
                
                if media_url_path.startswith('/uploads/'):
                    filename = media_url_path.replace('/uploads/', '')
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                else:
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], os.path.basename(media_url_path))
                
                if os.path.exists(filepath):
                    print(f"[PUBLISH] Uploading video directly to Facebook")
                    url = f"https://graph.facebook.com/v18.0/{page_id}/videos"
                    # Graph video uploads ignore `message`; send caption via `description`
                    video_payload = {
                        'description': post.caption or post.content or '',
                        'access_token': access_token
                    }
                    
                    with open(filepath, 'rb') as video_file:
                        files = {'source': video_file}
                        response = requests.post(url, data=video_payload, files=files, timeout=120)
                        response.raise_for_status()
                        result = response.json()
                        
                        if 'id' in result:
                            print(f"[PUBLISH] Successfully published video: {result['id']}")
                            return result['id']
                        else:
                            print(f"[PUBLISH] No ID in response: {result}")
                            return None
                else:
                    print(f"[PUBLISH] ERROR: Video file not found at {filepath}")
                    return None
        else:
            # No media - text-only post
            print(f"[PUBLISH] No media files - posting text only")
            url = f"https://graph.facebook.com/v18.0/{page_id}/feed"
            response = requests.post(url, data=data, timeout=15)
            response.raise_for_status()
            result = response.json()
            
            if 'id' in result:
                print(f"[PUBLISH] Successfully published text-only: {result['id']}")
                return result['id']
            else:
                print(f"[PUBLISH] No ID in response: {result}")
                return None
    
    except Exception as e:
        print(f"[PUBLISH] Error publishing to Facebook: {e}")
        import traceback
        traceback.print_exc()
        return None


def publish_to_tiktok(open_id, post, access_token):
    """Publish a TikTok video using the helper service."""
    print(f"[TIKTOK] Publishing to account {open_id}")
    if not tiktok_can_publish():
        missing_scopes = missing_tiktok_publish_scopes()
        readable_scopes = ', '.join(sorted(missing_scopes))
        print(f"[TIKTOK] Aborting publish - missing scopes: {readable_scopes}")
        return None
    media_files = PostMedia.query.filter_by(post_id=post.id).all()
    video_media = next((m for m in media_files if m.media_type == 'video'), None)

    if not video_media:
        print("[TIKTOK] Publishing requires a video attachment")
        return None

    media_url = video_media.media_url or ''
    cleanup_path = None

    try:
        if media_url.startswith('http'):
            response = requests.get(media_url, timeout=45)
            response.raise_for_status()
            suffix = os.path.splitext(media_url)[1] or '.mp4'
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            temp_file.write(response.content)
            temp_file.close()
            media_path = temp_file.name
            cleanup_path = media_path
        else:
            relative_path = media_url.lstrip('/')
            media_path = os.path.join(BASE_DIR, relative_path)
            if not os.path.exists(media_path):
                fallback = os.path.join(app.config['UPLOAD_FOLDER'], os.path.basename(media_url))
                media_path = fallback

        if not os.path.exists(media_path):
            print(f"[TIKTOK] Media file not found: {media_path}")
            return None

        caption = post.caption or post.content
        video_id = publish_tiktok_video(open_id, caption, media_path, access_token)
        print(f"[TIKTOK] Published video id={video_id}")
        return video_id
    except TikTokApiError as exc:
        print(f"[TIKTOK] API error: {exc} :: {getattr(exc, 'details', {})}")
        return None
    except requests.RequestException as exc:
        print(f"[TIKTOK] Failed to prepare media: {exc}")
        return None
    finally:
        if cleanup_path and os.path.exists(cleanup_path):
            try:
                os.remove(cleanup_path)
            except OSError:
                pass


# ======================== DRAFT & APPROVAL WORKFLOW ROUTES ========================

@app.route('/api/drafts', methods=['GET'])
@login_required
def get_drafts():
    """Get pending drafts for current user (admin/owner can see team member drafts)"""
    try:
        user_id = session.get('user_id')
        team_id = request.args.get('team_id', type=int)
        from sqlalchemy.orm import joinedload
        
        print(f"[DRAFTS] Fetching drafts for user {user_id}, team_id={team_id}")
        
        if team_id:
            # Get drafts submitted by team members to owner's pages (only for admins/owner)
            team = Team.query.get(team_id)
            if not team:
                print(f"[DRAFTS] Team {team_id} not found")
                return {'success': False, 'error': 'Team not found'}, 404
            
            print(f"[DRAFTS] Team found: {team.name}, owner_id={team.owner_id}")
            
            # Check if user is admin or owner
            if team.owner_id != user_id:
                team_member = TeamMember.query.filter_by(team_id=team_id, user_id=user_id).first()
                if not team_member or team_member.role != 'admin':
                    print(f"[DRAFTS] User {user_id} not authorized - not owner or admin")
                    return {'success': False, 'error': 'Unauthorized'}, 403
                print(f"[DRAFTS] User is team admin")
            else:
                print(f"[DRAFTS] User is team owner")
            
            # Get all pages owned by team owner
            owner_pages = ConnectedPage.query.filter_by(user_id=team.owner_id).all()
            owner_page_ids = [p.id for p in owner_pages]
            print(f"[DRAFTS] Team owner has {len(owner_pages)} pages: {owner_page_ids}")
            
            if not owner_page_ids:
                print(f"[DRAFTS] Team owner has no pages")
                return {'success': True, 'drafts': []}
            
            # Get drafts on those pages
            # Get both pending AND approved/rejected posts
            drafts = db.session.query(Post).options(
                joinedload(Post.media)
            ).join(
                PostPageAssociation, Post.id == PostPageAssociation.post_id
            ).filter(
                PostPageAssociation.page_id.in_(owner_page_ids),
                Post.approval_status.in_(['pending', 'approved', 'rejected'])
            ).order_by(Post.approval_requested_at.desc()).distinct().all()
            
            print(f"[DRAFTS] Found {len(drafts)} drafts (pending/approved/rejected) on team owner's pages")
        else:
            # WITHOUT team_id: Return drafts submitted by user OR if user is team owner/admin, return team member drafts
            print(f"[DRAFTS] No team_id provided, checking if user is team owner/admin")
            
            # Get drafts submitted by current user
            user_submitted_drafts = Post.query.options(
                joinedload(Post.media)
            ).filter_by(
                submitted_by_user_id=user_id
            ).filter(
                Post.approval_status.in_(['pending', 'approved', 'rejected'])
            ).order_by(Post.approval_requested_at.desc()).all()
            
            # Get all teams where user is owner or admin
            owned_teams = Team.query.filter_by(owner_id=user_id).all()
            admin_teams = TeamMember.query.filter_by(user_id=user_id, role='admin').all()
            
            print(f"[DRAFTS] User owns {len(owned_teams)} teams, is admin in {len(admin_teams)} teams")
            
            team_page_ids = []
            if owned_teams or admin_teams:
                # Collect all pages from owned/admin teams
                for team in owned_teams:
                    owner_pages = ConnectedPage.query.filter_by(user_id=team.owner_id).all()
                    team_page_ids.extend([p.id for p in owner_pages])
                
                # Also check team member's pages (in case different structure)
                for team_member_record in admin_teams:
                    team = Team.query.get(team_member_record.team_id)
                    if team:
                        owner_pages = ConnectedPage.query.filter_by(user_id=team.owner_id).all()
                        team_page_ids.extend([p.id for p in owner_pages])
                
                team_page_ids = list(set(team_page_ids))  # Remove duplicates
                print(f"[DRAFTS] Found {len(team_page_ids)} unique pages from teams")
                
                if team_page_ids:
                    # Get drafts on team owner's pages (all statuses)
                    team_member_drafts = db.session.query(Post).options(
                        joinedload(Post.media)
                    ).join(
                        PostPageAssociation, Post.id == PostPageAssociation.post_id
                    ).filter(
                        PostPageAssociation.page_id.in_(team_page_ids),
                        Post.approval_status.in_(['pending', 'approved', 'rejected'])
                    ).order_by(Post.approval_requested_at.desc()).distinct().all()
                    
                    print(f"[DRAFTS] Found {len(team_member_drafts)} drafts (all statuses) on team pages")
                    
                    # Combine: user's drafts + team member drafts (remove duplicates)
                    user_draft_ids = {d.id for d in user_submitted_drafts}
                    drafts = user_submitted_drafts + [d for d in team_member_drafts if d.id not in user_draft_ids]
                    print(f"[DRAFTS] Total: {len(drafts)} unique pending drafts")
                else:
                    drafts = user_submitted_drafts
                    print(f"[DRAFTS] User has no team pages, returning only user-submitted drafts: {len(drafts)}")
            else:
                drafts = user_submitted_drafts
                print(f"[DRAFTS] User is not a team owner/admin, returning only user-submitted drafts: {len(drafts)}")

        
        drafts_data = []
        for draft in drafts:
            submitted_by = User.query.get(draft.submitted_by_user_id) if draft.submitted_by_user_id else None
            approved_by = User.query.get(draft.approved_by_user_id) if draft.approved_by_user_id else None
            
            pages_info = []
            for assoc in draft.page_associations:
                pages_info.append({
                    'id': assoc.connected_page.id,
                    'name': assoc.connected_page.page_name,
                    'platform': assoc.connected_page.platform
                })

            media_items = []
            for media in draft.media:
                media_items.append({
                    'id': media.id,
                    'url': media.media_url,
                    'absolute_url': build_absolute_url(media.media_url),
                    'type': media.media_type,
                    'size': media.file_size,
                    'duration': media.duration,
                })
            
            drafts_data.append({
                'id': draft.id,
                'content': draft.caption or draft.content,
                'pages': pages_info,
                'submitted_by': submitted_by.username if submitted_by else 'Unknown',
                'submitted_at': draft.approval_requested_at.isoformat() if draft.approval_requested_at else None,
                'status': draft.approval_status,
                'approval_notes': draft.approval_notes,
                'approved_by': approved_by.username if approved_by else None,
                'approved_at': draft.approval_responded_at.isoformat() if draft.approval_responded_at else None,
                'media': media_items,
            })
        
        print(f"[DRAFTS] Returning {len(drafts_data)} drafts")
        return {'success': True, 'drafts': drafts_data}
    
    except Exception as e:
        print(f"[DRAFTS] Error fetching drafts: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/drafts/<int:draft_id>/approve', methods=['POST'])
@login_required
def approve_draft(draft_id):
    """Approve a draft post - convert to scheduled post"""
    try:
        user_id = session.get('user_id')
        
        draft = Post.query.get(draft_id)
        if not draft:
            return {'success': False, 'error': 'Draft not found'}, 404
        
        # Check if user is owner/admin of the pages this post is for
        page_assoc = PostPageAssociation.query.filter_by(post_id=draft_id).first()
        if not page_assoc:
            return {'success': False, 'error': 'No pages associated with draft'}, 400
        
        page = page_assoc.connected_page
        
        # Check authorization: must own the page or be admin of team
        is_page_owner = (page.user_id == user_id)
        is_team_admin = False
        
        if not is_page_owner:
            # Check if user is admin in any team that has access to this page
            # First find teams that have access to this page
            teams_with_access = db.session.query(ChannelAccess.team_id).filter(
                ChannelAccess.channel_id == page.id
            ).distinct().all()
            team_ids = [t[0] for t in teams_with_access]
            
            if team_ids:
                # Check if user is admin in any of these teams
                admin_check = TeamMember.query.filter(
                    TeamMember.team_id.in_(team_ids),
                    TeamMember.user_id == user_id,
                    TeamMember.role == 'admin'
                ).first()
                is_team_admin = admin_check is not None
        
        if not is_page_owner and not is_team_admin:
            return {'success': False, 'error': 'Unauthorized - only page owner or team admin can approve'}, 403
        
        # Get approval notes from request
        data = request.get_json() if request.is_json else request.form
        approval_notes = data.get('approval_notes', '').strip()
        
        # Update draft
        draft.approval_status = 'approved'
        draft.approved_by_user_id = user_id
        draft.approval_responded_at = datetime.utcnow()
        draft.approval_notes = approval_notes
        
        # Change status to scheduled if scheduled_time exists, otherwise scheduled
        if draft.scheduled_time:
            draft.status = 'scheduled'
        else:
            draft.status = 'scheduled'  # Default to scheduled once approved
        
        db.session.commit()
        
        print(f"[DRAFTS] Draft {draft_id} approved by user {user_id}")
        
        return {
            'success': True,
            'message': 'Draft approved successfully',
            'post_id': draft.id,
            'status': draft.status
        }
    
    except Exception as e:
        db.session.rollback()
        print(f"[DRAFTS] Error approving draft {draft_id}: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


@app.route('/api/drafts/<int:draft_id>/reject', methods=['POST'])
@login_required
def reject_draft(draft_id):
    """Reject a draft post - return to draft status for editing"""
    try:
        user_id = session.get('user_id')
        
        draft = Post.query.get(draft_id)
        if not draft:
            return {'success': False, 'error': 'Draft not found'}, 404
        
        # Check if user is owner/admin of the pages this post is for
        page_assoc = PostPageAssociation.query.filter_by(post_id=draft_id).first()
        if not page_assoc:
            return {'success': False, 'error': 'No pages associated with draft'}, 400
        
        page = page_assoc.connected_page
        
        # Check authorization: must own the page or be admin of team
        is_page_owner = (page.user_id == user_id)
        is_team_admin = False
        
        if not is_page_owner:
            # Check if user is admin in any team that has access to this page
            # First find teams that have access to this page
            teams_with_access = db.session.query(ChannelAccess.team_id).filter(
                ChannelAccess.channel_id == page.id
            ).distinct().all()
            team_ids = [t[0] for t in teams_with_access]
            
            if team_ids:
                # Check if user is admin in any of these teams
                admin_check = TeamMember.query.filter(
                    TeamMember.team_id.in_(team_ids),
                    TeamMember.user_id == user_id,
                    TeamMember.role == 'admin'
                ).first()
                is_team_admin = admin_check is not None
        
        if not is_page_owner and not is_team_admin:
            return {'success': False, 'error': 'Unauthorized - only page owner or team admin can reject'}, 403
        
        # Get rejection notes from request
        data = request.get_json() if request.is_json else request.form
        rejection_notes = data.get('rejection_notes', '').strip()
        
        # Update draft
        draft.approval_status = 'rejected'
        draft.approved_by_user_id = user_id
        draft.approval_responded_at = datetime.utcnow()
        draft.approval_notes = rejection_notes
        draft.status = 'draft'  # Return to draft for editing
        
        db.session.commit()
        
        print(f"[DRAFTS] Draft {draft_id} rejected by user {user_id}")
        
        return {
            'success': True,
            'message': 'Draft rejected - returned to draft for editing',
            'post_id': draft.id
        }
    
    except Exception as e:
        db.session.rollback()
        print(f"[DRAFTS] Error rejecting draft {draft_id}: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}, 500


# ======================== FAVICON ROUTE ========================

@app.route('/favicon.ico')
def favicon():
    """Serve favicon or return 204 No Content to suppress errors"""
    return '', 204


# ======================== ERROR HANDLERS ========================

@app.errorhandler(404)
def page_not_found(_):
    """Handle 404 errors"""
    return render_template('index.html'), 404


@app.errorhandler(500)
def internal_error(_):
    """Handle 500 errors"""
    db.session.rollback()
    flash('An internal server error occurred', 'danger')
    return redirect(url_for('index')), 500


def _ensure_self_cron_started():
    """Start the background scheduler once the module has initialized."""
    global self_cron_runner, _self_cron_started
    if not SELF_CRON_ENABLED:
        logger.info("SelfCron disabled via ENABLE_SELF_CRON env var")
        return
    if _self_cron_started:
        return

    try:
        from self_cron import create_self_cron

        self_cron_runner = create_self_cron(app, SELF_CRON_INTERVAL)
        self_cron_runner.start()
        atexit.register(self_cron_runner.stop)
        logger.info("SelfCron scheduler started (interval=%ss)", SELF_CRON_INTERVAL)
        _self_cron_started = True
    except Exception as cron_exc:
        logger.error(f"Failed to start SelfCron scheduler: {cron_exc}")



# ======================== APPLICATION ENTRY POINT ========================

if __name__ == '__main__':
    # Initialize database
    init_db()
    
    # Setup background scheduler for analytics
    try:
        from tasks import setup_scheduler
        setup_scheduler()
    except ImportError:
        print("Warning: APScheduler not installed. Install with: pip install apscheduler")
    except Exception as e:
        print(f"Warning: Could not setup scheduler: {e}")
    
    # Use environment variable for port (for cloud deployment)
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') != 'production'
    app.run(host='0.0.0.0', port=port, debug=debug)

