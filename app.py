from flask import Flask, render_template, request, redirect, url_for, flash, session
import os
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from datetime import datetime, timedelta
import secrets
import requests
from auth import PasswordHelper, login_required
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

load_dotenv()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', os.path.join(BASE_DIR, 'uploads'))

# Create the Flask app with explicit template folder
app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dfe8216ff1440e9e8137744e5087c537')
app.config['SESSION_COOKIE_SECURE'] = True  
app.config['SERVER_NAME'] = os.getenv('SERVER_NAME')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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
    status = db.Column(db.String(50), default='draft')  # 'draft', 'scheduled', 'sent', 'failed'
    scheduled_time = db.Column(db.DateTime, nullable=True)
    sent_time = db.Column(db.DateTime, nullable=True)
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
    
    # Time data
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    post_page_association = db.relationship('PostPageAssociation', backref='analytics')
    
    def __repr__(self):
        return f'<PostAnalytics post_page={self.post_page_association_id}>'


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
        response = requests.get(FACEBOOK_PAGES_API_URL, headers=headers, timeout=10)
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


def get_facebook_page_posts(page_id, access_token):
    """Fetch ALL historical posts from a Facebook page (paginate through all results)"""
    print(f"[POSTS] Fetching ALL posts for page {page_id}")
    all_posts = []
    try:
        # Try different field combinations, prioritizing useful data
        # Start with minimal, but ensure we get at least created_time for useful posts
        field_sets = [
            'id,created_time',  # Minimum useful data
            'id,message,created_time',  # With message
            'id,message,caption,created_time,type,permalink_url'  # Full set
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
                        print(f"[POSTS] ✓ Success with endpoint: {endpoint_url}")
                        print(f"[POSTS] ✓ Using fields: {fields}")
                        url = endpoint_url
                        working_fields = fields
                        break
                    else:
                        print(f"[POSTS] ✗ Failed with {test_response.status_code}")
                except Exception as e:
                    print(f"[POSTS] ✗ Exception: {e}")
                    continue
            
            if url:
                break
        
        if not url or not working_fields:
            print(f"[POSTS] Could not find working endpoint/fields combination for page {page_id}")
            return all_posts
        
        # Now fetch all posts with the working combination
        print(f"[POSTS] Fetching all posts with working fields: {working_fields}")
        params = {
            'fields': working_fields,
            'access_token': access_token,
            'limit': 100
        }
        
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
                        error_msg = error_data['error'].get('message', error_msg)
                except:
                    pass
                print(f"[POSTS] Error ({response.status_code}): {error_msg}")
                break
            
            result = response.json()
            posts_data = result.get('data', [])
            all_posts.extend(posts_data)
            print(f"[POSTS] Retrieved {len(posts_data)} posts in this batch (total: {len(all_posts)})")
            
            paging = result.get('paging', {})
            url = paging.get('next')
        
        print(f"[POSTS] Total posts retrieved: {len(all_posts)}")
        return all_posts
        
    except Exception as e:
        print(f"[POSTS] Error: {e}")
        import traceback
        traceback.print_exc()
        return []


def store_facebook_posts_to_db(user_id, connected_page, posts_data):
    """Store Facebook posts to database, avoiding duplicates"""
    print(f"[POSTS] Storing {len(posts_data)} posts to database for page {connected_page.page_name}")
    posts_added = 0
    
    for post_data in posts_data:
        post_id = post_data.get('id', '')
        print(f"[POSTS] Processing post: {post_id}")
        print(f"[POSTS] Post data: {post_data}")
        
        # Check if post already exists by checking both Facebook ID and user
        existing_post = Post.query.filter_by(
            user_id=user_id,
            title=post_id
        ).first()
        
        if existing_post:
            print(f"[POSTS] → Post already exists in DB, skipping")
            continue
        
        try:
            # Parse Facebook's created_time
            created_time = post_data.get('created_time', '')
            if created_time:
                sent_time = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
            else:
                sent_time = datetime.utcnow()
            
            # Create post from Facebook data
            post = Post(
                user_id=user_id,
                title=post_id,
                content=post_data.get('message') or post_data.get('caption') or f'Posted on {connected_page.page_name}',
                caption=post_data.get('message') or post_data.get('caption') or '',
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
            posts_added += 1
            print(f"[POSTS] → ✓ Added to DB (sent_time: {sent_time})")
        except Exception as e:
            print(f"[POSTS] → ✗ Error storing post: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if posts_added > 0:
        db.session.commit()
        print(f"[POSTS] Successfully added {posts_added} new posts to database")
    else:
        print(f"[POSTS] No new posts added (all duplicates or errors)")
    
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
                         user_posts=all_accessible_posts)


@app.route('/drafts', methods=['GET'])
@login_required
def drafts():
    """Draft approval queue for admins/owners"""
    user_id = session.get('user_id')
    team_id = request.args.get('team_id', type=int)
    
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
    
    return render_template('dashboard/drafts.html', team_id=team_id)


@app.route('/analyze', methods=['GET'])
@login_required
def analyze():
    """Analytics dashboard for user - Shows all posted/sent posts with optional analytics"""
    user_id = session.get('user_id')
    user = User.query.get(user_id)
    
    # If user doesn't exist in database (e.g., after db reset), redirect to login
    if not user:
        flash('Session expired. Please log in again.', 'warning')
        return redirect(url_for('login'))
    
    # Trigger analytics refresh in background to fetch latest metrics from Meta
    try:
        from tasks import refresh_all_post_analytics
        # Run analytics refresh asynchronously with application context
        import threading
        def run_refresh(flask_app):
            with flask_app.app_context():
                refresh_all_post_analytics()
        # Pass current app instance as argument, not via closure
        refresh_thread = threading.Thread(target=run_refresh, args=(app,))
        refresh_thread.daemon = True
        refresh_thread.start()
    except Exception as e:
        print(f"[ANALYZE] Warning: Could not trigger analytics refresh: {e}")
        # Don't fail the page load if refresh fails
    
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
    
    # Get all posts that were published to any of these pages
    accessible_posts = db.session.query(Post).join(
        PostPageAssociation, Post.id == PostPageAssociation.post_id
    ).filter(
        PostPageAssociation.page_id.in_(accessible_page_ids),
        Post.status == 'sent'
    ).order_by(Post.sent_time.desc()).distinct().all()

    
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
                # Get analytics for this post-page association if it exists
                analytics = PostAnalytics.query.filter_by(post_page_association_id=assoc.id).first()
                
                analytics_data = {}
                if analytics:
                    analytics_data = {
                        'impressions': analytics.impressions or 0,
                        'reach': analytics.reach or 0,
                        'clicks': analytics.clicks or 0,
                        'likes': analytics.likes or 0,
                        'comments': analytics.comments or 0,
                        'shares': analytics.shares or 0,
                        'saves': analytics.saves or 0
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
                        'saves': 0
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
            post_dict = {
                'id': post.id,
                'caption': post.caption or post.content,
                'content': post.content,
                'scheduled_time': post.scheduled_time.isoformat() + 'Z' if post.scheduled_time else None,
                'sent_time': post.sent_time.isoformat() + 'Z' if post.sent_time else None,
                'status': post.status,
                'media': media_list,
                'pages': page_analytics
            }
            posts_data.append(post_dict)
    
    return render_template('dashboard/analyze.html', 
                         pages_by_platform=pages_by_platform, 
                         user_posts=posts_data)


# ======================== TEAM MANAGEMENT ROUTES ========================

# Register team routes
from team_routes import register_team_routes
register_team_routes(app, db, User, Team, TeamMember, ChannelAccess, TeamInvitation, ConnectedPage,
                     check_owner_access, check_admin_access, check_team_member_access)


# ======================== TEMPLATE CONTEXT PROCESSOR ========================

@app.context_processor
def inject_pending_invitations_count():
    """Make pending invitations count available to all templates"""
    if 'user_id' in session:
        user_id = session.get('user_id')
        user = User.query.get(user_id)
        if user:
            # Count pending invitations for this user's email
            count = TeamInvitation.query.filter_by(
                invited_email=user.email,
                status='pending'
            ).filter(
                TeamInvitation.expires_at > datetime.utcnow()
            ).count()
            return {'pending_invitations_count': count}
    return {'pending_invitations_count': 0}


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
        flash('TikTok integration coming soon', 'info')
        return redirect(url_for('publish'))
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
    
    # Store pages data in session for selection page
    session['oauth_pages'] = pages_data['data']
    session['oauth_access_token'] = access_token
    print(f"[OAUTH] Pages stored in session")
    
    # Clean up temporary session data
    session.pop('oauth_state', None)
    
    print("[OAUTH] ========== FACEBOOK CALLBACK END ==========\n")
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
    
    if request.method == 'POST':
        print("[SELECT-ACCOUNTS] Processing POST request")
        selected_page_ids = request.form.getlist('selected_pages')
        print(f"[SELECT-ACCOUNTS] Selected page IDs: {selected_page_ids}")
        
        if not selected_page_ids:
            print("[SELECT-ACCOUNTS] No pages selected")
            flash('Please select at least one account', 'warning')
            return render_template('dashboard/select_accounts.html', pages=pages, platform=platform)
        
        # Add selected pages to ConnectedPage and fetch posts
        saved_count = 0
        access_token = session.get('oauth_access_token')
        
        for page_id in selected_page_ids:
            # Find page in pages list
            page_data = next((p for p in pages if p['id'] == page_id), None)
            if not page_data:
                print(f"[SELECT-ACCOUNTS] Page {page_id} not found in pages list")
                continue
            
            # Check if page already connected
            existing = ConnectedPage.query.filter_by(
                user_id=user_id,
                platform=platform,
                platform_page_id=page_id
            ).first()
            
            if not existing:
                connected_page = ConnectedPage(
                    user_id=user_id,
                    platform=platform,
                    platform_page_id=page_id,
                    page_name=page_data.get('name', 'Unnamed Page'),
                    page_username=page_data.get('username', ''),
                    page_access_token=page_data.get('access_token', ''),  # Store page access token
                    is_active=True
                )
                db.session.add(connected_page)
                db.session.flush()
                saved_count += 1
                print(f"[SELECT-ACCOUNTS] Added: {page_data.get('name', 'Unnamed')} (ID: {page_id})")
                print(f"[SELECT-ACCOUNTS] Stored page access token for {page_id}")
                
                # Fetch and populate posts from this page
                if access_token and platform.lower() == 'facebook':
                    print(f"[SELECT-ACCOUNTS] Fetching posts for page {page_id}")
                    posts_data = get_facebook_page_posts(page_id, access_token)
                    posts_added = store_facebook_posts_to_db(user_id, connected_page, posts_data)
            else:
                print(f"[SELECT-ACCOUNTS] Already exists: {page_id}")
        
        print(f"[SELECT-ACCOUNTS] About to commit {saved_count} new pages...")
        db.session.commit()
        print(f"[SELECT-ACCOUNTS] Committed successfully")
        
        # Clean up session data
        session.pop('oauth_pages', None)
        session.pop('oauth_access_token', None)
        
        flash(f'Successfully added {saved_count} account(s) to Postly!', 'success')
        print("[SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
        return redirect(url_for('publish'))
    
    print("[SELECT-ACCOUNTS] Rendering GET request")
    print("[SELECT-ACCOUNTS] ========== ROUTE END ==========\n")
    return render_template('dashboard/select_accounts.html', pages=pages, platform=platform)


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
            return {'success': False, 'error': 'TikTok integration coming soon'}, 501
        
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
    
    # Store pages data in session for selection page
    session['admin_oauth_pages'] = pages_data['data']
    session['admin_oauth_access_token'] = access_token
    print(f"[ADMIN-OAUTH-CALLBACK] Pages stored in session")
    
    # Clean up temporary session data
    session.pop('admin_oauth_state', None)
    
    print("[ADMIN-OAUTH-CALLBACK] ========== FACEBOOK ADMIN CALLBACK END ==========\n")
    
    # Redirect to admin account selection page
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
        selected_page_ids = request.form.getlist('selected_pages')
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
            page_data = next((p for p in pages if p['id'] == page_id), None)
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
            
            if not existing:
                connected_page = ConnectedPage(
                    user_id=user_id,  # Admin user who connected it
                    team_id=team_id,  # For team context
                    platform=platform,
                    platform_page_id=page_id,
                    page_name=page_data.get('name', 'Unnamed Page'),
                    page_username=page_data.get('username', ''),
                    page_access_token=page_data.get('access_token', ''),
                    is_active=True,
                    is_team_owned=True,  # Mark as team-owned
                    connected_by_user_id=user_id  # Track who connected
                )
                db.session.add(connected_page)
                db.session.flush()
                saved_count += 1
                print(f"[ADMIN-SELECT-ACCOUNTS] Added: {page_data.get('name', 'Unnamed')} (ID: {page_id})")
                
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
                
            else:
                print(f"[ADMIN-SELECT-ACCOUNTS] Already exists: {page_id}")
        
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
        
        if not pages_to_post:
            print("[POST] ERROR: No valid pages to post to")
            return {'success': False, 'error': 'No valid pages selected'}, 400
        
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
        media_files = []
        for key in request.files:
            if key.startswith('media_'):
                media_files.append(request.files[key])
        
        media_count = 0
        for media_file in media_files:
            if media_file and media_file.filename:
                # Save file to uploads folder
                filename = f"post_{post.id}_{secrets.token_hex(8)}_{media_file.filename}"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                media_file.save(filepath)
                
                # Determine media type
                if media_file.content_type.startswith('image/'):
                    media_type = 'image'
                elif media_file.content_type.startswith('video/'):
                    media_type = 'video'
                else:
                    media_type = 'file'
                
                # Create PostMedia record
                post_media = PostMedia(
                    post_id=post.id,
                    media_url=f'/uploads/{filename}',
                    media_type=media_type,
                    file_size=media_file.content_length
                )
                db.session.add(post_media)
                media_count += 1
                print(f"[POST] Media added: {filename} ({media_type})")
        
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
        
        # If "Post Now" is selected AND not submitting for approval, actually publish to Facebook immediately
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
                    continue
                
                print(f"[POST] Using page access token for page {page_id}")
                # Actually publish to Facebook
                fb_post_id = publish_to_facebook(page.platform_page_id, post, page.page_access_token)
                if fb_post_id:
                    # Update association with platform_post_id
                    assoc = PostPageAssociation.query.filter_by(post_id=post.id, page_id=page_id).first()
                    if assoc:
                        assoc.platform_post_id = fb_post_id
                        assoc.status = 'sent'
                    db.session.commit()
                    print(f"[POST] Published to Facebook, ID: {fb_post_id}")
                else:
                    print(f"[POST] Failed to publish to Facebook")
        elif submit_for_approval:
            print(f"[POST] NOT publishing - post submitted for approval (status={post.status}, approval_status={post.approval_status})")
        
        # Verify what was stored
        post_reload = Post.query.get(post.id)
        print(f"[POST] Post saved successfully. Media: {media_count}, Pages: {page_count}")
        print(f"[POST] DB verification - status: {post_reload.status}, approval_status: {post_reload.approval_status}, sent_time: {post_reload.sent_time}, scheduled_time: {post_reload.scheduled_time}")
        print("[POST] ========== CREATE POST END ==========\n")
        
        message = f'Post submitted for approval' if submit_for_approval else f'Post scheduled for {page_count} page(s)'
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
            for assoc in page_associations:
                page = assoc.connected_page
                pages_info.append({
                    'id': page.id,
                    'name': page.page_name,
                    'platform': page.platform
                })
            
            # Get media
            media_info = []
            for media in post.media:
                media_info.append({
                    'url': media.media_url,
                    'type': media.media_type
                })
            
            display_date = post.scheduled_time or post.sent_time or post.created_at
            print(f"[GET /api/posts] Post {post.id}: status={post.status}, date={display_date}, pages={len(pages_info)}")
            
            posts_data.append({
                'id': post.id,
                'caption': post.caption,
                'icon': post.post_icon,
                'status': post.status,
                'scheduled_time': post.scheduled_time.isoformat() + 'Z' if post.scheduled_time else None,
                'sent_time': post.sent_time.isoformat() + 'Z' if post.sent_time else None,
                'created_at': post.created_at.isoformat() + 'Z',
                'pages': pages_info,
                'media': media_info
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


@app.route('/api/posts/refresh/historical', methods=['POST'])
@login_required
def refresh_historical_posts():
    """Refresh historical posts from all connected Facebook pages"""
    try:
        user_id = session.get('user_id')
        
        # Get all connected pages for this user
        pages = ConnectedPage.query.filter_by(user_id=user_id).all()
        
        if not pages:
            return {'success': False, 'error': 'No connected pages found'}, 400
        
        total_posts_added = 0
        results = []
        
        for page in pages:
            if page.platform.lower() != 'facebook':
                results.append({
                    'page': page.page_name,
                    'status': 'skipped',
                    'reason': 'Not a Facebook page'
                })
                continue
            
            if not page.page_access_token:
                results.append({
                    'page': page.page_name,
                    'status': 'error',
                    'reason': 'No page access token available'
                })
                continue
            
            try:
                # Fetch historical posts from Facebook
                posts_data = get_facebook_page_posts(page.platform_page_id, page.page_access_token)
                
                # Store posts to database
                posts_added = store_facebook_posts_to_db(user_id, page, posts_data)
                
                total_posts_added += posts_added
                results.append({
                    'page': page.page_name,
                    'status': 'success',
                    'posts_added': posts_added
                })
                print(f"[REFRESH] Added {posts_added} posts for page {page.page_name}")
                
            except Exception as e:
                results.append({
                    'page': page.page_name,
                    'status': 'error',
                    'reason': str(e)
                })
                print(f"[REFRESH] Error fetching posts for page {page.page_name}: {e}")
        
        return {
            'success': True,
            'total_posts_added': total_posts_added,
            'pages_processed': len([r for r in results if r['status'] != 'error']),
            'results': results
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
        
        if page.platform.lower() != 'facebook':
            return {'success': False, 'error': 'Only Facebook pages can be diagnosed'}, 400
        
        if not page.page_access_token:
            return {'success': False, 'error': 'No page access token available'}, 400
        
        # Test 1: Verify token with a simple API call
        diagnosis = {
            'page_name': page.page_name,
            'platform_page_id': page.platform_page_id,
            'tests': {}
        }
        
        try:
            # Test token validity with simple page info call
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
                try:
                    error_data = response.json()
                    diagnosis['tests']['token_error'] = error_data.get('error', {}).get('message', response.text)
                except:
                    diagnosis['tests']['token_error'] = response.text
        except Exception as e:
            diagnosis['tests']['token_validity'] = f'error - {str(e)}'
        
        # Test 2: Try simple posts fetch with minimal fields
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
                try:
                    error_data = response.json()
                    diagnosis['tests']['posts_error'] = error_data.get('error', {}).get('message', response.text)
                except:
                    diagnosis['tests']['posts_error'] = response.text
        except Exception as e:
            diagnosis['tests']['posts_fetch'] = f'error - {str(e)}'
        
        # Test 3: Try full posts fetch with all fields
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
                try:
                    error_data = response.json()
                    diagnosis['tests']['posts_full_error'] = error_data.get('error', {}).get('message', response.text)
                except:
                    diagnosis['tests']['posts_full_error'] = response.text
        except Exception as e:
            diagnosis['tests']['posts_full_fetch'] = f'error - {str(e)}'
        
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
        
        # Get the post
        post = Post.query.filter_by(id=post_id, user_id=user_id).first()
        if not post:
            return {'success': False, 'error': 'Post not found'}, 404
        
        # Get data from JSON or form
        data = {}
        if request.is_json:
            data = request.get_json() or {}
        else:
            data = request.form.to_dict()
        
        # Extract fields
        caption = data.get('caption', '').strip()
        pages_data = data.get('pages', [])
        publish_type = data.get('publish_type', 'now')
        status = data.get('status', 'draft')
        scheduled_time_str = data.get('scheduled_time', '')
        
        print(f"[PUT] Updating post {post_id}: caption length={len(caption)}, pages count={len(pages_data) if pages_data else 0}")
        
        # Validate input
        if not caption:
            return {'success': False, 'error': 'Caption is required'}, 400
        
        # Validate pages
        if not pages_data or len(pages_data) == 0:
            return {'success': False, 'error': 'At least one page must be selected'}, 400
        
        # Update post content
        post.caption = caption
        post.content = caption
        post.status = status or 'draft'
        
        # Update scheduled time if needed
        if status == 'scheduled' and scheduled_time_str:
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
        else:
            post.scheduled_time = None
        
        # Handle media uploads (new media files)
        media_files = []
        for key in request.files:
            if key.startswith('media_'):
                media_files.append(request.files[key])
        
        media_count = 0
        for media_file in media_files:
            if media_file and media_file.filename:
                # Save file to uploads folder
                filename = f"post_{post.id}_{secrets.token_hex(8)}_{media_file.filename}"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                media_file.save(filepath)
                
                # Determine media type
                if media_file.content_type.startswith('image/'):
                    media_type = 'image'
                elif media_file.content_type.startswith('video/'):
                    media_type = 'video'
                else:
                    media_type = 'file'
                
                # Create PostMedia record
                post_media = PostMedia(
                    post_id=post.id,
                    media_url=f'/uploads/{filename}',
                    media_type=media_type,
                    file_size=media_file.content_length
                )
                db.session.add(post_media)
                media_count += 1
                print(f"[PUT] New media added: {filename} ({media_type})")
        
        # Update page associations - delete old ones and create new ones
        PostPageAssociation.query.filter_by(post_id=post.id).delete()
        
        page_count = 0
        for page_info in pages_data:
            if isinstance(page_info, dict):
                page_id = page_info.get('id')
            else:
                page_id = page_info
            
            # Verify user has access to this page (either owns it or has team access)
            page = ConnectedPage.query.get(page_id)
            if not page:
                print(f"[PUT] Page {page_id} not found")
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
        
        # Check authorization: must be post creator OR page owner
        is_creator = (post.user_id == user_id)
        is_page_owner = False
        
        if not is_creator:
            # Check if user is owner of any page this post is associated with
            assoc = PostPageAssociation.query.filter_by(post_id=post_id).first()
            if assoc:
                page = assoc.connected_page
                is_page_owner = (page.user_id == user_id)
        
        # Also check if user is team admin with access to this page
        is_team_admin = False
        if not is_creator and not is_page_owner:
            assoc = PostPageAssociation.query.filter_by(post_id=post_id).first()
            if assoc:
                page = assoc.connected_page
                if page.team_id:
                    try:
                        is_team_admin = check_admin_access(page.team_id, user_id)
                    except:
                        is_team_admin = False
        
        if not is_creator and not is_page_owner and not is_team_admin:
            return {'success': False, 'error': 'Unauthorized - only post creator, page owner, or team admin can delete'}, 403
        
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
                # Publish to Facebook/Instagram
                if page.platform.lower() == 'facebook':
                    print(f"[PUBLISH] Publishing post {post_id} to page {page.platform_page_id} using page token")
                    fb_post_id = publish_to_facebook(page.platform_page_id, post, page.page_access_token)
                    if fb_post_id:
                        assoc.platform_post_id = fb_post_id
                        assoc.status = 'sent'
                        published_count += 1
                        print(f"[PUBLISH] Post published to Facebook: {fb_post_id}")
                    else:
                        errors.append(f"Failed to publish to {page.page_name}")
            except Exception as e:
                errors.append(f"Error publishing to {page.page_name}: {str(e)}")
                print(f"[PUBLISH] Error: {e}")
        
        # Update post status only if published to at least one page
        if published_count > 0:
            post.status = 'sent'
            post.sent_time = datetime.utcnow()
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
    """Publish a post to Facebook/Instagram page"""
    print(f"[PUBLISH] Publishing to page {page_id}")
    try:
        url = f"https://graph.facebook.com/v18.0/{page_id}/feed"
        
        # Prepare post data
        data = {
            'message': post.caption or post.content,
            'access_token': access_token
        }
        
        # Get base URL for media - try multiple sources
        base_url = os.getenv('APP_URL')
        if not base_url:
            # Extract from Facebook redirect URI
            redirect_uri = os.getenv('FACEBOOK_OAUTH_REDIRECT_URI', '')
            if redirect_uri:
                # Remove the /oauth/facebook/callback part
                base_url = redirect_uri.split('/oauth/')[0]
        if not base_url:
            base_url = 'http://localhost:5000'
        
        # Add media if available
        media_files = PostMedia.query.filter_by(post_id=post.id).all()
        if media_files:
            # For now, we'll send the first media item
            # In production, you'd want to handle multiple media
            media = media_files[0]
            
            # Convert local path to full URL if needed
            if media.media_type == 'image':
                media_url = media.media_url
                if not media_url.startswith('http'):
                    media_url = f"{base_url.rstrip('/')}{media_url}"
                data['url'] = media_url
                print(f"[PUBLISH] Adding image: {media_url}")
            elif media.media_type == 'video':
                media_url = media.media_url
                if not media_url.startswith('http'):
                    media_url = f"{base_url.rstrip('/')}{media_url}"
                data['video_url'] = media_url
                print(f"[PUBLISH] Adding video: {media_url}")
        
        print(f"[PUBLISH] Sending data: {data}")
        response = requests.post(url, data=data, timeout=15)
        response.raise_for_status()
        result = response.json()
        
        if 'id' in result:
            print(f"[PUBLISH] Successfully published: {result['id']}")
            return result['id']
        else:
            print(f"[PUBLISH] No ID in response: {result}")
            return None
    
    except Exception as e:
        print(f"[PUBLISH] Error publishing to Facebook: {e}")
        return None


# ======================== DRAFT & APPROVAL WORKFLOW ROUTES ========================

@app.route('/api/drafts', methods=['GET'])
@login_required
def get_drafts():
    """Get pending drafts for current user (admin/owner can see team member drafts)"""
    try:
        user_id = session.get('user_id')
        team_id = request.args.get('team_id', type=int)
        
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
            drafts = db.session.query(Post).join(
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
            user_submitted_drafts = Post.query.filter_by(
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
                    team_member_drafts = db.session.query(Post).join(
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
            
            drafts_data.append({
                'id': draft.id,
                'content': draft.caption or draft.content,
                'pages': pages_info,
                'submitted_by': submitted_by.username if submitted_by else 'Unknown',
                'submitted_at': draft.approval_requested_at.isoformat() if draft.approval_requested_at else None,
                'status': draft.approval_status,
                'approval_notes': draft.approval_notes,
                'approved_by': approved_by.username if approved_by else None,
                'approved_at': draft.approval_responded_at.isoformat() if draft.approval_responded_at else None
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
    
    app.run(debug=True)
