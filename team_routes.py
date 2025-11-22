"""
Team Management Routes for Postly
Handles team creation, member management, invitations, and permissions
"""

from flask import render_template, request, jsonify, redirect, url_for, flash, session
from functools import wraps
from datetime import datetime, timedelta
import secrets

# These will be imported from app.py in integration
# from app import app, db, User, Team, TeamMember, ChannelAccess, TeamInvitation, ConnectedPage
# from app import check_admin_access, check_owner_access, check_team_member_access, can_publish_to_channel


def register_team_routes(app, db, User, Team, TeamMember, ChannelAccess, TeamInvitation, ConnectedPage,
                         check_admin_access, check_owner_access, check_team_member_access):
    """Register all team-related routes with the Flask app"""
    
    # ======================== DECORATORS ========================
    
    def login_required(f):
        """Require user to be logged in (custom decorator using session)"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Please log in to access this page', 'warning')
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function
    
    def require_team_owner(f):
        """Require user to be team owner of a specific team"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not user_id:
                flash('Please log in', 'error')
                return redirect(url_for('login'))
            
            # Get team_id from kwargs or session (for current team context)
            team_id = kwargs.get('team_id')
            if not team_id:
                # Fall back to session active team
                team_id = session.get('active_team_id')
            
            if not team_id:
                # No team specified, get first owned team for backwards compatibility
                team = Team.query.filter_by(owner_id=user_id).first()
                if not team:
                    flash('You do not own a team', 'error')
                    return redirect(url_for('team_dashboard'))
                team_id = team.id
                kwargs['team_id'] = team_id
            
            # Check owner access for this specific team
            if not check_owner_access(team_id, user_id):
                flash('You must be the team owner to perform this action', 'error')
                return redirect(url_for('team_dashboard'))
            
            return f(*args, **kwargs)
        return decorated_function
    
    def require_team_admin(f):
        """Require user to be team admin or owner of a specific team"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not user_id:
                flash('Please log in', 'error')
                return redirect(url_for('login'))
            
            # Get team_id from kwargs, session, or request args
            team_id = kwargs.get('team_id')
            if not team_id:
                team_id = session.get('active_team_id')
            if not team_id:
                # Check request args (for GET requests like /team/invite?team_id=1)
                team_id = request.args.get('team_id', type=int)
            
            if not team_id:
                # Fall back to first team user is in (for backwards compatibility)
                team = Team.query.filter_by(owner_id=user_id).first()
                if not team:
                    member = TeamMember.query.filter_by(user_id=user_id).first()
                    team = member.team if member else None
                if not team:
                    flash('You are not associated with any team', 'error')
                    return redirect(url_for('team_dashboard'))
                team_id = team.id
                kwargs['team_id'] = team_id
            
            # Check admin access for this specific team
            if not check_admin_access(team_id, user_id):
                flash('You must be an admin of the specified team to perform this action', 'error')
                return redirect(url_for('team_dashboard'))
            
            return f(*args, **kwargs)
        return decorated_function
    
    # ======================== TEAM DASHBOARD & MAIN ROUTES ========================
    
    @app.route('/team', methods=['GET'])
    @login_required
    def team_dashboard():
        """Team collaboration dashboard - shows all teams user belongs to"""
        user_id = session.get('user_id')
        user = User.query.get(user_id)
        
        if not user:
            flash('Session expired. Please log in again.', 'warning')
            return redirect(url_for('login'))
        
        # Get teams where user is owner
        owned_teams = Team.query.filter_by(owner_id=user_id).all()
        
        # Get teams where user is a member
        member_teams = []
        for membership in TeamMember.query.filter_by(user_id=user_id).all():
            member_teams.append(membership.team)
        
        # If user has no teams, create a default personal team
        if not owned_teams and not member_teams:
            default_team = Team(
                name=f"{user.username}'s Team",
                owner_id=user_id
            )
            db.session.add(default_team)
            db.session.commit()
            owned_teams = [default_team]
        
        # Determine active team
        # Priority: 1) Request param, 2) Session stored team, 3) First team
        active_team_id = request.args.get('team_id', type=int)
        if not active_team_id:
            active_team_id = session.get('active_team_id')
        
        # Get primary team for context (default if no specific team requested)
        primary_team = None
        
        if active_team_id:
            # Verify user has access to requested team
            if any(t.id == active_team_id for t in owned_teams + member_teams):
                primary_team = Team.query.get(active_team_id)
        
        if not primary_team:
            # Default to first owned team, then first member team
            primary_team = owned_teams[0] if owned_teams else (member_teams[0] if member_teams else None)
        
        # Store active team in session
        if primary_team:
            session['active_team_id'] = primary_team.id
        
        if primary_team:
            # Get team members with their details
            members_data = []
            
            # Add owner
            members_data.append({
                'id': None,
                'user': primary_team.owner,
                'email': primary_team.owner.email,
                'role': 'owner',
                'is_owner': True,
                'created_at': primary_team.created_at
            })
            
            # Add other members
            for member in primary_team.members:
                channel_access_count = len(ChannelAccess.query.filter_by(team_member_id=member.id).all())
                members_data.append({
                    'id': member.id,
                    'user': member.user,
                    'email': member.user.email,
                    'role': member.role,
                    'is_owner': False,
                    'channels_count': channel_access_count,
                    'created_at': member.created_at,
                    'member_obj': member
                })
            
            # Get pending invitations for THIS TEAM ONLY
            pending_invites = TeamInvitation.query.filter_by(
                team_id=primary_team.id,
                status='pending'
            ).all()
            
            # Get team's connected channels (pages)
            team_channels = ConnectedPage.query.filter_by(user_id=primary_team.owner_id).all()
            
            is_owner = primary_team.owner_id == user_id
            is_admin = is_owner or check_admin_access(primary_team.id, user_id)
            
            return render_template('team/dashboard.html',
                                 team=primary_team,
                                 owned_teams=owned_teams,
                                 member_teams=member_teams,
                                 all_teams=owned_teams + member_teams,
                                 active_team_id=primary_team.id,
                                 members=members_data,
                                 pending_invites=pending_invites,
                                 channels=team_channels,
                                 is_owner=is_owner,
                                 is_admin=is_admin,
                                 now=datetime.utcnow())
        
        # No teams found
        return render_template('team/dashboard.html',
                             team=None,
                             owned_teams=owned_teams,
                             member_teams=member_teams,
                             all_teams=owned_teams + member_teams,
                             active_team_id=None,
                             members=[],
                             pending_invites=[],
                             channels=[],
                             is_owner=False,
                             is_admin=False,
                             now=datetime.utcnow())
    
    
    # ======================== TEAM INVITATION ROUTES ========================
    
    @app.route('/team/invite', methods=['GET', 'POST'])
    @login_required
    @require_team_admin
    def team_invite(team_id=None):
        """Invite a new member to the team"""
        user_id = session.get('user_id')
        
        # Get team from parameter, session, or default
        if not team_id:
            team_id = request.args.get('team_id', type=int)
        if not team_id:
            team_id = session.get('active_team_id')
        if not team_id:
            # Get user's owned team or first member team
            team = Team.query.filter_by(owner_id=user_id).first()
            if not team:
                member = TeamMember.query.filter_by(user_id=user_id).first()
                team = member.team if member else None
        else:
            team = Team.query.get(team_id)
        
        if not team:
            flash('Team not found', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Verify user is admin of this team
        if not check_admin_access(team.id, user_id):
            flash('You do not have permission to invite members to this team', 'error')
            return redirect(url_for('team_dashboard'))
        
        if request.method == 'POST':
            invited_name = request.form.get('invited_name', '').strip()
            invited_email = request.form.get('invited_email', '').strip()
            role = request.form.get('role', 'member')
            
            # Validate input
            if not invited_name or not invited_email or role not in ['admin', 'member']:
                flash('Invalid invitation details. Please check your input.', 'error')
                return redirect(url_for('team_invite', team_id=team.id))
            
            # Check if user already invited
            existing_invite = TeamInvitation.query.filter_by(
                team_id=team.id,
                invited_email=invited_email,
                status='pending'
            ).first()
            
            if existing_invite:
                flash(f'{invited_email} has already been invited to this team', 'warning')
                return redirect(url_for('team_invite', team_id=team.id))
            
            # Check if user already a member
            existing_user = User.query.filter_by(email=invited_email).first()
            if existing_user and team.has_member(existing_user.id):
                flash(f'{invited_email} is already a member of this team', 'warning')
                return redirect(url_for('team_invite', team_id=team.id))
            
            # Prevent inviting the owner
            if team.owner_id == existing_user.id if existing_user else False:
                flash('Cannot invite the team owner', 'warning')
                return redirect(url_for('team_invite', team_id=team.id))
            
            # Create invitation
            invitation_token = secrets.token_urlsafe(32)
            invitation = TeamInvitation(
                team_id=team.id,
                invited_email=invited_email,
                invited_name=invited_name,
                role=role,
                invitation_token=invitation_token,
                created_by_user_id=user_id
            )
            db.session.add(invitation)
            db.session.commit()
            
            # TODO: Send invitation email
            flash(f'Invitation sent to {invited_email}!', 'success')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        return render_template('team/invite.html', team=team)
    
    
    @app.route('/team/accept-invite/<token>', methods=['GET'])
    @login_required
    def team_accept_invite(token):
        """Accept team invitation by token"""
        user_id = session.get('user_id')
        user = User.query.get(user_id)
        
        # Find invitation
        invitation = TeamInvitation.query.filter_by(invitation_token=token).first()
        
        if not invitation:
            flash('Invalid or expired invitation link', 'error')
            return redirect(url_for('team_dashboard'))
        
        if not invitation.is_valid():
            flash('This invitation has expired. Please ask the team owner to send a new one.', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Check if email matches
        if user.email != invitation.invited_email:
            flash('This invitation is for a different email address. Please log in with that email.', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Check if already a member
        if invitation.team.has_member(user_id):
            flash('You are already a member of this team', 'info')
            return redirect(url_for('team_dashboard', team_id=invitation.team_id))
        
        # Add user to team
        new_member = TeamMember(
            team_id=invitation.team_id,
            user_id=user_id,
            role=invitation.role
        )
        db.session.add(new_member)
        
        # Mark invitation as accepted
        invitation.status = 'accepted'
        invitation.accepted_at = datetime.utcnow()
        
        db.session.commit()
        
        # Store this team as active and redirect to it
        session['active_team_id'] = invitation.team_id
        
        flash(f'Welcome to {invitation.team.name}! You have been added as a {invitation.role}.', 'success')
        return redirect(url_for('team_dashboard', team_id=invitation.team_id))
    
    
    @app.route('/team/pending-invitations', methods=['GET'])
    @login_required
    def team_pending_invitations():
        """Show all pending invitations for logged-in user across all teams"""
        user_id = session.get('user_id')
        user = User.query.get(user_id)
        
        if not user:
            flash('Session expired. Please log in again.', 'warning')
            return redirect(url_for('login'))
        
        # Get pending invitations for this user's email across ALL teams
        all_invitations = TeamInvitation.query.filter_by(
            invited_email=user.email,
            status='pending'
        ).all()
        
        # Filter out expired invitations and include team info
        valid_invitations = []
        for inv in all_invitations:
            if inv.is_valid():
                # Enrich with team and inviter info
                inv.team_obj = inv.team
                inv.inviter_name = inv.created_by.first_name or inv.created_by.username
                valid_invitations.append(inv)
        
        return render_template('team/pending_invitations.html', 
                             invitations=valid_invitations,
                             invitation_count=len(valid_invitations))
    
    
    @app.route('/team/decline-invite/<token>', methods=['POST'])
    @login_required
    def team_decline_invite(token):
        """Decline a team invitation"""
        user_id = session.get('user_id')
        user = User.query.get(user_id)
        
        # Find invitation
        invitation = TeamInvitation.query.filter_by(invitation_token=token).first()
        
        if not invitation:
            flash('Invalid or expired invitation link', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Check if email matches
        if user.email != invitation.invited_email:
            flash('This invitation is not for your email address', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Mark as declined
        invitation.status = 'declined'
        invitation.accepted_at = datetime.utcnow()
        db.session.commit()
        
        flash(f'You have declined the invitation to {invitation.team.name}', 'info')
        return redirect(url_for('team_pending_invitations'))
    
    
    # ======================== TEAM MEMBER MANAGEMENT ========================
    
    @app.route('/team/<int:team_id>/member/<int:member_id>/edit', methods=['POST'])
    @login_required
    @require_team_admin
    def team_member_edit(team_id, member_id):
        """Edit team member role and permissions"""
        user_id = session.get('user_id')
        
        # Get and verify team
        team = Team.query.get_or_404(team_id)
        if not check_admin_access(team.id, user_id):
            flash('You do not have permission to edit team members', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Get member from THIS team
        member = TeamMember.query.filter_by(id=member_id, team_id=team.id).first_or_404()
        
        # Prevent removing self
        if member.user_id == user_id:
            flash('You cannot modify your own role', 'error')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        new_role = request.form.get('role', 'member')
        
        if new_role not in ['admin', 'member']:
            flash('Invalid role', 'error')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        # Update role
        old_role = member.role
        member.role = new_role
        member.updated_at = datetime.utcnow()
        db.session.commit()
        
        flash(f'{member.user.username}\'s role updated from {old_role} to {new_role}', 'success')
        return redirect(url_for('team_dashboard', team_id=team.id))
    
    
    @app.route('/team/<int:team_id>/member/<int:member_id>/delete', methods=['POST'])
    @login_required
    @require_team_admin
    def team_member_delete(team_id, member_id):
        """Remove team member"""
        user_id = session.get('user_id')
        
        # Get and verify team
        team = Team.query.get_or_404(team_id)
        if not check_admin_access(team.id, user_id):
            flash('You do not have permission to remove team members', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Get member from THIS team
        member = TeamMember.query.filter_by(id=member_id, team_id=team.id).first_or_404()
        
        # Prevent removing owner (owner cannot be removed)
        if team.owner_id == member.user_id:
            flash('You cannot remove the team owner', 'error')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        # Prevent removing self
        if member.user_id == user_id:
            flash('You cannot remove yourself from the team', 'error')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        member_name = member.user.username
        
        # Remove all channel access for this member
        ChannelAccess.query.filter_by(team_member_id=member.id).delete()
        
        # Remove member
        db.session.delete(member)
        db.session.commit()
        
        flash(f'{member_name} has been removed from the team', 'success')
        return redirect(url_for('team_dashboard', team_id=team.id))
    
    
    # ======================== CHANNEL ACCESS MANAGEMENT ========================
    
    @app.route('/team/<int:team_id>/member/<int:member_id>/channels', methods=['GET', 'POST'])
    @login_required
    @require_team_admin
    def team_member_channels(team_id, member_id):
        """Manage channel access for a team member"""
        user_id = session.get('user_id')
        
        # Get and verify team
        team = Team.query.get_or_404(team_id)
        if not check_admin_access(team.id, user_id):
            flash('You do not have permission to manage member permissions', 'error')
            return redirect(url_for('team_dashboard'))
        
        # Get member from THIS team
        member = TeamMember.query.filter_by(id=member_id, team_id=team.id).first_or_404()
        
        # Get all team channels
        channels = ConnectedPage.query.filter_by(user_id=team.owner_id).all()
        
        if request.method == 'POST':
            # Clear existing access for this member
            ChannelAccess.query.filter_by(team_member_id=member.id).delete()
            
            # Add new channel access based on form submission
            for channel in channels:
                access_level = request.form.get(f'channel_{channel.id}', 'none')
                
                if access_level != 'none':
                    channel_access = ChannelAccess(
                        team_id=team.id,
                        team_member_id=member.id,
                        channel_id=channel.id,
                        access_level=access_level
                    )
                    db.session.add(channel_access)
            
            db.session.commit()
            flash(f'Channel permissions updated for {member.user.username}', 'success')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        # Get current channel access for member
        current_access = {}
        for access in ChannelAccess.query.filter_by(team_member_id=member.id).all():
            current_access[access.channel_id] = access.access_level
        
        return render_template('team/member_channels.html',
                             team=team,
                             member=member,
                             channels=channels,
                             current_access=current_access)
    
    
    # ======================== OWNER-ONLY ROUTES ========================
    
    @app.route('/team/<int:team_id>/transfer-ownership', methods=['GET', 'POST'])
    @login_required
    @require_team_owner
    def team_transfer_ownership(team_id):
        """Transfer team ownership to another member"""
        user_id = session.get('user_id')
        team = Team.query.get_or_404(team_id)
        
        # Verify ownership
        if team.owner_id != user_id:
            flash('You must be the team owner to transfer ownership', 'error')
            return redirect(url_for('team_dashboard'))
        
        if request.method == 'POST':
            new_owner_id = request.form.get('new_owner_id')
            
            try:
                new_owner_id = int(new_owner_id)
            except (ValueError, TypeError):
                flash('Invalid member selected', 'error')
                return redirect(url_for('team_transfer_ownership', team_id=team.id))
            
            # Verify new owner is an admin
            new_owner_member = TeamMember.query.filter_by(
                team_id=team.id,
                user_id=new_owner_id,
                role='admin'
            ).first()
            
            if not new_owner_member:
                flash('New owner must be an admin member of the team', 'error')
                return redirect(url_for('team_transfer_ownership', team_id=team.id))
            
            # Transfer ownership
            team.owner_id = new_owner_id
            team.updated_at = datetime.utcnow()
            
            db.session.commit()
            
            flash(f'Team ownership transferred to {new_owner_member.user.username}', 'success')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        # Get eligible members (admins only)
        admins = team.get_admins()
        
        return render_template('team/transfer_ownership.html',
                             team=team,
                             eligible_members=admins)
    
    
    @app.route('/team/<int:team_id>/settings', methods=['GET', 'POST'])
    @login_required
    @require_team_owner
    def team_settings(team_id):
        """Team settings - name, etc"""
        user_id = session.get('user_id')
        team = Team.query.get_or_404(team_id)
        
        # Verify ownership
        if team.owner_id != user_id:
            flash('You must be the team owner to change settings', 'error')
            return redirect(url_for('team_dashboard'))
        
        if request.method == 'POST':
            new_name = request.form.get('name', '').strip()
            
            if not new_name or len(new_name) < 3:
                flash('Team name must be at least 3 characters long', 'error')
                return redirect(url_for('team_settings', team_id=team.id))
            
            old_name = team.name
            team.name = new_name
            team.updated_at = datetime.utcnow()
            db.session.commit()
            
            flash(f'Team name changed from "{old_name}" to "{new_name}"', 'success')
            return redirect(url_for('team_dashboard', team_id=team.id))
        
        return render_template('team/settings.html', team=team)

