from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import fb_posting
import os
from werkzeug.utils import secure_filename
from urllib.parse import urlencode
import requests
import uuid
from flask_sqlalchemy import SQLAlchemy
import csv
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
import pytz
import random

load_dotenv()

def json_filter(value):
    """Custom filter to parse JSON strings in templates"""
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', os.path.join(BASE_DIR, 'uploads'))

# Create the Flask app with explicit template folder
app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dfe8216ff1440e9e8137744e5087c537')
app.config['SESSION_COOKIE_SECURE'] = True  
app.config['SERVER_NAME'] = os.getenv('SERVER_NAME')  # For OAuth redirect URI

# Register custom template filters
app.jinja_env.filters['fromjson'] = json_filter

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Database configuration
database_url = os.getenv('DATABASE_URL')
if database_url and database_url.startswith('postgres://'):
    # Render provides PostgreSQL URLs starting with postgres://
    # but SQLAlchemy needs postgresql://
    database_url = database_url.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url or f"sqlite:///{os.path.join(BASE_DIR, 'fb_tokens.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class PageToken(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(128), index=True)
    page_id = db.Column(db.String(64))
    page_name = db.Column(db.String(255))
    page_access_token = db.Column(db.String(1024))

class ScheduledPost(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(128), index=True)
    page_id = db.Column(db.String(64))
    message = db.Column(db.Text)
    media_paths = db.Column(db.Text)  # JSON string of media paths
    scheduled_time = db.Column(db.DateTime, index=True)
    status = db.Column(db.String(20), default='pending')  # pending, completed, failed
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    result = db.Column(db.Text, nullable=True)  # Store post ID or error message
    
    @property
    def media_paths_list(self):
        """Parse media_paths JSON string into a list"""
        if not self.media_paths:
            return []
        try:
            return json.loads(self.media_paths)
        except (json.JSONDecodeError, TypeError):
            return []

# Ensure DB exists
with app.app_context():
    db.create_all()

# Initialize scheduler and attach to app 
from scheduler import scheduler
scheduler.init_app(app)

SCHEDULER_STARTED = False

@app.before_request
def ensure_scheduler_started():
    """Start the scheduler once when the first request arrives.

    Using before_request with a module-level guard is more portable than relying on
    before_first_request in environments where it may not be available.
    """
    global SCHEDULER_STARTED
    if SCHEDULER_STARTED:
        return
    try:
        if scheduler.scheduler and not scheduler.scheduler.running:
            scheduler.start()
            print('Scheduler started (ensure_scheduler_started)')
        SCHEDULER_STARTED = True
    except Exception as e:
        print('Error starting scheduler in ensure_scheduler_started:', e)

# Facebook OAuth config
FB_APP_ID = os.getenv('FB_APP_ID')
FB_APP_SECRET = os.getenv('FB_APP_SECRET')
FB_API_VERSION = os.getenv('FB_API_VERSION', 'v24.0')
FB_REDIRECT_URI = os.getenv('FB_REDIRECT_URI')


@app.route('/', methods=['GET'])
def index():
    # Start with selecting post type
    return render_template('index.html', step='select_type')

@app.route('/post', methods=['GET', 'POST'])
def post():
    # Ensure a session identifier so we can tie stored tokens to this user/browser
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())

    if request.method == 'POST':
        message = request.form.get('message')
        media_files = request.files.getlist('media[]')
        media_paths = []
        
        try:
            # Save all media files
            for media in media_files:
                if media and media.filename:
                    filename = secure_filename(media.filename)
                    media_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    media.save(media_path)
                    media_paths.append(media_path)

            # Determine which page/token to use
            selected_page_id = request.form.get('page_select')
            page_token = None
            if selected_page_id:
                token_row = PageToken.query.filter_by(session_id=session['sid'], page_id=selected_page_id).first()
                if token_row:
                    page_token = token_row.page_access_token
            
            # Post with or without media (no delays)
            post_id = fb_posting.post_to_facebook(message, media_paths, page_access_token=page_token, page_id=selected_page_id)
            if post_id:
                flash(f'Success! Post ID: {post_id}', 'success')
            else:
                flash('Failed to post to Facebook.', 'danger')
        except Exception as e:
            flash(f'Error: {str(e)}', 'danger')
        finally:
            # Clean up all media files
            for media_path in media_paths:
                if os.path.exists(media_path):
                    os.remove(media_path)
        return redirect(url_for('post'))
    # Query stored pages for this session to populate the page select dropdown
    pages = PageToken.query.filter_by(session_id=session.get('sid')).all()
    return render_template('index.html', step='post', pages=pages)


@app.route('/bulk_upload', methods=['POST'])
def bulk_upload():
    if 'sid' not in session:
        flash('Please connect your Facebook account first', 'warning')
        return redirect(url_for('index'))
        
    message = request.form.get('bulk_message')
    media_files = request.files.getlist('bulk_media[]')
    media_paths = []
    results = []

    try:
        # Save all media files
        for media in media_files:
            if media and media.filename:
                filename = secure_filename(media.filename)
                media_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                media.save(media_path)
                media_paths.append(media_path)

        # Get all selected pages for this session
        pages = PageToken.query.filter_by(session_id=session['sid']).all()
        if not pages:
            flash('No pages selected. Please select pages first.', 'warning')
            return redirect(url_for('select_pages'))

        # Create scheduled posts for each page with random delays to avoid Meta ban/suspension
        # This ensures posts are spread out naturally across pages (2-10 minutes between each page)
        now = datetime.utcnow()
        for idx, page in enumerate(pages):
            # Add cumulative random delay per page to spread posts naturally
            # First page posts immediately, then each subsequent page has additional delay
            delay_seconds = random.randint(120, 600)  # 2-10 minutes per page
            cumulative_delay = timedelta(seconds=delay_seconds * (idx + 1))
            scheduled_time = now + cumulative_delay
            
            try:
                scheduled_post = ScheduledPost(
                    session_id=session['sid'],
                    page_id=page.page_id,
                    message=message,
                    media_paths=json.dumps(media_paths.copy()),
                    scheduled_time=scheduled_time,
                    status='pending'
                )
                db.session.add(scheduled_post)
                
                page_result = {
                    'success': True, 
                    'post_id': f'SCHEDULED_{scheduled_post.id}',
                    'scheduled_time': scheduled_time.isoformat()
                }
            except Exception as e:
                page_result = {'success': False, 'error': str(e)}

            results.append({
                'page_id': page.page_id,
                'page_name': page.page_name,
                'posts': [page_result]
            })
        
        db.session.commit()
    finally:
        # Clean up all media files
        for media_path in media_paths:
            if os.path.exists(media_path):
                os.remove(media_path)

    return render_template('bulk_results.html', results=results)


@app.route('/login')
def login():
    # Redirect the user to Facebook's OAuth dialog
    if not FB_APP_ID or not FB_REDIRECT_URI:
        flash('Facebook OAuth not configured (FB_APP_ID/FB_REDIRECT_URI)', 'danger')
        return redirect(url_for('index'))

    # Create and save a state value to protect against CSRF
    state = uuid.uuid4().hex
    session['oauth_state'] = state

    params = {
        'client_id': FB_APP_ID,
        'redirect_uri': FB_REDIRECT_URI,
        'scope': 'pages_manage_posts,pages_read_engagement,pages_show_list',
        'response_type': 'code',
        'state': state,
        'auth_type': 'rerequest' 
    }
    auth_url = f'https://www.facebook.com/{FB_API_VERSION}/dialog/oauth?{urlencode(params)}'
    return redirect(auth_url)


@app.route('/oauth/callback')
def oauth_callback():
    # Handle the OAuth callback from Facebook
    code = request.args.get('code')
    state = request.args.get('state')
    # Verify state to prevent CSRF
    saved_state = session.get('oauth_state')
    if saved_state is None or state != saved_state:
        flash('Invalid OAuth state. Please try connecting again.', 'danger')
        return redirect(url_for('index'))
    # remove state after check
    session.pop('oauth_state', None)
    if not code:
        flash('No code returned from Facebook', 'danger')
        return redirect(url_for('index'))
    # Exchange code for short-lived token
    token_exchange = requests.get(
        f'https://graph.facebook.com/{FB_API_VERSION}/oauth/access_token',
        params={
            'client_id': FB_APP_ID,
            'redirect_uri': FB_REDIRECT_URI,
            'client_secret': FB_APP_SECRET,
            'code': code
        }
    ).json()

    if 'error' in token_exchange:
        err = token_exchange.get('error', {})
        error_message = err.get('message', '')
        if 'app not authorized' in error_message.lower():
            flash("You need to be added as a tester while the app is in development mode. Please contact the administrator.", 'danger')
        elif 'permissions' in error_message.lower():
            flash("Required permissions not granted. Please ensure you accept all permission requests.", 'danger')
        else:
            flash(f"Token exchange error: {err.get('message', err)}", 'danger')
        return redirect(url_for('index'))

    short_lived_token = token_exchange.get('access_token')
    if not short_lived_token:
        flash('Failed to get access token from Facebook', 'danger')
        return redirect(url_for('index'))

    # Exchange for long-lived token
    long_token_res = requests.get(
        f'https://graph.facebook.com/{FB_API_VERSION}/oauth/access_token',
        params={
            'grant_type': 'fb_exchange_token',
            'client_id': FB_APP_ID,
            'client_secret': FB_APP_SECRET,
            'fb_exchange_token': short_lived_token
        }
    ).json()

    if 'error' in long_token_res:
        err = long_token_res.get('error')
        flash(f"Long token exchange error: {err.get('message', err)}", 'danger')
        return redirect(url_for('index'))

    long_lived_token = long_token_res.get('access_token')
    if not long_lived_token:
        flash('Failed to exchange for long-lived token', 'danger')
        return redirect(url_for('index'))

    # Get pages the user manages and their page access tokens
    pages_res = requests.get(
        f'https://graph.facebook.com/{FB_API_VERSION}/me/accounts',
        params={
            'access_token': long_lived_token
        }
    ).json()

    if 'error' in pages_res:
        err = pages_res.get('error')
        flash(f"Error fetching pages: {err.get('message', err)}", 'danger')
        return redirect(url_for('index'))

    pages = pages_res.get('data', [])
    # Store page tokens in DB associated with this session
    sid = session.get('sid') or str(uuid.uuid4())
    session['sid'] = sid
    for p in pages:
        # Upsert
        existing = PageToken.query.filter_by(session_id=sid, page_id=p.get('id')).first()
        if existing:
            existing.page_name = p.get('name')
            existing.page_access_token = p.get('access_token')
        else:
            new = PageToken(session_id=sid, page_id=p.get('id'), page_name=p.get('name'), page_access_token=p.get('access_token'))
            db.session.add(new)
    db.session.commit()

    flash('Facebook connected — please select the pages you want to manage', 'success')
    return redirect(url_for('select_pages'))

@app.route('/select_pages', methods=['GET', 'POST'])
def select_pages():
    if 'sid' not in session:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        # Get selected pages
        selected_pages = request.form.getlist('selected_pages')
        if not selected_pages:
            flash('Please select at least one page', 'warning')
            return redirect(url_for('select_pages'))
            
        # Keep only selected pages in database
        PageToken.query.filter_by(session_id=session['sid']).filter(~PageToken.page_id.in_(selected_pages)).delete(synchronize_session=False)
        db.session.commit()
        
        flash('Pages saved successfully!', 'success')
        return redirect(url_for('post'))
        
    # Get all available pages for this session
    pages = PageToken.query.filter_by(session_id=session['sid']).all()
    return render_template('index.html', step='select_pages', pages=pages)

@app.route('/schedule', methods=['GET'])
def schedule():
    if 'sid' not in session:
        return redirect(url_for('login'))
    
    pages = PageToken.query.filter_by(session_id=session['sid']).all()
    
    # Get scheduled posts from the database
    db_scheduled_posts = ScheduledPost.query.filter_by(session_id=session['sid']).order_by(ScheduledPost.scheduled_time).all()
    
    # Convert UTC times to Vietnam timezone for display
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    for post in db_scheduled_posts:
        post.scheduled_time = post.scheduled_time.replace(tzinfo=pytz.UTC).astimezone(vietnam_tz)
    
    # Get scheduled posts from Google Sheets
    try:
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
        if spreadsheet_id:
            from sheets_sync import read_schedule_sheet
            sheet_posts = read_schedule_sheet(spreadsheet_id)
            if sheet_posts is not None and not sheet_posts.empty:
                sheet_posts_list = []
                for _, post in sheet_posts.iterrows():
                    # Convert scheduled time to datetime if it's not already
                    if post.get('scheduled_time') is not None and str(post['scheduled_time']).strip():
                        try:
                            scheduled_time_str = str(post['scheduled_time']).strip()
                            # Try multiple date formats
                            try:
                                scheduled_time = vietnam_tz.localize(
                                    datetime.strptime(scheduled_time_str, '%Y-%m-%d %H:%M')
                                )
                            except ValueError:
                                try:
                                    scheduled_time = vietnam_tz.localize(
                                        datetime.strptime(scheduled_time_str, '%m/%d/%Y %H:%M')
                                    )
                                except ValueError:
                                    print(f"Could not parse date: {scheduled_time_str}")
                                    continue
                        except Exception as e:
                            print(f"Error parsing scheduled_time: {e}")
                            continue
                    else:
                        continue
                    
                    # Convert media URLs to a list
                    media_urls = []
                    if post.get('media_urls') is not None and str(post['media_urls']).strip():
                        media_urls = [url.strip() for url in str(post['media_urls']).split(',') if url.strip()]
                    
                    # Convert page IDs to a list
                    page_ids = []
                    if post.get('page_ids') is not None and str(post['page_ids']).strip():
                        page_ids = [pid.strip() for pid in str(post['page_ids']).split(',') if pid.strip()]
                    
                    sheet_posts_list.append({
                        'source': 'sheets',
                        'message': str(post.get('message', '')),
                        'page_ids': ','.join(page_ids),
                        'scheduled_time': scheduled_time.isoformat(),
                        'status': str(post.get('status', 'pending')).lower(),
                        'media_urls': media_urls,
                        'campaign': str(post.get('campaign', '')) if post.get('campaign') is not None else '',
                        'row_index': int(post.get('row_index', 0)) if post.get('row_index') is not None else 0,
                        'author': str(post.get('author', '')) if post.get('author') is not None else '',
                        'notes': str(post.get('notes', '')) if post.get('notes') is not None else '',
                    })
        else:
            sheet_posts_list = []
    except Exception as e:
        print(f"Error fetching Google Sheets data: {e}")
        import traceback
        traceback.print_exc()
        sheet_posts_list = []
    
    return render_template('schedule.html', 
                         pages=pages, 
                         database_posts=db_scheduled_posts,
                         sheet_posts=sheet_posts_list)

@app.route('/schedule/create', methods=['POST'])
def create_schedule():
    if 'sid' not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    try:
        # Handle form data for media files
        # Convert input time from Vietnam timezone to UTC for storage (store as naive UTC)
        local_time = datetime.fromisoformat(request.form['scheduled_time'].replace('Z', ''))
        vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        local_time = vietnam_tz.localize(local_time)
        scheduled_time = local_time.astimezone(pytz.UTC).replace(tzinfo=None)
        message = request.form['message']
        
        # Handle bulk scheduling
        page_ids = request.form.getlist('page_ids[]') if 'page_ids[]' in request.form else [request.form['page_id']]
        
        # Verify page access for all pages
        pages = PageToken.query.filter(
            PageToken.session_id == session['sid'],
            PageToken.page_id.in_(page_ids)
        ).all()
        
        if len(pages) != len(page_ids):
            return jsonify({'error': 'One or more pages not found or access denied'}), 403

        # Handle media files and Google Drive URLs
        media_files = request.files.getlist('media[]')
        google_drive_url = request.form.get('google_drive_url', '').strip()
        media_paths = []
        scheduled_posts = []
        
        try:
            # Save local media files first
            for media in media_files:
                if media and media.filename:
                    filename = secure_filename(f"{uuid.uuid4()}_{media.filename}")
                    media_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    media.save(media_path)
                    media_paths.append(media_path)
            
            # Add Google Drive URL if provided
            if google_drive_url:
                media_paths.append(google_drive_url)

            # Create scheduled posts for each page
            for page_id in page_ids:
                # Add random delay between 2-10 minutes for each page
                random_delay = timedelta(seconds=random.randint(120, 600))
                page_scheduled_time = scheduled_time + random_delay
                
                scheduled_post = ScheduledPost(
                    session_id=session['sid'],
                    page_id=page_id,
                    message=message,
                    media_paths=json.dumps(media_paths.copy()),  # Copy the list to ensure each post has its own copy
                    scheduled_time=page_scheduled_time,
                    status='pending'
                )
                scheduled_posts.append(scheduled_post)
                db.session.add(scheduled_post)
            
            db.session.commit()
            
            return jsonify({
                'posts': [{
                    'id': post.id,
                    'page_id': post.page_id,
                    'scheduled_time': scheduled_time.isoformat(),
                    'status': 'pending'
                } for post in scheduled_posts]
            })
            
        except Exception as e:
            # Clean up any saved media files if there was an error
            for path in media_paths:
                if os.path.exists(path):
                    os.remove(path)
            raise e
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/schedule/<int:post_id>', methods=['DELETE'])
def manage_schedule(post_id):
    if 'sid' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
        
    post = ScheduledPost.query.filter_by(id=post_id, session_id=session['sid']).first()
    if not post:
        return jsonify({'error': 'Post not found'}), 404
    if post.status != 'pending':
        return jsonify({'error': 'Cannot delete non-pending posts'}), 400

    try:
        # Clean up associated media files
        if post.media_paths:
            media_paths = json.loads(post.media_paths)
            for media_path in media_paths:
                if os.path.exists(media_path):
                    os.remove(media_path)

        # Delete the post from database
        db.session.delete(post)
        db.session.commit()
        
        return jsonify({'status': 'deleted'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/manage_posts', methods=['GET'])
def manage_posts_list():
    """List all pages for the user to select which one to manage."""
    if 'sid' not in session:
        return redirect(url_for('login'))
    
    pages = PageToken.query.filter_by(session_id=session['sid']).all()
    
    if not pages:
        flash('No pages found. Please connect your Facebook account.', 'warning')
        return redirect(url_for('login'))
    
    # Add post counts for each page
    pages_with_counts = []
    for page in pages:
        try:
            posts = fb_posting.get_page_posts(page.page_id, page.page_access_token, limit=1)
            posts_count = len(posts) if posts else 0
        except:
            posts_count = 0
        
        pages_with_counts.append({
            'page_id': page.page_id,
            'page_name': page.page_name,
            'posts_count': posts_count
        })
    
    return render_template('manage_posts_select.html', pages=pages_with_counts)

@app.route('/manage_posts/<string:page_id>')
def manage_posts(page_id):
    """Management interface for each page."""
    if 'sid' not in session:
        return redirect(url_for('index'))

    # Find token in db
    token_row = PageToken.query.filter_by(session_id=session['sid'],
                                          page_id=page_id).first()
    if not token_row:
        flash('Page does not exist or you have not selected this page', 'danger')
        return redirect(url_for('manage_posts_list'))

    # Get page posts
    posts = fb_posting.get_page_posts(page_id, token_row.page_access_token)
    posts_data = []
    for post in posts:
        engagement = fb_posting.get_post_engagement(post['id'],
                                                    token_row.page_access_token)
        insights = fb_posting.get_post_insights(
            post['id'], token_row.page_access_token,
            metrics=['post_impressions', 'post_clicks']
        )
        posts_data.append({
            'id': post['id'],
            'message': post.get('message', ''),
            'created_time': post.get('created_time', ''),
            'likes': engagement['likes'],
            'comments': engagement['comments'],
            'shares': engagement['shares'],
            'impressions': insights.get('post_impressions', 0),
            'clicks': insights.get('post_clicks', 0)
        })
    return render_template('manage_posts.html',
                           page_id=page_id,
                           page_name=token_row.page_name,
                           posts=posts_data)

if __name__ == '__main__':
    try:
        app.run(debug=True)
    finally:
        try:
            if scheduler.scheduler and scheduler.scheduler.running:
                scheduler.shutdown()
        except Exception:
            pass
