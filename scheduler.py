from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import requests
from datetime import datetime, timedelta
import pytz
import logging
import sheets_sync
import os
import random

# Set up logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

class PostScheduler:
    def __init__(self, app=None):
        self.scheduler = None
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.app = app
        self.scheduler = BackgroundScheduler(
            job_defaults={
                'coalesce': True,  # Combine multiple pending runs into one
                'max_instances': 1  # Only one instance of each job can run at a time
            }
        )
        
        # Add the scheduled post processing job for database
        self.scheduler.add_job(
            self.process_scheduled_posts,
            CronTrigger(minute='*/5'),  # Every 5 minutes
            id='process_scheduled_posts',
            replace_existing=True
        )

        # Add the Google Sheets sync job
        self.scheduler.add_job(
            self.process_sheets_posts,
            CronTrigger(minute='*/2'),  # Every 2 minutes
            id='process_sheets_posts',
            replace_existing=True
        )

    def start(self):
        if self.scheduler:
            self.scheduler.start()
            print("Scheduler started successfully")
        else:
            print("Error: Scheduler not initialized")

    def shutdown(self):
        if self.scheduler:
            self.scheduler.shutdown()

    def process_sheets_posts(self):
        """Process scheduled posts from Google Sheets"""
        with self.app.app_context():
            try:
                spreadsheet_id = os.getenv('GOOGLE_SHEET_ID')
                sheet_name = os.getenv('GOOGLE_SHEET_NAME')
                if not spreadsheet_id:
                    print("GOOGLE_SHEET_ID not configured")
                    return

                from app import PageToken, db, fb_posting
                # Pass optional sheet_name through to the sheets sync functions
                pending_posts = sheets_sync.get_pending_posts(spreadsheet_id, sheet_name=sheet_name)
                print(f"process_sheets_posts: found {len(pending_posts)} pending rows (sheet='{sheet_name or '[first]'}')")

                for post in pending_posts:
                    try:
                        success = False
                        post_ids = []

                        # Process each page ID
                        for page_id in post['page_ids']:
                            # Get the page token
                            page = PageToken.query.filter_by(page_id=page_id).first()
                            if not page:
                                print(f"Page token not found for page ID: {page_id}")
                                continue

                            # Download media from URLs to temporary files if needed
                            media_paths = []
                            if post['media_urls']:
                                for url in post['media_urls']:
                                    if url:  # Skip empty URLs
                                        try:
                                            # Convert Google Drive links to download URLs
                                            download_url = sheets_sync.convert_google_drive_to_download_url(url)
                                            
                                            response = requests.get(download_url)
                                            if response.status_code == 200:
                                                # Create filename from URL or use a generic name
                                                filename = os.path.basename(url.split('?')[0]) or f"media_{os.urandom(4).hex()}"
                                                media_path = os.path.join(
                                                    self.app.config['UPLOAD_FOLDER'],
                                                    f"temp_{filename}"
                                                )
                                                with open(media_path, 'wb') as f:
                                                    f.write(response.content)
                                                media_paths.append(media_path)
                                                print(f"Downloaded media from {url}")
                                        except Exception as e:
                                            print(f"Error downloading media from {url}: {e}")

                            try:
                                # Post to Facebook
                                post_id = fb_posting.post_to_facebook(
                                    post['message'],
                                    media_paths if media_paths else None,
                                    page_access_token=page.page_access_token,
                                    page_id=page_id
                                )

                                if post_id:
                                    success = True
                                    post_ids.append(post_id)
                                
                            finally:
                                # Clean up temporary media files
                                for path in media_paths:
                                    if os.path.exists(path):
                                        os.remove(path)

                        # Update status in Google Sheet
                        if success:
                            sheets_sync.update_post_status(
                                spreadsheet_id,
                                post['row_index'],
                                'posted',
                                ','.join(post_ids),
                                sheet_name=sheet_name
                            )
                        else:
                            sheets_sync.update_post_status(
                                spreadsheet_id,
                                post['row_index'],
                                'failed',
                                sheet_name=sheet_name
                            )

                    except Exception as e:
                        print(f"Error processing post from sheet: {e}")
                        sheets_sync.update_post_status(
                            spreadsheet_id,
                            post['row_index'],
                            'failed',
                            sheet_name=sheet_name
                        )

            except Exception as e:
                print(f"Error in process_sheets_posts: {e}")

    def process_scheduled_posts(self):
        """Process all pending scheduled posts"""
        with self.app.app_context():
            try:
                # Process posts that are due
                now = datetime.utcnow()
                from app import ScheduledPost, PageToken, db, fb_posting
                import json

                # Get posts that are due
                due_posts = ScheduledPost.query.filter(
                    ScheduledPost.status == 'pending',
                    ScheduledPost.scheduled_time <= now
                ).all()

                for post in due_posts:
                    try:
                        # Get the page token
                        page = PageToken.query.filter_by(
                            session_id=post.session_id,
                            page_id=post.page_id
                        ).first()

                        if not page:
                            post.status = 'failed'
                            post.result = 'Page access token not found'
                            continue

                        media_paths = json.loads(post.media_paths) if post.media_paths else []

                        if media_paths:
                            # Post with all media in a single post
                            post_id = fb_posting.post_to_facebook(
                                post.message,
                                media_paths,
                                page_access_token=page.page_access_token,
                                page_id=page.page_id
                            )
                            if post_id:
                                post.status = 'completed'
                                post.result = f'Posted with ID: {post_id}'
                            else:
                                post.status = 'failed'
                                post.result = 'Failed to get post ID'
                        else:
                            # Post without media
                            post_id = fb_posting.post_to_facebook(
                                post.message,
                                None,
                                page_access_token=page.page_access_token,
                                page_id=page.page_id
                            )
                            if post_id:
                                post.status = 'completed'
                                post.result = f'Posted with ID: {post_id}'
                            else:
                                post.status = 'failed'
                                post.result = 'Failed to get post ID'

                    except Exception as e:
                        post.status = 'failed'
                        post.result = f'Error: {str(e)}'
                        print(f"Error processing scheduled post {post.id}: {str(e)}")

                    db.session.commit()

                print(f"Processed {len(due_posts)} scheduled posts")

            except Exception as e:
                print(f"Error in process_scheduled_posts: {str(e)}")

scheduler = PostScheduler()
