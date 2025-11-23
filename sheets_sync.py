"""
Google Sheets Sync Module for Postly
Syncs scheduled posts from Google Sheets to the Postly database
"""

import os
import sys
import re
import random
from datetime import datetime
import pytz

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

# Import Flask app and database models
from app import app, db, User, Post, PostMedia, PostPageAssociation, ConnectedPage

# Content variations for randomization
EMOJI_LIST = ["✨", "🎯", "💡", "🚀", "⭐", "🌟", "💫", "🎉"]
INTRO_PHRASES = [
    "Check this out!",
    "Here's something exciting:",
    "Don't miss this:",
    "Quick update:",
    "Attention:",
    "Hey there!",
]
CLOSING_PHRASES = [
    "Let us know what you think!",
    "Share your thoughts below!",
    "What do you think?",
    "Drop a comment!",
    "Tell us in the comments!",
    "We'd love to hear from you!",
]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def extract_google_drive_file_id(url):
    """Extract file ID from various Google Drive URL formats"""
    if not url:
        return None
    
    url = str(url).strip()
    
    # Format: https://drive.google.com/file/d/FILE_ID/view
    match = re.search(r'/file/d/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    
    # Format: https://drive.google.com/open?id=FILE_ID
    match = re.search(r'[?&]id=([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    
    if re.match(r'^[a-zA-Z0-9-_]+$', url):
        return url
    
    return None

def convert_google_drive_to_download_url(url):
    """Convert a Google Drive URL to a direct download URL"""
    file_id = extract_google_drive_file_id(url)
    if file_id:
        return f"https://drive.google.com/uc?id={file_id}&export=download"
    return url

def randomize_content(message, campaign=None):
    """Add random variations to the message while preserving its core content"""
    # Only add variations if message isn't too short
    if len(message) < 10:
        return message
    
    # Add random emoji
    emoji = random.choice(EMOJI_LIST)
    
    # Add random intro and closing phrases (only sometimes)
    use_intro = random.random() > 0.5
    use_closing = random.random() > 0.5
    
    intro = random.choice(INTRO_PHRASES) if use_intro else ""
    closing = random.choice(CLOSING_PHRASES) if use_closing else ""
    
    # Add campaign hashtag if provided
    campaign_tag = f"\n\n#{campaign.replace(' ', '')}" if campaign and campaign.strip() else ""
    
    # Construct the randomized message
    parts = []
    if intro:
        parts.append(f"{intro} {emoji}")
    parts.append(message)
    if closing:
        parts.append(closing)
    if campaign_tag:
        parts.append(campaign_tag)
    
    return "\n\n".join(parts)


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_credentials():
    """Get credentials from service account file"""
    try:
        # For production, use service account
        return service_account.Credentials.from_service_account_file(
            'google_credentials.json', scopes=SCOPES)
    except FileNotFoundError:
        raise Exception("Google Sheets credentials file not found. Please add google_credentials.json")

def init_sheets_service():
    """Initialize the Sheets API service"""
    try:
        creds = get_credentials()
        return build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"Error initializing Sheets service: {e}")
        return None

def read_schedule_sheet(spreadsheet_id, sheet_name=None):
    """Read schedule data from Google Sheets"""
    try:
        service = init_sheets_service()
        sheet = service.spreadsheets()

        # Get the actual sheet name if not provided
        if not sheet_name:
            # Get the first sheet name
            metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_name = metadata['sheets'][0]['properties']['title']

        # Build the range with the actual sheet name
        range_name = f"'{sheet_name}'!A2:J"
        
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        values = result.get('values', [])

        if not values:
            # Return an empty df with the expected columns 
            df_empty = pd.DataFrame(columns=columns)
            df_empty['row_index'] = []
            return df_empty

        # Convert to df 
        columns = [
            'message', 'page_ids', 'scheduled_time', 'status',
            'media_urls', 'campaign', 'author', 'notes', 'post_id', 'row_index'
        ]
        df = pd.DataFrame(values, columns=columns[:len(values[0])])
        
        # Add missing columns if needed
        for col in columns:
            if col not in df.columns:
                df[col] = None
                
        # Add row index (for updating specific rows later)
        df['row_index'] = range(2, len(df) + 2)  # Sheet rows start at 1, header at row 1
        
        return df

    except HttpError as err:
        print(f"Error reading Google Sheet: {err}")
        return None

def update_post_status(spreadsheet_id, row_index, status, post_id=None, sheet_name=None):
    """Update the status and post_id of a scheduled post in the sheet"""
    try:
        service = init_sheets_service()
        sheet = service.spreadsheets()

        # Get the sheet name
        if not sheet_name:
            metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_name = metadata['sheets'][0]['properties']['title']
        
        # Update status
        range_name = f"'{sheet_name}'!D{row_index}"
        body = {
            'values': [[status]]
        }
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        # Update post_id if provided
        if post_id:
            range_name = f"'{sheet_name}'!I{row_index}"
            body = {
                'values': [[post_id]]
            }
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

        return True
    except HttpError as err:
        print(f"Error updating Google Sheet: {err}")
        return False

def get_pending_posts(spreadsheet_id, sheet_name=None):
    """Get all pending posts that are scheduled to be published"""
    df = read_schedule_sheet(spreadsheet_id, sheet_name=sheet_name)
    if df is None:
        return []

    # Convert scheduled_time to datetime
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(vietnam_tz)

    pending_posts = []
    for _, row in df.iterrows():
        # Handling for missing/NaN values
        status_val = row.get('status') if 'status' in row.index else None
        if pd.isna(status_val):
            status = ''
        else:
            status = str(status_val).strip().lower()

        if status != 'pending':
            continue

        try:
            # scheduled_time must be present and parseable
            sched_val = row.get('scheduled_time') if 'scheduled_time' in row.index else None
            if pd.isna(sched_val) or not str(sched_val).strip():
                print(f"[SHEETS] Skipping row {row.get('row_index', '?')}: missing scheduled_time")
                continue

            # Parse scheduled time (assuming format: YYYY-MM-DD HH:MM)
            scheduled_time = vietnam_tz.localize(
                datetime.strptime(str(sched_val).strip(), '%Y-%m-%d %H:%M')
            )

            # Skip if not yet time to post
            if scheduled_time > now:
                continue

            # Convert comma-separated strings to lists, handle NaN
            page_ids_val = row.get('page_ids') if 'page_ids' in row.index else None
            if pd.isna(page_ids_val) or not str(page_ids_val).strip():
                page_ids = []
            else:
                page_ids = [pid.strip() for pid in str(page_ids_val).split(',') if pid.strip()]

            media_urls_val = row.get('media_urls') if 'media_urls' in row.index else None
            if pd.isna(media_urls_val) or not str(media_urls_val).strip():
                media_urls = []
            else:
                media_urls = [url.strip() for url in str(media_urls_val).split(',') if url.strip()]

            original_message = row.get('message') if 'message' in row.index else ''
            campaign = row.get('campaign') if 'campaign' in row.index else None
            
            # Create a single post that will be published to all pages
            # Each page will get a slightly randomized version during publishing
            pending_posts.append({
                'message': original_message,
                'page_ids': page_ids,  # All page IDs for this post
                'scheduled_time': scheduled_time,
                'media_urls': media_urls,
                'campaign': campaign,
                'row_index': row.get('row_index'),
                'author': row.get('author') if 'author' in row.index else None,
                'notes': row.get('notes') if 'notes' in row.index else None,
            })

        except (ValueError, AttributeError) as e:
            print(f"[SHEETS] Error parsing row {row.get('row_index', '?')}: {e}")
            continue

    return pending_posts


def sync_posts_from_sheets(spreadsheet_id, user_id, sheet_name=None):
    """
    Sync pending posts from Google Sheets to Postly database
    Returns: (success_count, error_count, errors_list)
    """
    with app.app_context():
        pending_posts = get_pending_posts(spreadsheet_id, sheet_name)
        
        if not pending_posts:
            print("[SHEETS] No pending posts to sync")
            return 0, 0, []
        
        print(f"[SHEETS] Found {len(pending_posts)} pending post(s) to sync")
        
        success_count = 0
        error_count = 0
        errors = []
        
        # Get user
        user = User.query.get(user_id)
        if not user:
            return 0, 0, [f"User {user_id} not found"]
        
        for post_data in pending_posts:
            try:
                # Get page IDs from the sheet
                page_ids = post_data.get('page_ids', [])
                if not page_ids:
                    error_msg = f"Row {post_data.get('row_index')}: No pages specified"
                    print(f"[SHEETS] {error_msg}")
                    errors.append(error_msg)
                    error_count += 1
                    continue
                
                # Verify pages exist and user has access
                valid_pages = []
                for page_id_str in page_ids:
                    try:
                        page_id = int(page_id_str)
                        page = ConnectedPage.query.get(page_id)
                        if page and page.user_id == user_id and page.is_active:
                            valid_pages.append(page)
                        else:
                            print(f"[SHEETS] Page {page_id} not found or not accessible for user {user_id}")
                    except (ValueError, TypeError):
                        print(f"[SHEETS] Invalid page ID: {page_id_str}")
                
                if not valid_pages:
                    error_msg = f"Row {post_data.get('row_index')}: No valid pages found"
                    print(f"[SHEETS] {error_msg}")
                    errors.append(error_msg)
                    error_count += 1
                    continue
                
                # Create randomized content for this post
                message = randomize_content(
                    post_data.get('message', ''),
                    post_data.get('campaign')
                )
                
                # Convert scheduled time to UTC
                scheduled_time_local = post_data.get('scheduled_time')
                if scheduled_time_local.tzinfo:
                    scheduled_time_utc = scheduled_time_local.astimezone(pytz.UTC).replace(tzinfo=None)
                else:
                    scheduled_time_utc = scheduled_time_local
                
                # Create Post
                post = Post(
                    user_id=user_id,
                    content=message,
                    caption=message,
                    status='scheduled',
                    scheduled_time=scheduled_time_utc,
                    post_icon='fas fa-calendar'
                )
                db.session.add(post)
                db.session.flush()  # Get post ID
                
                print(f"[SHEETS] Created post {post.id} for row {post_data.get('row_index')}")
                
                # Add media URLs
                media_urls = post_data.get('media_urls', [])
                for media_url in media_urls:
                    # Convert Google Drive links to direct download URLs
                    media_url = convert_google_drive_to_download_url(media_url)
                    
                    # Determine media type from URL
                    media_type = 'image'
                    if any(ext in media_url.lower() for ext in ['.mp4', '.mov', '.avi', '.mkv']):
                        media_type = 'video'
                    
                    post_media = PostMedia(
                        post_id=post.id,
                        media_url=media_url,
                        media_type=media_type
                    )
                    db.session.add(post_media)
                
                # Create associations with pages
                for page in valid_pages:
                    association = PostPageAssociation(
                        post_id=post.id,
                        page_id=page.id,
                        status='pending'
                    )
                    db.session.add(association)
                
                db.session.commit()
                
                # Update sheet status to 'synced'
                update_post_status(
                    spreadsheet_id, 
                    post_data.get('row_index'),
                    'synced',
                    post.id,
                    sheet_name
                )
                
                success_count += 1
                print(f"[SHEETS] Successfully synced post {post.id} to {len(valid_pages)} page(s)")
                
            except Exception as e:
                db.session.rollback()
                error_msg = f"Row {post_data.get('row_index')}: {str(e)}"
                print(f"[SHEETS] Error: {error_msg}")
                errors.append(error_msg)
                error_count += 1
                
                # Update sheet status to 'error'
                try:
                    update_post_status(
                        spreadsheet_id,
                        post_data.get('row_index'),
                        f'error: {str(e)[:50]}',
                        None,
                        sheet_name
                    )
                except:
                    pass
        
        return success_count, error_count, errors