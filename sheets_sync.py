import os
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import pytz
import random
from time import sleep
import re

# Content variations for randomization
EMOJI_LIST = ["emoji1", "emoji2"]
INTRO_PHRASES = [
    "intro1",
    "intro2"
]
CLOSING_PHRASES = [
    "outro1",
    "outro2"
]

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
    # Add random emoji
    emoji = random.choice(EMOJI_LIST)
    
    # Add random intro and closing phrases
    intro = random.choice(INTRO_PHRASES)
    closing = random.choice(CLOSING_PHRASES)
    
    # Add campaign hashtag if provided
    campaign_tag = f"\n#{campaign}" if campaign and campaign.strip() else ""
    
    # Construct the randomized message
    randomized_message = f"{intro} {emoji}\n\n{message}\n\n{closing}{campaign_tag}"
    
    return randomized_message

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
                print(f"Skipping row {row.get('row_index', '?')}: missing scheduled_time")
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
            
            # Create a list to store page-specific variations
            page_specific_posts = []
            
            # Create unique variations for each page
            for page_id in page_ids:
                # Randomize the content for each page
                randomized_message = randomize_content(original_message, campaign)
                
                # Create a separate post entry for each page with randomized content
                page_specific_posts.append({
                    'message': randomized_message,
                    'page_ids': [page_id],  # Single page ID for this specific post
                    'scheduled_time': scheduled_time,
                    'media_urls': media_urls,
                    'campaign': campaign,
                    'row_index': row.get('row_index'),
                    'author': row.get('author') if 'author' in row.index else None,
                    'notes': row.get('notes') if 'notes' in row.index else None,
                    'delay': random.randint(120, 600)  # Random delay between 2-10 minutes
                })
            
            # Add all page-specific variations to pending posts
            pending_posts.extend(page_specific_posts)

        except (ValueError, AttributeError) as e:
            print(f"Error parsing row {row.get('row_index', '?')}: {e}")
            continue

    return pending_posts
