import os
import requests
import random
from dotenv import load_dotenv
from io import IOBase
from datetime import datetime

# Load environment variables
load_dotenv()

# Get credentials from environment
PAGE_ID = os.getenv('FB_PAGE_ID')
PAGE_ACCESS_TOKEN = os.getenv('FB_PAGE_ACCESS_TOKEN')

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

def randomize_content(message):
    emoji = random.choice(EMOJI_LIST)  
    intro = random.choice(INTRO_PHRASES)
    closing = random.choice(CLOSING_PHRASES)
    
    # Construct the randomized message
    randomized_message = f"{intro} {emoji}\n\n{message}\n\n{closing}"
    
    return randomized_message

def post_to_facebook(message, media_paths=None, page_access_token=None, page_id=None):
    opened_files = []
    try:
        token = page_access_token or PAGE_ACCESS_TOKEN
        page = page_id or PAGE_ID

        if not token:
            raise ValueError("No page access token available")
        if not page:
            raise ValueError("No page ID available")

        # Randomize the content
        randomized_message = randomize_content(message)

        if media_paths:
            if not isinstance(media_paths, list):
                media_paths = [media_paths]  

            # Filter out Google Drive URLs (they shouldn't be in media_paths for video/photo upload)
            valid_paths = [p for p in media_paths if not p.startswith('http')]
            
            # Check if any videos are present
            has_video = any(path.lower().endswith(('.mp4', '.mov', '.avi')) for path in valid_paths if os.path.exists(path))

            if has_video:
                # Upload one video each post
                video_path = next((path for path in valid_paths if path.lower().endswith(('.mp4', '.mov', '.avi')) and os.path.exists(path)), None)
                if video_path:
                    url = f"https://graph.facebook.com/v24.0/{page}/videos"
                    video_file = open(video_path, 'rb')
                    opened_files.append(video_file)
                    files = {'source': video_file}
                    payload = {
                        'message': randomized_message,
                        'access_token': token
                    }
                    response = requests.post(url, files=files, data=payload)
                else:
                    raise ValueError("Video file not found")
            else:
                # For multiple photos
                photo_ids = []
                for photo_path in valid_paths:
                    if not os.path.exists(photo_path):
                        continue
                    url = f"https://graph.facebook.com/v24.0/{page}/photos"
                    photo_file = open(photo_path, 'rb')
                    opened_files.append(photo_file)
                    files = {'source': photo_file}
                    payload = {
                        'published': 'false',
                        'access_token': token
                    }
                    photo_response = requests.post(url, files=files, data=payload)
                    photo_response.raise_for_status()
                    photo_ids.append(photo_response.json()['id'])

                # Create the post with all photos
                url = f"https://graph.facebook.com/v24.0/{page}/feed"
                payload = {
                    'message': randomized_message,
                    'attached_media': [{'media_fbid': photo_id} for photo_id in photo_ids],
                    'access_token': token
                }
                response = requests.post(url, json=payload)
        else:
            # Text-only posts
            url = f"https://graph.facebook.com/v24.0/{page}/feed"
            payload = {
                'message': message,
                'access_token': token
            }
            response = requests.post(url, data=payload)

        response.raise_for_status()
        result = response.json()
        post_id = result.get('post_id') or result.get('id')
        print(f"Success! Post ID: {post_id}")
        return post_id

    except requests.exceptions.RequestException as e:
        print(f"Error posting to Facebook: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"Error posting to Facebook: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        # Ensure all opened files are closed
        for f in opened_files:
            try:
                f.close()
            except:
                pass

import requests
FB_API_VERSION = os.getenv("FB_API_VERSION", "v24.0")

def get_page_posts(page_id: str, access_token: str, limit: int = 25) -> list[dict]:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{page_id}/posts"
    params = {"access_token": access_token,
              "fields": "id,message,created_time",
              "limit": limit}
    data = requests.get(url, params=params).json().get("data", [])
    return data

def get_post_engagement(post_id: str, access_token: str) -> dict:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{post_id}"
    params = {
        "access_token": access_token,
        "fields": "shares,reactions.limit(0).summary(true),comments.limit(0).summary(true)",
    }
    result = requests.get(url, params=params).json()
    likes = result.get("reactions", {}).get("summary", {}).get("total_count", 0)
    comments = result.get("comments", {}).get("summary", {}).get("total_count", 0)
    shares = result.get("shares", {}).get("count", 0) if result.get("shares") else 0
    return {"likes": likes, "comments": comments, "shares": shares}

def get_post_insights(post_id: str, access_token: str, metrics=None) -> dict:
    if not metrics:
        metrics = ["post_impressions", "post_clicks"]
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{post_id}/insights"
    params = {"access_token": access_token, "metric": ",".join(metrics)}
    data = requests.get(url, params=params).json().get("data", [])
    insights = {}
    for item in data:
        # lấy giá trị gần nhất
        val = item["values"][0]["value"]
        if isinstance(val, dict):
            val = sum(val.values())
        insights[item["name"]] = val
    for metric in metrics:
        insights.setdefault(metric, 0)
    return insights

def get_post_media(post_id: str, access_token: str) -> list:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{post_id}"
    params = {
        "access_token": access_token,
        "fields": "attachments{media,type,url}"
    }
    try:
        result = requests.get(url, params=params).json()
        attachments = result.get("attachments", {}).get("data", [])
        media_list = []
        
        for attachment in attachments:
            media = attachment.get("media", {})
            media_type = attachment.get("type", "")
            
            if media_type == "photo":
                media_list.append({
                    "type": "photo",
                    "url": media.get("image", {}).get("src", ""),
                    "thumbnail": media.get("image", {}).get("src", "")
                })
            elif media_type == "video":
                media_list.append({
                    "type": "video",
                    "url": attachment.get("url", ""),
                    "thumbnail": media.get("image", {}).get("src", "")
                })
        
        return media_list
    except Exception as e:
        print(f"Error fetching post media: {e}")
        return []

if __name__ == "__main__":
    message = "Hello!"

    print(f"Posting to page: {PAGE_ID}")
    post_to_facebook(message)
