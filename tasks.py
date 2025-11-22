"""
Background tasks for fetching analytics from Facebook/Instagram APIs
"""
import logging
from datetime import datetime, timedelta
from flask import current_app
import requests

logger = logging.getLogger(__name__)

def get_db():
    """Get the SQLAlchemy db instance from current app"""
    # Flask-SQLAlchemy stores the db instance directly in extensions
    return current_app.extensions.get('sqlalchemy')

def fetch_facebook_comments_count(page_access_token, post_id):
    """
    Fetch comment count for a post from Facebook Graph API
    
    Args:
        page_access_token: Facebook page access token
        post_id: Facebook post ID
    
    Returns:
        int: Number of comments on the post
    """
    try:
        url = f"https://graph.facebook.com/v18.0/{post_id}/comments"
        params = {
            'summary': 'true',
            'access_token': page_access_token
        }
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            summary = data.get('summary', {})
            comment_count = summary.get('total_count', 0)
            print(f"[COMMENTS] Retrieved comment count: {comment_count}")
            return comment_count
        else:
            print(f"[COMMENTS] Failed to fetch comments ({response.status_code})")
            return 0
    except Exception as e:
        print(f"[COMMENTS] Error fetching comments: {e}")
        logger.warning(f"Error fetching comments for post {post_id}: {e}")
        return 0

def fetch_facebook_shares_count(page_access_token, post_id):
    """
    Fetch share count for a post from Facebook Graph API
    
    Args:
        page_access_token: Facebook page access token
        post_id: Facebook post ID
    
    Returns:
        int: Number of shares of the post
    """
    try:
        url = f"https://graph.facebook.com/v18.0/{post_id}"
        params = {
            'fields': 'shares',
            'access_token': page_access_token
        }
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            shares_count = data.get('shares', 0)
            print(f"[SHARES] Retrieved share count: {shares_count}")
            return shares_count
        else:
            print(f"[SHARES] Failed to fetch shares ({response.status_code})")
            return 0
    except Exception as e:
        print(f"[SHARES] Error fetching shares: {e}")
        logger.warning(f"Error fetching shares for post {post_id}: {e}")
        return 0


def fetch_facebook_post_analytics(page_access_token, post_id, post_page_assoc_id):
    """
    Fetch analytics for a single post from Facebook Graph API
    
    Args:
        page_access_token: Facebook page access token (must have read_insights permission)
        post_id: Facebook post ID (format: page_id_post_id)
        post_page_assoc_id: Database ID of PostPageAssociation
    """
    try:
        # Verify we have a valid token
        if not page_access_token:
            print(f"[INSIGHTS] ERROR: No access token provided for post {post_id}")
            return False
        
        # Use the edge API to get basic post information and engagement
        # This endpoint returns post metadata including likes, comments, shares
        url = f"https://graph.facebook.com/v18.0/{post_id}"
        
        # Get basic post fields - use simpler syntax without summary aggregation
        params = {
            'fields': 'id,message,created_time,likes,comments',
            'access_token': page_access_token
        }
        
        print(f"[INSIGHTS] Fetching analytics for post {post_id}")
        print(f"[INSIGHTS] Endpoint: {url}")
        print(f"[INSIGHTS] Fields: id, message, created_time, likes, comments")
        
        response = requests.get(url, params=params, timeout=15)
        
        # Fetch response
        if response.status_code != 200:
            error_text = response.text
            try:
                error_data = response.json()
                if 'error' in error_data:
                    error_msg = error_data['error'].get('message', error_text)
                    error_code = error_data['error'].get('code', 'unknown')
                    if error_code == 104:  # Access token invalid
                        logger.error(f"[INSIGHTS] Invalid access token for post {post_id}")
                        print(f"[INSIGHTS] ERROR 104: Invalid access token")
                        return False
                    elif error_code == 200:  # Permissions error
                        logger.error(f"[INSIGHTS] Insufficient permissions (200) for post {post_id}: {error_msg}")
                        print(f"[INSIGHTS] ERROR 200: Insufficient permissions - {error_msg}")
                        return False
                    else:
                        logger.error(f"[INSIGHTS] Facebook API error ({error_code}): {error_msg}")
                        print(f"[INSIGHTS] ERROR {error_code}: {error_msg}")
            except:
                logger.error(f"[INSIGHTS] Failed to get analytics ({response.status_code}): {error_text}")
                print(f"[INSIGHTS] FAILED ({response.status_code}): {error_text}")
            return False
        
        response.raise_for_status()
        data = response.json()
        print(f"[INSIGHTS] Response received successfully")
        
        # Parse the response - data contains the post fields
        analytics_data = {
            'impressions': 0,
            'reach': 0,
            'clicks': 0,
            'likes': 0,
            'comments': 0,
            'shares': 0,
            'engagement': 0
        }
        
        # Extract likes count
        if 'likes' in data and isinstance(data['likes'], dict):
            analytics_data['likes'] = data['likes'].get('summary', {}).get('total_count', 0)
            print(f"[INSIGHTS]   likes = {analytics_data['likes']}")
        
        # Extract comments count
        if 'comments' in data and isinstance(data['comments'], dict):
            analytics_data['comments'] = data['comments'].get('summary', {}).get('total_count', 0)
            print(f"[INSIGHTS]   comments = {analytics_data['comments']}")
        
        # Extract shares count
        if 'shares' in data:
            analytics_data['shares'] = data['shares'].get('count', 0)
            print(f"[INSIGHTS]   shares = {analytics_data['shares']}")
        
        print(f"[INSIGHTS] Parsed analytics: {analytics_data}")
        
        # Get db within current app context
        db = get_db()
        # Get the PostAnalytics model dynamically
        from app import PostAnalytics
        
        analytics = PostAnalytics.query.filter_by(
            post_page_association_id=post_page_assoc_id
        ).first()
        
        if analytics:
            analytics.impressions = analytics_data['impressions']
            analytics.reach = analytics_data['reach']
            analytics.clicks = analytics_data['clicks']
            analytics.likes = analytics_data['likes']
            analytics.comments = analytics_data['comments']
            analytics.shares = analytics_data['shares']
            analytics.last_updated = datetime.utcnow()
            print(f"[INSIGHTS] Updated existing analytics record")
        else:
            analytics = PostAnalytics(
                post_page_association_id=post_page_assoc_id,
                impressions=analytics_data['impressions'],
                reach=analytics_data['reach'],
                clicks=analytics_data['clicks'],
                likes=analytics_data['likes'],
                comments=analytics_data['comments'],
                shares=analytics_data['shares']
            )
            db.session.add(analytics)
            print(f"[INSIGHTS] Created new analytics record")
        
        db.session.commit()
        logger.info(f"[INSIGHTS] Successfully updated analytics for post {post_id}: impressions={analytics_data['impressions']}, likes={analytics_data['likes']}, comments={analytics_data['comments']}, shares={analytics_data['shares']}")
        print(f"[INSIGHTS] SUCCESS: Successfully saved analytics to database")
        return True
        
    except requests.exceptions.Timeout:
        logger.error(f"[INSIGHTS] Timeout fetching analytics for post {post_id}")
        print(f"[INSIGHTS] TIMEOUT fetching data")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"[INSIGHTS] Network error fetching analytics for post {post_id}: {e}")
        print(f"[INSIGHTS] NETWORK ERROR: {e}")
        return False
    except Exception as e:
        logger.error(f"[INSIGHTS] Unexpected error fetching analytics for post {post_id}: {e}")
        print(f"[INSIGHTS] UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def refresh_all_post_analytics():
    """
    Fetch analytics for all published posts from all connected pages
    This should be called periodically (e.g., every hour or every 6 hours)
    """
    try:
        print("\n[REFRESH] ========== Starting analytics refresh ==========")
        logger.info("Starting refresh_all_post_analytics task")
        
        # Get db instance that's registered with current app context
        db = get_db()
        # Get the model classes using db.Model registry
        from app import PostPageAssociation
        
        # Execute query using db.session directly to ensure it uses the right connection
        # Query all published posts with their page associations
        associations = db.session.query(PostPageAssociation).filter(
            PostPageAssociation.status.in_(['sent', 'published'])
        ).all()
        
        print(f"[REFRESH] Found {len(associations)} published post-page associations")
        logger.info(f"Found {len(associations)} published post-page associations")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for assoc in associations:
            # Skip if no platform_post_id
            if not assoc.platform_post_id:
                print(f"[REFRESH] Skipping association {assoc.id}: no platform_post_id")
                logger.debug(f"Skipping association {assoc.id}: no platform_post_id")
                skipped_count += 1
                continue
            
            page = assoc.connected_page
            
            # Only fetch for Facebook and Instagram
            if page.platform.lower() not in ['facebook', 'instagram']:
                print(f"[REFRESH] Skipping association {assoc.id}: platform {page.platform} not supported")
                skipped_count += 1
                continue
            
            # Check if page has access token (try page_access_token first, then fallback to access_token)
            access_token = page.page_access_token or page.access_token
            if not access_token:
                print(f"[REFRESH] Skipping association {assoc.id}: no access token available")
                logger.warning(f"Skipping association {assoc.id}: no access token available")
                failed_count += 1
                continue
            
            print(f"[REFRESH] Fetching analytics for post {assoc.platform_post_id} on page {page.page_name}")
            
            # Try to fetch analytics
            if fetch_facebook_post_analytics(
                access_token,
                assoc.platform_post_id,
                assoc.id
            ):
                success_count += 1
                print(f"[REFRESH] SUCCESS: Successfully fetched analytics")
            else:
                failed_count += 1
                print(f"[REFRESH] X Failed to fetch analytics")
        
        print(f"[REFRESH] ========== Analytics refresh complete ==========")
        print(f"[REFRESH] Success: {success_count}, Failed: {failed_count}, Skipped: {skipped_count}")
        logger.info(f"refresh_all_post_analytics completed: {success_count} success, {failed_count} failed, {skipped_count} skipped")
        return {'success': success_count, 'failed': failed_count, 'skipped': skipped_count}
        
    except Exception as e:
        logger.error(f"Error in refresh_all_post_analytics: {e}")
        print(f"[REFRESH] ========== ERROR in analytics refresh ==========")
        print(f"[REFRESH] Error: {e}")
        import traceback
        traceback.print_exc()
        return {'success': 0, 'failed': 0, 'skipped': 0, 'error': str(e)}


def setup_scheduler():
    """
    Setup APScheduler to run analytics refresh periodically
    Call this from your Flask app initialization
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        
        scheduler = BackgroundScheduler()
        
        # Run every 6 hours
        scheduler.add_job(
            func=refresh_all_post_analytics,
            trigger=IntervalTrigger(hours=6),
            id='refresh_post_analytics',
            name='Refresh post analytics from Facebook',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Scheduler started: analytics refresh every 6 hours")
        
    except ImportError:
        logger.warning("APScheduler not installed. Analytics refresh will not run automatically.")
    except Exception as e:
        logger.error(f"Error setting up scheduler: {e}")
