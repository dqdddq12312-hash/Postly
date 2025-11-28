"""
Background tasks for fetching analytics from Facebook/Instagram APIs
"""
import logging
from datetime import datetime, timedelta
from flask import current_app, has_app_context
from app import app, db, Post, PostPageAssociation, PostAnalytics, ConnectedPage
from tiktok_service import TikTokApiError, fetch_tiktok_post_stats
import requests

logger = logging.getLogger(__name__)
ANALYTICS_REFRESH_COOLDOWN_HOURS = 1
AUTO_PUBLISH_LOCK_TIMEOUT_MINUTES = 5


def _run_with_correct_app_context(function):
    """Ensure SQLAlchemy uses the Flask app instance bound to `db`."""
    needs_own_context = True
    if has_app_context():
        try:
            if current_app._get_current_object() is app:
                needs_own_context = False
        except RuntimeError:
            needs_own_context = True
    if needs_own_context:
        with app.app_context():
            return function()
    return function()


def _get_accessible_page_ids_subquery(user_id):
    """Return a subquery of ConnectedPage IDs the user can access."""
    if not user_id:
        return None
    from app import ChannelAccess, TeamMember
    return db.session.query(ConnectedPage.id).filter(
        db.or_(
            ConnectedPage.user_id == user_id,
            ConnectedPage.id.in_(
                db.session.query(ChannelAccess.channel_id).join(
                    TeamMember, ChannelAccess.team_member_id == TeamMember.id
                ).filter(TeamMember.user_id == user_id)
            )
        )
    ).subquery()


def _build_posts_needing_refresh_query(user_id=None, page_id=None):
    """Shared base query for posts that require analytics refresh."""
    cooldown_cutoff = datetime.utcnow() - timedelta(hours=ANALYTICS_REFRESH_COOLDOWN_HOURS)
    query = PostPageAssociation.query.filter(
        PostPageAssociation.status.in_(['sent', 'published']),
        PostPageAssociation.platform_post_id.isnot(None)
    )

    if user_id:
        accessible_page_ids = _get_accessible_page_ids_subquery(user_id)
        if accessible_page_ids is not None:
            query = query.filter(PostPageAssociation.page_id.in_(accessible_page_ids))

    if page_id:
        query = query.filter(PostPageAssociation.page_id == page_id)

    query = query.outerjoin(
        PostAnalytics,
        PostAnalytics.post_page_association_id == PostPageAssociation.id
    ).filter(
        db.or_(
            PostAnalytics.last_updated.is_(None),
            PostAnalytics.last_updated < cooldown_cutoff
        )
    )

    return query

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
        response = requests.get(url, params=params, timeout=5)
        
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
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            shares_data = data.get('shares', {})
            # shares can be {'count': 1} or 0 if no shares
            if isinstance(shares_data, dict):
                shares_count = shares_data.get('count', 0)
            else:
                shares_count = shares_data if isinstance(shares_data, int) else 0
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
    def _task():
        try:
            # Check if this is an Instagram post (different ID format)
            # Instagram post IDs are typically longer and contain underscores
            # Facebook post IDs are like: 123456789_987654321
            # Instagram post IDs are like: 17841400123456789 or 310978498765248_122242676810318154
            
            # First, try to determine if this post still exists
            check_url = f"https://graph.facebook.com/v18.0/{post_id}"
            check_params = {
                'fields': 'id',
                'access_token': page_access_token
            }
            
            print(f"[INSIGHTS] Checking if post {post_id} exists...")
            check_response = requests.get(check_url, params=check_params, timeout=8)
            
            if check_response.status_code != 200:
                error_data = check_response.json() if check_response.text else {}
                error_code = error_data.get('error', {}).get('code', 0)
                error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                
                # Error 10 = Permission/feature access error
                if error_code == 10:
                    print(f"[INSIGHTS] Permission error for post {post_id} (Error 10)")
                    print(f"[INSIGHTS] The page needs 'pages_read_engagement' permission or 'Page Public Content Access' feature")
                    print(f"[INSIGHTS] This usually means: 1) Page needs to be reconnected, 2) App lacks required permissions, 3) Page access token expired")
                    logger.warning(f"Permission error for post {post_id} (Error 10) - page may need reconnection")
                    return False
                # Error 100 often means post doesn't exist or no access
                elif error_code == 100:
                    print(f"[INSIGHTS] Post {post_id} not found or no access (Error 100)")
                    print(f"[INSIGHTS] This usually means: 1) Post was deleted, 2) Token lacks permissions, or 3) Wrong post ID format")
                    logger.warning(f"Post {post_id} not accessible (Error 100): {error_msg}")
                    return False
                elif error_code == 190:
                    print(f"[INSIGHTS] Access token is invalid or expired (Error 190)")
                    logger.error(f"Invalid access token for post {post_id}: {error_msg}")
                    return False
                else:
                    print(f"[INSIGHTS] Error {error_code}: {error_msg}")
                    logger.error(f"Error checking post {post_id} (code {error_code}): {error_msg}")
                    return False
            
            print(f"[INSIGHTS] Post {post_id} exists, fetching analytics...")
            
            # Build the API endpoint for Facebook post insights
            # Facebook post IDs are in format: page_id_post_id
            url = f"https://graph.facebook.com/v18.0/{post_id}/insights"
            
            # Metrics to fetch from Facebook Insights API
            # These metrics require 'read_insights' permission
            # Using only metrics confirmed to work with Facebook API v18.0+
            # Note: Many metrics were deprecated - these are the currently available ones
            core_metrics = [
                'post_impressions_unique',       # Reach (unique impressions) - WORKS
                'post_clicks',                   # Link clicks on the post - WORKS
                'post_reactions_by_type_total',  # Reaction counts (likes, love, haha, etc.) - WORKS
                'post_video_views'               # Video views (0 for non-video posts) - WORKS
            ]
            
            print(f"[INSIGHTS] Fetching analytics for post {post_id}")
            print(f"[INSIGHTS] Endpoint: {url}")
            print(f"[INSIGHTS] Core Metrics: {core_metrics}")
            
            params = {
                'metric': ','.join(core_metrics),
                'access_token': page_access_token
            }
            
            response = requests.get(url, params=params, timeout=8)
            
            # Fetch insights response
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
                        elif error_code == 100:  # Invalid metric or post not found
                            logger.warning(f"[INSIGHTS] Post {post_id} insights not available (Error 100): {error_msg}")
                            print(f"[INSIGHTS] ERROR 100: Post insights not available")
                            print(f"[INSIGHTS] This can happen if: 1) Post is too new, 2) Insights not enabled, 3) Post deleted")
                            
                            # Try with just a basic metric to check if insights work at all
                            print(f"[INSIGHTS] Trying fallback with basic metrics...")
                            for test_metric in ['post_impressions', 'post_impressions_unique']:
                                params_fallback = {
                                    'metric': test_metric,
                                    'access_token': page_access_token
                                }
                                response = requests.get(url, params=params_fallback, timeout=15)
                                if response.status_code == 200:
                                    print(f"[INSIGHTS] Success with {test_metric}")
                                    break
                                else:
                                    print(f"[INSIGHTS] {test_metric} also failed")
                            
                            if response.status_code != 200:
                                print(f"[INSIGHTS] All fallback attempts failed - skipping this post")
                                return False
                        else:
                            logger.error(f"[INSIGHTS] Facebook API error ({error_code}): {error_msg}")
                            print(f"[INSIGHTS] ERROR {error_code}: {error_msg}")
                except:
                    logger.error(f"[INSIGHTS] Failed to get analytics ({response.status_code}): {error_text}")
                    print(f"[INSIGHTS] FAILED ({response.status_code}): {error_text}")
                if response.status_code != 200:
                    return False
            
            response.raise_for_status()
            data = response.json()
            print(f"[INSIGHTS] Response: {data}")
            
            # Also fetch comments and shares using separate endpoints
            print(f"[INSIGHTS] Fetching comments count for post {post_id}")
            comments_count = fetch_facebook_comments_count(page_access_token, post_id)
            print(f"[INSIGHTS] Got {comments_count} comments")
            
            print(f"[INSIGHTS] Fetching shares count for post {post_id}")
            shares_count = fetch_facebook_shares_count(page_access_token, post_id)
            print(f"[INSIGHTS] Got {shares_count} shares")
            
            # Parse the response
            analytics_data = {
                'impressions': 0,  # Not available anymore, using reach as primary metric
                'reach': 0,
                'clicks': 0,
                'likes': 0,
                'comments': comments_count,  # From separate endpoint
                'shares': shares_count,       # From separate endpoint
                'engagement': 0,
                'engaged_users': 0,           # Not available anymore
                'video_views': 0              # From insights
            }
            
            if 'data' in data:
                for metric in data['data']:
                    metric_name = metric.get('name', '')
                    metric_values = metric.get('values', [])
                    metric_value = metric_values[0].get('value', 0) if metric_values else 0
                    
                    print(f"[INSIGHTS]   {metric_name} = {metric_value}")
                    
                    if metric_name == 'post_impressions_unique':
                        analytics_data['reach'] = metric_value
                        analytics_data['impressions'] = metric_value  # Use reach as impressions since total impressions not available
                    elif metric_name == 'post_clicks':
                        analytics_data['clicks'] = metric_value
                    elif metric_name == 'post_reactions_by_type_total':
                        # Total reactions = sum of all reaction types (like, love, haha, wow, sad, angry)
                        # The value can be a dict like {'like': 1, 'love': 0} or just an integer
                        if isinstance(metric_value, dict):
                            analytics_data['likes'] = sum(metric_value.values())
                        else:
                            analytics_data['likes'] = metric_value
                    elif metric_name == 'post_video_views':
                        analytics_data['video_views'] = metric_value
            
            print(f"[INSIGHTS] Parsed analytics: {analytics_data}")
            
            # Calculate engagement rate
            # Ensure all values are integers for calculation
            shares_val = analytics_data['shares']
            if isinstance(shares_val, dict):
                shares_val = shares_val.get('count', 0)
            
            total_engagements = (
                analytics_data['likes'] + 
                analytics_data['comments'] + 
                shares_val + 
                analytics_data['clicks']
            )
            if analytics_data['reach'] > 0:
                analytics_data['engagement'] = (total_engagements / analytics_data['reach']) * 100
            
            # Update or create PostAnalytics record
            analytics = PostAnalytics.query.filter_by(
                post_page_association_id=post_page_assoc_id
            ).first()
            
            if analytics:
                # Ensure shares is integer
                shares_val = analytics_data['shares']
                if isinstance(shares_val, dict):
                    shares_val = shares_val.get('count', 0)
                
                analytics.impressions = analytics_data['impressions']
                analytics.reach = analytics_data['reach']
                analytics.clicks = analytics_data['clicks']
                analytics.likes = analytics_data['likes']
                analytics.comments = analytics_data['comments']
                analytics.shares = shares_val
                analytics.engagement = analytics_data['engagement']
                analytics.video_views = analytics_data['video_views']
                analytics.last_updated = datetime.utcnow()
                print(f"[INSIGHTS] Updated existing analytics record")
            else:
                # Ensure shares is integer
                shares_val = analytics_data['shares']
                if isinstance(shares_val, dict):
                    shares_val = shares_val.get('count', 0)
                
                analytics = PostAnalytics(
                    post_page_association_id=post_page_assoc_id,
                    impressions=analytics_data['impressions'],
                    reach=analytics_data['reach'],
                    clicks=analytics_data['clicks'],
                    likes=analytics_data['likes'],
                    comments=analytics_data['comments'],
                    shares=shares_val,
                    engagement=analytics_data['engagement'],
                    video_views=analytics_data['video_views']
                )
                db.session.add(analytics)
                print(f"[INSIGHTS] Created new analytics record")
            
            db.session.commit()
            # Use shares_val (integer) for logging instead of analytics_data['shares'] (could be dict)
            logger.info(f"[INSIGHTS] Successfully updated analytics for post {post_id}: impressions={analytics_data['impressions']}, likes={analytics_data['likes']}, comments={analytics_data['comments']}, shares={shares_val}")
            print(f"[INSIGHTS] SUCCESS: Saved analytics to database")
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

    return _run_with_correct_app_context(_task)


def fetch_tiktok_post_analytics(open_id, access_token, post_id, post_page_assoc_id):
    """Fetch analytics for a TikTok video and persist to PostAnalytics."""

    def _task():
        try:
            stats = fetch_tiktok_post_stats(open_id, post_id, access_token)
            if not stats:
                logger.warning(f"[TIKTOK] No analytics returned for video {post_id}")
                return False

            analytics = PostAnalytics.query.filter_by(post_page_association_id=post_page_assoc_id).first()
            if not analytics:
                analytics = PostAnalytics(
                    post_page_association_id=post_page_assoc_id,
                    impressions=stats.get('views', 0),
                    reach=stats.get('views', 0),
                    clicks=0,
                    likes=stats.get('likes', 0),
                    comments=stats.get('comments', 0),
                    shares=stats.get('shares', 0),
                    saves=stats.get('favorites', 0)
                )
                db.session.add(analytics)
            else:
                analytics.impressions = stats.get('views', 0)
                analytics.reach = stats.get('views', 0)
                analytics.likes = stats.get('likes', 0)
                analytics.comments = stats.get('comments', 0)
                analytics.shares = stats.get('shares', 0)
                analytics.saves = stats.get('favorites', 0)
                analytics.last_updated = datetime.utcnow()

            db.session.commit()
            logger.info(f"[TIKTOK] Updated analytics for video {post_id}")
            return True
        except TikTokApiError as exc:
            logger.error(f"[TIKTOK] API error for video {post_id}: {exc}")
            return False
        except Exception as exc:
            logger.error(f"[TIKTOK] Unexpected analytics error for {post_id}: {exc}")
            return False

    return _run_with_correct_app_context(_task)


def refresh_all_post_analytics(user_id=None, limit=5, page_id=None):
    """
    Fetch analytics for published posts from connected pages
    
    Args:
        user_id: If provided, only refresh posts from pages accessible to this user
        limit: Maximum number of posts to process per batch (default 5 to avoid worker timeout)
        page_id: Optional specific ConnectedPage ID to limit refresh scope
              Each post takes ~10-15s with 3 API calls, so 5 posts = ~50-75s max
    """
    def _task():
        try:
            print("\n[REFRESH] ========== Starting analytics refresh ==========")
            logger.info(f"Starting refresh_all_post_analytics task (user_id={user_id}, limit={limit})")
            
            # Build query for published posts with their page associations
            query = _build_posts_needing_refresh_query(user_id, page_id).order_by(
                PostAnalytics.last_updated.asc().nullsfirst(),
                PostPageAssociation.id.desc()
            )
            
            if limit is not None:
                associations = query.limit(limit).all()
            else:
                associations = query.all()
            
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
                
                platform_name = page.platform.lower()
                # Only fetch for Facebook/Instagram/TikTok
                if platform_name not in ['facebook', 'instagram', 'tiktok']:
                    print(f"[REFRESH] Skipping association {assoc.id}: platform {page.platform} not supported")
                    skipped_count += 1
                    continue
                
                # Check if page has access token
                if not page.page_access_token:
                    print(f"[REFRESH] Skipping association {assoc.id}: no page access token")
                    logger.warning(f"Skipping association {assoc.id}: no page access token")
                    failed_count += 1
                    continue
                
                print(f"[REFRESH] Fetching analytics for post {assoc.platform_post_id} on page {page.page_name} (platform: {platform_name})")
                
                # Try to fetch analytics
                try:
                    if platform_name == 'tiktok':
                        success = fetch_tiktok_post_analytics(
                            page.platform_page_id,
                            page.page_access_token,
                            assoc.platform_post_id,
                            assoc.id
                        )
                    else:
                        # For Facebook and Instagram posts
                        success = fetch_facebook_post_analytics(
                            page.page_access_token,
                            assoc.platform_post_id,
                            assoc.id
                        )

                    if success:
                        success_count += 1
                        print(f"[REFRESH] SUCCESS: Fetched analytics for {assoc.platform_post_id}")
                    else:
                        failed_count += 1
                        print(f"[REFRESH] FAILED: Could not fetch analytics for {assoc.platform_post_id}")
                        logger.warning(f"Failed to fetch analytics for post {assoc.platform_post_id} (association {assoc.id})")
                except Exception as e:
                    failed_count += 1
                    print(f"[REFRESH] EXCEPTION: Error fetching analytics for {assoc.platform_post_id}: {e}")
                    logger.error(f"Exception fetching analytics for post {assoc.platform_post_id}: {e}")
                    continue
            
            print(f"[REFRESH] ========== Analytics refresh complete ==========")
            print(f"[REFRESH] Processed {len(associations)} associations: Success: {success_count}, Failed: {failed_count}, Skipped: {skipped_count}")
            logger.info(f"refresh_all_post_analytics completed: {success_count} success, {failed_count} failed, {skipped_count} skipped (limit={limit})")
            return {'success': success_count, 'failed': failed_count, 'skipped': skipped_count, 'processed': len(associations), 'limit': limit}
            
        except Exception as e:
            logger.error(f"Error in refresh_all_post_analytics: {e}")
            print(f"[REFRESH] ========== ERROR in analytics refresh ==========")
            print(f"[REFRESH] Error: {e}")
            import traceback
            traceback.print_exc()
            return {'success': 0, 'failed': 0, 'skipped': 0, 'error': str(e)}

    return _run_with_correct_app_context(_task)


def count_posts_needing_refresh(user_id, page_id=None):
    """
    Count how many posts need analytics refresh for a user
    
    Args:
        user_id: User ID to count posts for
        page_id: Optional ConnectedPage ID to scope the query
    
    Returns:
        int: Number of posts that need refresh
    """
    def _task():
        try:
            query = _build_posts_needing_refresh_query(user_id, page_id)
            return query.count()
        except Exception as e:
            logger.error(f"Error counting posts needing refresh: {e}")
            return 0
    
    return _run_with_correct_app_context(_task)


def check_and_publish_scheduled_posts():
    """
    Check for scheduled posts that need to be published and publish them
    This runs every minute to check if any posts have reached their scheduled time
    """
    def _task():
        try:
            from datetime import datetime
            now = datetime.utcnow()
            stale_cutoff = now - timedelta(minutes=AUTO_PUBLISH_LOCK_TIMEOUT_MINUTES)
            
            # Find due posts, including any stuck in publishing state past the timeout
            scheduled_posts = Post.query.filter(
                db.or_(
                    db.and_(Post.status == 'scheduled', Post.scheduled_time <= now),
                    db.and_(
                        Post.status == 'publishing',
                        Post.scheduled_time <= now,
                        Post.updated_at <= stale_cutoff
                    )
                )
            ).order_by(Post.scheduled_time.asc()).all()
            
            if not scheduled_posts:
                return
            
            logger.info(f"Found {len(scheduled_posts)} scheduled posts ready to publish")
            post_ids = [post.id for post in scheduled_posts]
            
            for post_id in post_ids:
                try:
                    # Attempt to lock this post by moving it to 'publishing' state
                    rows_updated = Post.query.filter(
                        Post.id == post_id,
                        db.or_(
                            Post.status == 'scheduled',
                            db.and_(Post.status == 'publishing', Post.updated_at <= stale_cutoff)
                        )
                    ).update(
                        {
                            'status': 'publishing',
                            'updated_at': datetime.utcnow()
                        },
                        synchronize_session=False
                    )
                    
                    if not rows_updated:
                        db.session.rollback()
                        continue  # Another worker already claimed it
                    
                    db.session.commit()  # Persist the lock
                    post = Post.query.get(post_id)
                    if not post:
                        continue
                    
                    print(f"[SCHEDULER] Auto-publishing post {post.id} (scheduled for {post.scheduled_time})")
                    
                    # Get associated pages
                    associations = PostPageAssociation.query.filter_by(post_id=post.id).all()
                    
                    if not associations:
                        print(f"[SCHEDULER] No pages associated with post {post.id}")
                        post.status = 'failed'
                        db.session.commit()
                        continue
                    
                    published_count = 0
                    errors = []
                    
                    for assoc in associations:
                        if assoc.status == 'sent':
                            continue  # Already published
                        
                        page = ConnectedPage.query.get(assoc.page_id)
                        if not page:
                            errors.append(f"Page {assoc.page_id} not found")
                            continue
                        
                        platform = page.platform.lower()
                        platform_post_id = None
                        
                        try:
                            from app import publish_to_facebook, publish_to_tiktok
                            
                            if platform in ['facebook', 'instagram']:
                                platform_post_id = publish_to_facebook(page.platform_page_id, post, page.page_access_token)
                            elif platform == 'tiktok':
                                platform_post_id = publish_to_tiktok(page.platform_page_id, post, page.page_access_token)
                            
                            if platform_post_id:
                                assoc.platform_post_id = platform_post_id
                                assoc.status = 'sent'
                                published_count += 1
                                print(f"[SCHEDULER] Published to {page.page_name}: {platform_post_id}")
                            else:
                                errors.append(f"Failed to publish to {page.page_name}")
                        except Exception as e:
                            errors.append(f"Error publishing to {page.page_name}: {str(e)}")
                            print(f"[SCHEDULER] Error: {e}")
                    
                    # Update post status based on publish results
                    if published_count > 0:
                        post.status = 'sent'
                        post.sent_time = datetime.utcnow()
                        post.scheduled_time = None
                        db.session.commit()
                        logger.info(f"Post {post.id} auto-published to {published_count} page(s)")
                    else:
                        post.status = 'failed'
                        db.session.commit()
                        logger.error(f"Post {post.id} failed to publish: {' | '.join(errors)}")
                        
                except Exception as e:
                    logger.error(f"Error processing scheduled post {post_id}: {e}")
                    db.session.rollback()
                    try:
                        Post.query.filter_by(id=post_id).update({'status': 'scheduled'})
                        db.session.commit()
                    except Exception as reset_error:
                        db.session.rollback()
                        logger.error(f"Failed to reset status for post {post_id}: {reset_error}")
                    
        except Exception as e:
            logger.error(f"Error in check_and_publish_scheduled_posts: {e}")
    
    return _run_with_correct_app_context(_task)


def daily_analytics_refresh():
    """
    Background job to refresh analytics for all users' posts daily
    This runs once per day and processes all posts across all users
    """
    def _task():
        try:
            logger.info("Starting daily analytics refresh for all users")
            
            from app import User
            
            # Get all active users
            users = User.query.all()
            
            total_refreshed = 0
            total_failed = 0
            
            for user in users:
                try:
                    # Refresh posts for this user (no limit for background job)
                    result = refresh_all_post_analytics(user.id, limit=None)
                    
                    if result:
                        total_refreshed += result.get('success', 0)
                        total_failed += result.get('failed', 0)
                        logger.info(f"User {user.id}: Refreshed {result.get('success', 0)} posts")
                except Exception as e:
                    logger.error(f"Error refreshing analytics for user {user.id}: {e}")
                    continue
            
            logger.info(f"Daily analytics refresh complete: {total_refreshed} posts refreshed, {total_failed} failed")
            return {'success': total_refreshed, 'failed': total_failed}
            
        except Exception as e:
            logger.error(f"Error in daily_analytics_refresh: {e}")
            return {'success': 0, 'failed': 0, 'error': str(e)}
    
    return _run_with_correct_app_context(_task)


def setup_scheduler():
    """Configure and start the APScheduler instance used by the worker."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

        def _scheduler_listener(event):
            if event.code == EVENT_JOB_MISSED:
                logger.warning("Scheduler missed job %s", getattr(event, 'job_id', 'unknown'))
            elif event.code == EVENT_JOB_ERROR:
                logger.error(
                    "Scheduler job %s raised an exception: %s",
                    getattr(event, 'job_id', 'unknown'),
                    event.exception,
                )

        scheduler = BackgroundScheduler(
            job_defaults={'max_instances': 1, 'coalesce': True, 'misfire_grace_time': 120},
            timezone="UTC",
        )

        scheduler.add_listener(_scheduler_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

        scheduler.add_job(
            func=daily_analytics_refresh,
            trigger=CronTrigger(hour=2, minute=0),
            id='daily_analytics_refresh',
            name='Daily analytics refresh for all users',
            replace_existing=True,
        )

        scheduler.add_job(
            func=check_and_publish_scheduled_posts,
            trigger=IntervalTrigger(minutes=1),
            id='check_scheduled_posts',
            name='Check and publish scheduled posts',
            replace_existing=True,
            max_instances=1,
        )

        scheduler.start()
        logger.info(
            "Scheduler started with %d job(s); analytics @02:00 UTC, scheduled post scan every minute",
            len(scheduler.get_jobs()),
        )
        return scheduler

    except ImportError:
        logger.warning("APScheduler not installed. Automatic tasks will not run.")
    except Exception as e:
        logger.error(f"Error setting up scheduler: {e}")
    return None
