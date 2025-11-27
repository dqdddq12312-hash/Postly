"""
Daily Analytics Summary Population Script
=========================================
This script aggregates PostAnalytics data into DailyAnalyticsSummary table
for Buffer-style cached analytics. Can be run as a cron job or manually.

Usage:
    # Update yesterday's data (default - for daily cron job)
    python scripts/populate_daily_summaries.py
    
    # Backfill historical data (e.g., last 90 days)
    python scripts/populate_daily_summaries.py --backfill 90
    
    # Update specific date range
    python scripts/populate_daily_summaries.py --start-date 2024-01-01 --end-date 2024-01-31
    
    # Force recalculation (overwrite existing data)
    python scripts/populate_daily_summaries.py --force

Cron job setup (daily at midnight):
    0 0 * * * cd /path/to/postly && /path/to/python scripts/populate_daily_summaries.py >> /var/log/postly_analytics.log 2>&1
"""

import sys
import os
import argparse
from datetime import datetime, timedelta, date
from sqlalchemy import func, and_

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, app, DailyAnalyticsSummary, Post, PostAnalytics, PostPageAssociation, User


def calculate_engagement_rate(likes, comments, shares, reach):
    """Calculate engagement rate: (likes + comments + shares) / reach * 100"""
    if reach == 0:
        return 0.0
    total_engagement = (likes or 0) + (comments or 0) + (shares or 0)
    return round((total_engagement / reach) * 100, 2)


def populate_summary_for_date(target_date, force=False):
    """
    Populate or update DailyAnalyticsSummary for a specific date.
    
    Args:
        target_date: date object for the day to process
        force: if True, overwrite existing data; if False, skip existing
    
    Returns:
        dict with statistics (processed, skipped, errors)
    """
    stats = {
        'processed': 0,
        'skipped': 0,
        'errors': 0,
        'user_summaries': 0,
        'page_summaries': 0
    }
    
    print(f"\n{'='*60}")
    print(f"Processing date: {target_date}")
    print(f"{'='*60}")
    
    # Get all users
    users = User.query.all()
    print(f"Found {len(users)} users")
    
    for user in users:
        try:
            # Get all posts sent on target_date for this user
            posts_query = db.session.query(Post).filter(
                and_(
                    Post.user_id == user.id,
                    func.date(Post.sent_time) == target_date,
                    Post.status == 'published'
                )
            ).all()
            
            if not posts_query:
                print(f"  User {user.email}: No posts on {target_date}")
                stats['skipped'] += 1
                continue
            
            print(f"  User {user.email}: {len(posts_query)} posts")
            
            # Aggregate user-level metrics (all pages combined)
            user_summary = aggregate_posts_metrics(posts_query)
            
            # Check if user summary already exists
            existing_user_summary = DailyAnalyticsSummary.query.filter_by(
                user_id=user.id,
                page_id=None,
                date=target_date
            ).first()
            
            if existing_user_summary and not force:
                print(f"    ⏭️  Skipping user summary (already exists)")
                stats['skipped'] += 1
            else:
                if existing_user_summary:
                    # Update existing
                    for key, value in user_summary.items():
                        setattr(existing_user_summary, key, value)
                    existing_user_summary.updated_at = datetime.utcnow()
                    print(f"    ✅ Updated user summary (force mode)")
                else:
                    # Create new
                    new_summary = DailyAnalyticsSummary(
                        user_id=user.id,
                        page_id=None,
                        date=target_date,
                        **user_summary
                    )
                    db.session.add(new_summary)
                    print(f"    ✅ Created user summary")
                
                stats['user_summaries'] += 1
                stats['processed'] += 1
            
            # Aggregate by page
            pages_data = {}
            for post in posts_query:
                # Get all page associations for this post
                associations = PostPageAssociation.query.filter_by(post_id=post.id).all()
                
                for assoc in associations:
                    page_id = assoc.page_id
                    if page_id not in pages_data:
                        pages_data[page_id] = []
                    
                    # Get analytics for this association
                    analytics = PostAnalytics.query.filter_by(
                        post_page_association_id=assoc.id
                    ).first()
                    
                    if analytics:
                        pages_data[page_id].append({
                            'reach': analytics.reach or 0,
                            'impressions': analytics.impressions or 0,
                            'clicks': analytics.clicks or 0,
                            'likes': analytics.likes or 0,
                            'comments': analytics.comments or 0,
                            'shares': analytics.shares or 0,
                            'video_views': analytics.video_views or 0
                        })
            
            # Create page-level summaries
            for page_id, page_posts_data in pages_data.items():
                page_summary = {
                    'total_posts': len(page_posts_data),
                    'total_reach': sum(p['reach'] for p in page_posts_data),
                    'total_impressions': sum(p['impressions'] for p in page_posts_data),
                    'total_clicks': sum(p['clicks'] for p in page_posts_data),
                    'total_likes': sum(p['likes'] for p in page_posts_data),
                    'total_comments': sum(p['comments'] for p in page_posts_data),
                    'total_shares': sum(p['shares'] for p in page_posts_data),
                    'total_video_views': sum(p['video_views'] for p in page_posts_data),
                }
                
                # Calculate avg engagement rate
                page_summary['avg_engagement_rate'] = calculate_engagement_rate(
                    page_summary['total_likes'],
                    page_summary['total_comments'],
                    page_summary['total_shares'],
                    page_summary['total_reach']
                )
                
                # Check if page summary exists
                existing_page_summary = DailyAnalyticsSummary.query.filter_by(
                    user_id=user.id,
                    page_id=page_id,
                    date=target_date
                ).first()
                
                if existing_page_summary and not force:
                    print(f"    ⏭️  Skipping page {page_id} summary (already exists)")
                    stats['skipped'] += 1
                else:
                    if existing_page_summary:
                        # Update existing
                        for key, value in page_summary.items():
                            setattr(existing_page_summary, key, value)
                        existing_page_summary.updated_at = datetime.utcnow()
                        print(f"    ✅ Updated page {page_id} summary (force mode)")
                    else:
                        # Create new
                        new_page_summary = DailyAnalyticsSummary(
                            user_id=user.id,
                            page_id=page_id,
                            date=target_date,
                            **page_summary
                        )
                        db.session.add(new_page_summary)
                        print(f"    ✅ Created page {page_id} summary")
                    
                    stats['page_summaries'] += 1
                    stats['processed'] += 1
            
            # Commit after each user to prevent losing all progress on error
            db.session.commit()
            
        except Exception as e:
            db.session.rollback()
            print(f"  ❌ Error processing user {user.email}: {e}")
            stats['errors'] += 1
    
    return stats


def aggregate_posts_metrics(posts):
    """
    Aggregate metrics from a list of posts (user-level aggregation).
    
    Args:
        posts: List of Post objects
    
    Returns:
        dict with aggregated metrics
    """
    total_reach = 0
    total_impressions = 0
    total_clicks = 0
    total_likes = 0
    total_comments = 0
    total_shares = 0
    total_video_views = 0
    
    for post in posts:
        # Get all analytics for this post across all pages
        associations = PostPageAssociation.query.filter_by(post_id=post.id).all()
        
        for assoc in associations:
            analytics = PostAnalytics.query.filter_by(
                post_page_association_id=assoc.id
            ).first()
            
            if analytics:
                total_reach += analytics.reach or 0
                total_impressions += analytics.impressions or 0
                total_clicks += analytics.clicks or 0
                total_likes += analytics.likes or 0
                total_comments += analytics.comments or 0
                total_shares += analytics.shares or 0
                total_video_views += analytics.video_views or 0
    
    avg_engagement_rate = calculate_engagement_rate(
        total_likes, total_comments, total_shares, total_reach
    )
    
    return {
        'total_posts': len(posts),
        'total_reach': total_reach,
        'total_impressions': total_impressions,
        'total_clicks': total_clicks,
        'total_likes': total_likes,
        'total_comments': total_comments,
        'total_shares': total_shares,
        'total_video_views': total_video_views,
        'avg_engagement_rate': avg_engagement_rate
    }


def main():
    parser = argparse.ArgumentParser(
        description='Populate DailyAnalyticsSummary table with aggregated analytics data'
    )
    parser.add_argument(
        '--backfill',
        type=int,
        metavar='DAYS',
        help='Backfill last N days of data (e.g., --backfill 90)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        metavar='YYYY-MM-DD',
        help='Start date for processing (e.g., 2024-01-01)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        metavar='YYYY-MM-DD',
        help='End date for processing (e.g., 2024-01-31)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force recalculation (overwrite existing data)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("DAILY ANALYTICS SUMMARY POPULATION")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    with app.app_context():
        # Determine date range to process
        if args.start_date and args.end_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            print(f"Mode: Custom date range ({start_date} to {end_date})")
        elif args.backfill:
            end_date = date.today() - timedelta(days=1)  # Yesterday
            start_date = end_date - timedelta(days=args.backfill - 1)
            print(f"Mode: Backfill last {args.backfill} days ({start_date} to {end_date})")
        else:
            # Default: process yesterday only (for daily cron job)
            target_date = date.today() - timedelta(days=1)
            start_date = target_date
            end_date = target_date
            print(f"Mode: Daily update (yesterday: {target_date})")
        
        if args.force:
            print("⚠️  Force mode: Will overwrite existing data")
        
        # Process each date in range
        current_date = start_date
        total_stats = {
            'processed': 0,
            'skipped': 0,
            'errors': 0,
            'user_summaries': 0,
            'page_summaries': 0
        }
        
        while current_date <= end_date:
            date_stats = populate_summary_for_date(current_date, force=args.force)
            
            # Aggregate stats
            for key in total_stats:
                total_stats[key] += date_stats[key]
            
            current_date += timedelta(days=1)
        
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"Total records processed: {total_stats['processed']}")
        print(f"  - User summaries: {total_stats['user_summaries']}")
        print(f"  - Page summaries: {total_stats['page_summaries']}")
        print(f"Total records skipped: {total_stats['skipped']}")
        print(f"Total errors: {total_stats['errors']}")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")
        
        if total_stats['errors'] > 0:
            sys.exit(1)  # Exit with error code for cron monitoring


if __name__ == '__main__':
    main()
