"""
Database Migration: Create DailyAnalyticsSummary Table
======================================================
This migration creates the daily_analytics_summary table for Buffer-style
pre-calculated analytics caching.

Run this migration:
    python migrations/create_daily_analytics_summary.py

To rollback:
    python migrations/create_daily_analytics_summary.py --rollback
"""

import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, app


def upgrade():
    """Create daily_analytics_summary table with indexes"""
    print("=" * 60)
    print("MIGRATION: Creating daily_analytics_summary table")
    print("=" * 60)
    
    with app.app_context():
        # Check if table already exists
        inspector = db.inspect(db.engine)
        if 'daily_analytics_summary' in inspector.get_table_names():
            print("⚠️  Table 'daily_analytics_summary' already exists. Skipping creation.")
            return
        
        # Detect database type
        db_type = db.engine.dialect.name
        print(f"Database type: {db_type}")
        
        # Create table using raw SQL for precise control
        if db_type == 'postgresql':
            # PostgreSQL syntax
            db.engine.execute("""
                CREATE TABLE daily_analytics_summary (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    page_id INTEGER NULL,
                    date DATE NOT NULL,
                    total_posts INTEGER DEFAULT 0,
                    total_impressions INTEGER DEFAULT 0,
                    total_reach INTEGER DEFAULT 0,
                    total_clicks INTEGER DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_comments INTEGER DEFAULT 0,
                    total_shares INTEGER DEFAULT 0,
                    total_video_views INTEGER DEFAULT 0,
                    avg_engagement_rate REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE,
                    FOREIGN KEY (page_id) REFERENCES connected_page (id) ON DELETE CASCADE,
                    UNIQUE (user_id, page_id, date)
                )
            """)
        else:
            # SQLite syntax
            db.engine.execute("""
                CREATE TABLE daily_analytics_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    page_id INTEGER NULL,
                    date DATE NOT NULL,
                    total_posts INTEGER DEFAULT 0,
                    total_impressions INTEGER DEFAULT 0,
                    total_reach INTEGER DEFAULT 0,
                    total_clicks INTEGER DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_comments INTEGER DEFAULT 0,
                    total_shares INTEGER DEFAULT 0,
                    total_video_views INTEGER DEFAULT 0,
                    avg_engagement_rate REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE,
                    FOREIGN KEY (page_id) REFERENCES connected_page (id) ON DELETE CASCADE,
                    UNIQUE (user_id, page_id, date)
                )
            """)
        print("✅ Created table: daily_analytics_summary")
        
        # Create indexes for fast queries
        db.engine.execute("""
            CREATE INDEX idx_user_date 
            ON daily_analytics_summary (user_id, date)
        """)
        print("✅ Created index: idx_user_date")
        
        db.engine.execute("""
            CREATE INDEX idx_page_date 
            ON daily_analytics_summary (page_id, date)
        """)
        print("✅ Created index: idx_page_date")
        
        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE ✅")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Run: python scripts/populate_daily_summaries.py --backfill 90")
        print("2. Set up cron job to run daily at midnight:")
        print("   0 0 * * * cd /path/to/postly && python scripts/populate_daily_summaries.py")
        print("=" * 60)


def downgrade():
    """Drop daily_analytics_summary table and indexes"""
    print("=" * 60)
    print("ROLLBACK: Dropping daily_analytics_summary table")
    print("=" * 60)
    
    with app.app_context():
        inspector = db.inspect(db.engine)
        if 'daily_analytics_summary' not in inspector.get_table_names():
            print("⚠️  Table 'daily_analytics_summary' does not exist. Nothing to rollback.")
            return
        
        # Drop indexes first
        try:
            db.engine.execute("DROP INDEX IF EXISTS idx_user_date")
            print("✅ Dropped index: idx_user_date")
        except Exception as e:
            print(f"⚠️  Could not drop idx_user_date: {e}")
        
        try:
            db.engine.execute("DROP INDEX IF EXISTS idx_page_date")
            print("✅ Dropped index: idx_page_date")
        except Exception as e:
            print(f"⚠️  Could not drop idx_page_date: {e}")
        
        # Drop table
        db.engine.execute("DROP TABLE daily_analytics_summary")
        print("✅ Dropped table: daily_analytics_summary")
        
        print("\n" + "=" * 60)
        print("ROLLBACK COMPLETE ✅")
        print("=" * 60)


if __name__ == '__main__':
    if '--rollback' in sys.argv or '--downgrade' in sys.argv:
        confirm = input("⚠️  Are you sure you want to rollback? This will delete all cached analytics data. (yes/no): ")
        if confirm.lower() == 'yes':
            downgrade()
        else:
            print("Rollback cancelled.")
    else:
        upgrade()
