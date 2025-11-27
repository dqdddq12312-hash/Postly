"""
Quick Backfill Script for DailyAnalyticsSummary
===============================================
This script is a wrapper to quickly backfill historical analytics data
after initial migration. Run this once after creating the table.

Usage:
    python scripts/backfill_analytics_90_days.py
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("\n" + "="*60)
print("QUICK BACKFILL: Last 90 Days Analytics")
print("="*60)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nThis will populate DailyAnalyticsSummary with the last 90 days")
print("of analytics data for faster Analyze page loading.")
print("\nEstimated time: 2-5 minutes depending on data volume")
print("="*60)

confirm = input("\nContinue? (yes/no): ")
if confirm.lower() != 'yes':
    print("Backfill cancelled.")
    sys.exit(0)

# Import and run the populate script with backfill argument
sys.argv = ['populate_daily_summaries.py', '--backfill', '90', '--force']

from populate_daily_summaries import main

main()

print("\n" + "="*60)
print("BACKFILL COMPLETE ✅")
print("="*60)
print("\nYour Analyze page should now load much faster!")
print("Summary metrics will be cached and load in < 100ms")
print("\nNext steps:")
print("1. Test the Analyze page: python app.py")
print("2. Visit /dashboard/analyze to see the improvements")
print("3. Set up daily cron job to keep data fresh")
print("="*60 + "\n")
