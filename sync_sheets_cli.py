"""
CLI tool to sync posts from Google Sheets to Postly database
Usage: python sync_sheets_cli.py <spreadsheet_id> <user_id> [sheet_name]
"""

import sys
from sheets_sync import sync_posts_from_sheets

def main():
    if len(sys.argv) < 3:
        print("Usage: python sync_sheets_cli.py <spreadsheet_id> <user_id> [sheet_name]")
        print("\nExample:")
        print("  python sync_sheets_cli.py 1abc123def456 1")
        print("  python sync_sheets_cli.py 1abc123def456 1 'Schedule 2024'")
        sys.exit(1)
    
    spreadsheet_id = sys.argv[1]
    try:
        user_id = int(sys.argv[2])
    except ValueError:
        print("Error: user_id must be an integer")
        sys.exit(1)
    
    sheet_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"\n{'='*60}")
    print("Postly - Google Sheets Sync")
    print(f"{'='*60}")
    print(f"Spreadsheet ID: {spreadsheet_id}")
    print(f"User ID: {user_id}")
    print(f"Sheet Name: {sheet_name or '(default/first sheet)'}")
    print(f"{'='*60}\n")
    
    try:
        success_count, error_count, errors = sync_posts_from_sheets(
            spreadsheet_id,
            user_id,
            sheet_name
        )
        
        print(f"\n{'='*60}")
        print("Sync Results")
        print(f"{'='*60}")
        print(f"✅ Successfully synced: {success_count} post(s)")
        print(f"❌ Errors: {error_count}")
        
        if errors:
            print(f"\nError Details:")
            for i, error in enumerate(errors, 1):
                print(f"  {i}. {error}")
        
        print(f"{'='*60}\n")
        
        if error_count > 0:
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
