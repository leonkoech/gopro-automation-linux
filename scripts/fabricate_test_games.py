#!/usr/bin/env python3
"""
Fabricate Test Games in Firebase for Video Integration Testing.

This script creates 3 test games in Firebase's basketball-games collection
with timestamps that align with the recorded video segments.

Segment Session: enxd43260ef4d38_20260120_195030
Recording Start: 2026-01-20 19:50:30 UTC
Total Duration: ~105 minutes (3 chapters)

Test Scenarios:
1. Game 1: Entirely within chapter 1 (simple extraction)
2. Game 2: Spans chapter 1 and 2 (tests concatenation)
3. Game 3: Entirely within chapter 2 (simple extraction)

Usage:
    python scripts/fabricate_test_games.py [--cleanup] [--dry-run]
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# ==================== Configuration ====================

# Firebase credentials
FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    '/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json'
)

# Recording session metadata
RECORDING_SESSION = {
    'session_name': 'enxd43260ef4d38_20260120_195030',
    'start_time': datetime(2026, 1, 20, 19, 50, 30, tzinfo=timezone.utc),
    'chapters': [
        {'name': 'chapter_001_GX018471.MP4', 'size_gb': 9.51, 'duration_minutes': 35},
        {'name': 'chapter_002_GX028471.MP4', 'size_gb': 9.25, 'duration_minutes': 35},
        {'name': 'chapter_003_GX038471.MP4', 'size_gb': 8.55, 'duration_minutes': 35},
    ]
}

# Test games configuration
# Each game has:
# - name: Descriptive name
# - left_team/right_team: Team names
# - start_offset: Minutes from recording start
# - duration: Game duration in minutes
# - scores: Final scores
# - scenario: What this tests
TEST_GAMES = [
    {
        'name': 'Test Game 1 - Within Chapter 1',
        'left_team': 'ALPHA WOLVES',
        'right_team': 'BETA HAWKS',
        'start_offset_minutes': 5,   # Starts at 19:55:30
        'duration_minutes': 20,       # Ends at 20:15:30
        'left_score': 45,
        'right_score': 42,
        'scenario': 'Simple extraction from single chapter',
    },
    {
        'name': 'Test Game 2 - Spans Chapter 1 & 2',
        'left_team': 'GAMMA TIGERS',
        'right_team': 'DELTA BEARS',
        'start_offset_minutes': 25,  # Starts at 20:15:30 (within chapter 1)
        'duration_minutes': 30,       # Ends at 20:45:30 (spans into chapter 2)
        'left_score': 68,
        'right_score': 71,
        'scenario': 'Tests FFmpeg concatenation across chapters',
    },
    {
        'name': 'Test Game 3 - Within Chapter 2',
        'left_team': 'EPSILON LIONS',
        'right_team': 'ZETA EAGLES',
        'start_offset_minutes': 65,  # Starts at 20:55:30
        'duration_minutes': 20,       # Ends at 21:15:30
        'left_score': 52,
        'right_score': 55,
        'scenario': 'Simple extraction from second chapter',
    },
]


# ==================== Firebase Functions ====================

def initialize_firebase() -> firestore.Client:
    """Initialize Firebase Admin SDK and return Firestore client."""
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Firebase credentials not found: {FIREBASE_CREDENTIALS_PATH}")

    # Check if already initialized
    try:
        app = firebase_admin.get_app()
    except ValueError:
        # Not initialized yet
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def to_js_iso_format(dt: datetime) -> str:
    """Convert datetime to JavaScript-compatible ISO format with Z suffix."""
    # Ensure UTC and format like: 2026-01-20T19:55:30.000Z
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def create_game_document(
    db: firestore.Client,
    game_config: Dict[str, Any],
    recording_start: datetime,
    user_email: str = 'test@uball.com',
    dry_run: bool = False
) -> Optional[str]:
    """
    Create a game document in Firebase.

    Returns the document ID if created, None if dry run.
    Schema matches exactly what frontend creates.
    """
    start_time = recording_start + timedelta(minutes=game_config['start_offset_minutes'])
    end_time = start_time + timedelta(minutes=game_config['duration_minutes'])

    # Use JS-compatible ISO format
    start_iso = to_js_iso_format(start_time)
    end_iso = to_js_iso_format(end_time)

    game_doc = {
        'createdAt': start_iso,
        'endedAt': end_iso,
        'createdBy': user_email,
        'leftTeam': {
            'name': game_config['left_team'],
            'finalScore': game_config['left_score'],
            'finalFouls': 3,
        },
        'rightTeam': {
            'name': game_config['right_team'],
            'finalScore': game_config['right_score'],
            'finalFouls': 2,
        },
        'finalPeriod': '4th',  # Match frontend format: "1st", "2nd", "3rd", "4th"
        'status': 'completed',
        'logs': [
            {
                'timestamp': start_iso,
                'loggedBy': user_email,
                'actionType': 'game_started',
                'team': None,
                'teamName': '',
                'payload': {},
                'gameTime': 600000,  # 10 minutes (frontend default)
                'gameTimeFormatted': '10:00',
                'shotClock': 24000,
                'period': '1st',
            },
            {
                'timestamp': end_iso,
                'loggedBy': user_email,
                'actionType': 'game_ended',
                'team': None,
                'teamName': '',
                'payload': {},
                'gameTime': 0,
                'gameTimeFormatted': '00:00',
                'shotClock': 0,
                'period': '4th',
            },
        ],
        # Custom field for testing (can be used for cleanup)
        '_test_metadata': {
            'scenario': game_config['scenario'],
            'fabricated': True,
            'recording_session': RECORDING_SESSION['session_name'],
        }
    }

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Creating game: {game_config['name']}")
    print(f"  Teams: {game_config['left_team']} vs {game_config['right_team']}")
    print(f"  Start: {start_time.isoformat()}")
    print(f"  End: {end_time.isoformat()}")
    print(f"  Duration: {game_config['duration_minutes']} minutes")
    print(f"  Scenario: {game_config['scenario']}")

    if dry_run:
        print("  [Skipped - dry run]")
        return None

    # Create document
    doc_ref = db.collection('basketball-games').document()
    doc_ref.set(game_doc)

    print(f"  Created: {doc_ref.id}")
    return doc_ref.id


def cleanup_test_games(db: firestore.Client, dry_run: bool = False) -> int:
    """
    Remove test games that were fabricated by this script.

    Returns count of deleted games.
    """
    print("\nLooking for fabricated test games to clean up...")

    # Query for games with our test marker
    games_ref = db.collection('basketball-games')

    # We need to check each document since Firestore doesn't support
    # nested field queries well
    deleted_count = 0

    for doc in games_ref.stream():
        data = doc.to_dict()
        metadata = data.get('_test_metadata', {})

        if metadata.get('fabricated') == True:
            print(f"  {'[DRY RUN] ' if dry_run else ''}Deleting: {doc.id}")
            print(f"    Teams: {data.get('leftTeam', {}).get('name')} vs {data.get('rightTeam', {}).get('name')}")

            if not dry_run:
                doc.reference.delete()

            deleted_count += 1

    print(f"\n{'Would delete' if dry_run else 'Deleted'} {deleted_count} test games")
    return deleted_count


def list_all_games(db: firestore.Client) -> List[Dict[str, Any]]:
    """List all games in the collection."""
    games = []
    for doc in db.collection('basketball-games').order_by('createdAt', direction=firestore.Query.DESCENDING).stream():
        data = doc.to_dict()
        data['id'] = doc.id
        games.append(data)
    return games


def unsync_game(db: firestore.Client, game_id: str, dry_run: bool = False) -> bool:
    """
    Remove uballGameId from a Firebase game to allow re-syncing.
    """
    doc_ref = db.collection('basketball-games').document(game_id)
    doc = doc_ref.get()

    if not doc.exists:
        print(f"Game {game_id} not found")
        return False

    data = doc.to_dict()
    if 'uballGameId' not in data:
        print(f"Game {game_id} is not synced (no uballGameId)")
        return False

    print(f"{'[DRY RUN] ' if dry_run else ''}Removing uballGameId from game {game_id}")

    if not dry_run:
        doc_ref.update({'uballGameId': firestore.DELETE_FIELD})

    return True


# ==================== Main ====================

def main():
    parser = argparse.ArgumentParser(description='Fabricate test games in Firebase')
    parser.add_argument('--cleanup', action='store_true', help='Remove fabricated test games')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--list', action='store_true', help='List all games')
    parser.add_argument('--unsync', metavar='GAME_ID', help='Remove uballGameId from a game')
    parser.add_argument('--unsync-all', action='store_true', help='Remove uballGameId from all test games')

    args = parser.parse_args()

    print("=" * 60)
    print("Firebase Test Games Fabrication Tool")
    print("=" * 60)
    print(f"\nRecording Session: {RECORDING_SESSION['session_name']}")
    print(f"Recording Start: {RECORDING_SESSION['start_time'].isoformat()}")
    print(f"Chapters: {len(RECORDING_SESSION['chapters'])}")

    # Calculate chapter boundaries
    current_time = RECORDING_SESSION['start_time']
    print("\nChapter Timeline:")
    for i, chapter in enumerate(RECORDING_SESSION['chapters'], 1):
        end_time = current_time + timedelta(minutes=chapter['duration_minutes'])
        print(f"  Chapter {i}: {current_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')} ({chapter['name']})")
        current_time = end_time

    # Initialize Firebase
    print(f"\nFirebase credentials: {FIREBASE_CREDENTIALS_PATH}")
    try:
        db = initialize_firebase()
        print("Firebase initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize Firebase: {e}")
        sys.exit(1)

    # Handle different modes
    if args.list:
        print("\n" + "=" * 60)
        print("All Games in Firebase")
        print("=" * 60)
        games = list_all_games(db)
        for game in games:
            metadata = game.get('_test_metadata', {})
            synced = 'uballGameId' in game
            test_marker = '[TEST]' if metadata.get('fabricated') else ''
            sync_marker = '[SYNCED]' if synced else ''
            print(f"\n{game['id']} {test_marker} {sync_marker}")
            print(f"  {game.get('leftTeam', {}).get('name')} vs {game.get('rightTeam', {}).get('name')}")
            print(f"  Created: {game.get('createdAt')}")
            print(f"  Ended: {game.get('endedAt')}")
            if synced:
                print(f"  Uball Game ID: {game.get('uballGameId')}")
        print(f"\nTotal: {len(games)} games")
        return

    if args.unsync:
        print("\n" + "=" * 60)
        print("Unsync Game")
        print("=" * 60)
        unsync_game(db, args.unsync, dry_run=args.dry_run)
        return

    if args.unsync_all:
        print("\n" + "=" * 60)
        print("Unsync All Test Games")
        print("=" * 60)
        games = list_all_games(db)
        count = 0
        for game in games:
            if game.get('_test_metadata', {}).get('fabricated'):
                if unsync_game(db, game['id'], dry_run=args.dry_run):
                    count += 1
        print(f"\n{'Would unsync' if args.dry_run else 'Unsynced'} {count} games")
        return

    if args.cleanup:
        print("\n" + "=" * 60)
        print("Cleanup Test Games")
        print("=" * 60)
        cleanup_test_games(db, dry_run=args.dry_run)
        return

    # Default: Create test games
    print("\n" + "=" * 60)
    print("Creating Test Games")
    print("=" * 60)

    created_ids = []
    for game_config in TEST_GAMES:
        game_id = create_game_document(
            db,
            game_config,
            RECORDING_SESSION['start_time'],
            dry_run=args.dry_run
        )
        if game_id:
            created_ids.append(game_id)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    if args.dry_run:
        print(f"Would create {len(TEST_GAMES)} test games")
    else:
        print(f"Created {len(created_ids)} test games")
        print("\nGame IDs:")
        for gid in created_ids:
            print(f"  {gid}")

    print("\nNext steps:")
    print("  1. Open the frontend at /game-logs to see the test games")
    print("  2. Use the 'Sync to Uball' button to sync each game")
    print("  3. Verify teams are created and game is registered in Uball")
    print("  4. Run video extraction for each game")


if __name__ == '__main__':
    main()
