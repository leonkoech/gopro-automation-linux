#!/usr/bin/env python3
"""
Fix Mar 28 game split: The game UiIK8Nl0ZKoK4TR6Xt8f contains two games.

Game 1: Rim Job (73) vs ? (71) — 01:44 to ~02:58 UTC
Game 2: Hialeah Mentality (51) vs Locksmith (72) — ~03:07 to 03:59 UTC

This script:
1. Updates the existing game to be "Rim Job vs ?" with correct end time and scores
2. Creates a new game for "Hialeah Mentality vs Locksmith"

Usage:
  python scripts/fix_mar28_game_split.py --dry-run
  python scripts/fix_mar28_game_split.py
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uball-gopro-fleet-firebase-adminsdk.json'),
)

GAME_ID = 'UiIK8Nl0ZKoK4TR6Xt8f'

# Log split: logs[0:185] = Game 1, logs[185:189] = between-game noise, logs[189:] = Game 2
GAME1_LOG_END = 185      # exclusive: last Game 1 log is [184]
GAME2_LOG_START = 189     # inclusive: first Game 2 log (timer_started)


def init_firebase() -> firestore.Client:
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials not found: {FIREBASE_CREDENTIALS_PATH}")
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def main():
    parser = argparse.ArgumentParser(description='Fix Mar 28 game split')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without applying')
    args = parser.parse_args()

    db = init_firebase()
    games_ref = db.collection('basketball-games')

    # Read existing game
    doc = games_ref.document(GAME_ID).get()
    if not doc.exists:
        print(f"ERROR: Game {GAME_ID} not found")
        sys.exit(1)

    data = doc.to_dict()
    logs = data.get('logs', [])
    print(f"Found game {GAME_ID} with {len(logs)} logs")
    print(f"  Current: {data['leftTeam']['name']} vs {data['rightTeam']['name']}")
    print(f"  Time: {data['createdAt']} → {data['endedAt']}")
    print(f"  Scores: {data['leftTeam'].get('finalScore')} - {data['rightTeam'].get('finalScore')}")
    print()

    # --- Game 1: Rim Job (73) vs ? (71) ---
    game1_logs = logs[:GAME1_LOG_END]
    # Add a game_ended event for Game 1
    game1_logs.append({
        'gameTimeFormatted': '0:00',
        'team': None,
        'period': '1st',
        'actionType': 'game_ended',
        'shotClock': 0,
        'loggedBy': 'system_fix',
        'gameTime': 0,
        'timestamp': '2026-03-28T02:58:38.801Z',  # time of first score reset
        'payload': {},
        'teamName': ''
    })

    game1_update = {
        'leftTeam': {
            'name': 'Rim Job',
            'displayName': 'RIM JOB',
            'jerseyColor': '#3b82f6',       # Blue
            'jerseyColorName': 'Blue',
            'finalScore': 73,
            'finalFouls': data['leftTeam'].get('finalFouls', 0),
        },
        'rightTeam': {
            'name': '?',
            'displayName': '?',
            'jerseyColor': '#ef4444',       # Red (placeholder)
            'jerseyColorName': 'Red',
            'finalScore': 71,
            'finalFouls': data['rightTeam'].get('finalFouls', 0),
        },
        'endedAt': '2026-03-28T02:58:38.801Z',
        'logs': game1_logs,
        # Clear uball sync so it re-syncs with correct data
        'uballGameId': firestore.DELETE_FIELD,
        'syncedAt': firestore.DELETE_FIELD,
    }

    print(f"=== Game 1 Update (existing doc {GAME_ID}) ===")
    print(f"  Teams: Rim Job (73) vs ? (71)")
    print(f"  Start: {data['createdAt']} (unchanged)")
    print(f"  End: 2026-03-28T02:58:38.801Z (was {data['endedAt']})")
    print(f"  Logs: {len(game1_logs)} (was {len(logs)})")
    print()

    # --- Game 2: Hialeah Mentality (51) vs Locksmith (72) ---
    game2_logs = logs[GAME2_LOG_START:]
    # Prepend a game_started event for Game 2
    game2_logs.insert(0, {
        'gameTimeFormatted': '10:00',
        'team': None,
        'period': '1st',
        'actionType': 'game_started',
        'shotClock': 24000,
        'loggedBy': 'system_fix',
        'gameTime': 600000,
        'timestamp': '2026-03-28T03:03:56.009Z',  # when timer was restarted
        'payload': {},
        'teamName': ''
    })

    game2_data = {
        'createdAt': '2026-03-28T03:03:56.009Z',
        'endedAt': '2026-03-28T03:59:44.853Z',
        'createdBy': data.get('createdBy', ''),
        'finalPeriod': '1st',
        'leftTeam': {
            'name': 'Hialeah Mentality',
            'displayName': 'HIALEAH MENTALITY',
            'jerseyColor': '#9ca3af',       # Grey
            'jerseyColorName': 'Grey',
            'finalScore': 51,
            'finalFouls': 0,
        },
        'rightTeam': {
            'name': 'Locksmith',
            'displayName': 'LOCKSMITH',
            'jerseyColor': '#22c55e',       # Green
            'jerseyColorName': 'Green',
            'finalScore': 72,
            'finalFouls': 0,
        },
        'logs': game2_logs,
    }

    print(f"=== Game 2 (NEW document) ===")
    print(f"  Teams: Hialeah Mentality (51) vs Locksmith (72)")
    print(f"  Start: 2026-03-28T03:03:56.009Z")
    print(f"  End: 2026-03-28T03:59:44.853Z")
    print(f"  Logs: {len(game2_logs)}")
    print()

    if args.dry_run:
        print("[DRY RUN] No changes made.")
        return

    # Apply changes
    print("Applying changes...")

    # Update Game 1
    games_ref.document(GAME_ID).update(game1_update)
    print(f"  ✓ Updated game {GAME_ID} → Rim Job (73) vs ? (71)")

    # Create Game 2
    _, new_doc_ref = games_ref.add(game2_data)
    new_game_id = new_doc_ref.id
    print(f"  ✓ Created new game {new_game_id} → Hialeah Mentality (51) vs Locksmith (72)")

    print()
    print("Done! Firebase now has 4 games for Mar 28 night session.")
    print(f"New game ID: {new_game_id}")


if __name__ == '__main__':
    main()
