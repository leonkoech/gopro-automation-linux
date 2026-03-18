#!/usr/bin/env python3
"""
Inspect Firebase games and recording-sessions to verify timestamps are in UTC.

Pipeline matching requires:
- recording-sessions: startedAt, endedAt (ISO 8601 UTC, e.g. 2026-02-02T14:09:46.000Z)
- basketball-games: createdAt, endedAt (ISO 8601 UTC)

Run from repo root (so FIREBASE_CREDENTIALS_PATH and .env work):
  python scripts/inspect_firebase_timestamps.py
  python scripts/inspect_firebase_timestamps.py --segment-date 20260202
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uball-gopro-fleet-firebase-adminsdk.json'),
)


def init_firebase() -> firestore.Client:
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials not found: {FIREBASE_CREDENTIALS_PATH}")
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def format_utc(iso_str: str) -> str:
    """Parse ISO string and show as UTC for clarity."""
    if not iso_str:
        return "(none)"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return iso_str


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Inspect Firebase games and sessions (UTC)')
    parser.add_argument('--segment-date', type=str, default=None,
                        help='Filter sessions by segment date in name, e.g. 20260202')
    parser.add_argument('--games-limit', type=int, default=20, help='Max games to list')
    parser.add_argument('--sessions-limit', type=int, default=30, help='Max sessions to list')
    args = parser.parse_args()

    db = init_firebase()

    print("=" * 70)
    print("Firebase timestamps (pipeline expects all UTC, ISO 8601 with Z)")
    print("=" * 70)

    # Basketball games
    print("\n--- basketball-games (createdAt, endedAt = game start/end in UTC) ---")
    games_ref = db.collection('basketball-games')
    games = list(games_ref.order_by('createdAt', direction=firestore.Query.DESCENDING).limit(args.games_limit).stream())
    for doc in games:
        d = doc.to_dict()
        gid = doc.id
        left = d.get('leftTeam', {}).get('name', '?')
        right = d.get('rightTeam', {}).get('name', '?')
        created = d.get('createdAt', '')
        ended = d.get('endedAt', '')
        print(f"  {gid}")
        print(f"    Teams: {left} vs {right}")
        print(f"    createdAt: {created}  -> {format_utc(created)}")
        print(f"    endedAt:   {ended}  -> {format_utc(ended)}")
        print()
    print(f"  Total shown: {len(games)}")

    # Recording sessions
    print("\n--- recording-sessions (startedAt, endedAt = recording start/end in UTC) ---")
    sessions_ref = db.collection('recording-sessions')
    sessions = list(
        sessions_ref.order_by('startedAt', direction=firestore.Query.DESCENDING).limit(args.sessions_limit).stream()
    )
    for doc in sessions:
        d = doc.to_dict()
        seg = d.get('segmentSession', '')
        if args.segment_date and args.segment_date not in seg:
            continue
        sid = doc.id
        jetson = d.get('jetsonId', '')
        angle = d.get('angleCode', '')
        started = d.get('startedAt', '')
        ended = d.get('endedAt', '')
        print(f"  {sid}")
        print(f"    segmentSession: {seg}")
        print(f"    jetsonId: {jetson}  angleCode: {angle}")
        print(f"    startedAt: {started}  -> {format_utc(started)}")
        print(f"    endedAt:   {ended}  -> {format_utc(ended)}")
        print()
    print(f"  Total shown: {len(sessions)}" + (f" (filtered by segment date {args.segment_date})" if args.segment_date else ""))

    print("\n--- Pipeline matching ---")
    print("  Games are included if: game.createdAt <= recording_end AND game.endedAt >= recording_start")
    print("  recording_start/end = min(session.startedAt), max(session.endedAt) over pipeline sessions.")
    print("  All values must be comparable as UTC strings (e.g. 2026-02-02T14:09:46.000Z).")
    print("=" * 70)


if __name__ == '__main__':
    main()
