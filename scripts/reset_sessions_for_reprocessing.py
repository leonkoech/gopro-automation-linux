#!/usr/bin/env python3
"""
Reset recording-sessions for re-processing: clear s3Prefix and processedGames.

This allows you to re-run the pipeline on sessions that were already uploaded.

Usage:
  python scripts/reset_sessions_for_reprocessing.py --segment-date 20260202
  python scripts/reset_sessions_for_reprocessing.py --session-id SESSION_ID
  python scripts/reset_sessions_for_reprocessing.py --dry-run
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
    parser = argparse.ArgumentParser(description='Reset sessions for re-processing')
    parser.add_argument('--segment-date', type=str, help='Reset sessions with segment date (e.g., 20260202)')
    parser.add_argument('--session-id', type=str, help='Reset specific session ID')
    parser.add_argument('--jetson-id', type=str, help='Filter by jetson ID')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    args = parser.parse_args()

    if not args.segment_date and not args.session_id:
        print("ERROR: Specify --segment-date or --session-id")
        sys.exit(1)

    db = init_firebase()

    sessions_ref = db.collection('recording-sessions')
    
    # Build query
    if args.session_id:
        doc = sessions_ref.document(args.session_id).get()
        if not doc.exists:
            print(f"ERROR: Session {args.session_id} not found")
            sys.exit(1)
        docs = [doc]
    else:
        query = sessions_ref.order_by('startedAt', direction=firestore.Query.DESCENDING).limit(100)
        if args.jetson_id:
            query = query.where('jetsonId', '==', args.jetson_id)
        docs = list(query.stream())

    # Filter by segment date if specified
    if args.segment_date:
        docs = [doc for doc in docs if args.segment_date in doc.to_dict().get('segmentSession', '')]

    if not docs:
        print("No sessions found matching criteria")
        return

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Found {len(docs)} sessions to reset\n")

    for doc in docs:
        d = doc.to_dict()
        sid = doc.id
        seg = d.get('segmentSession', '')
        jetson = d.get('jetsonId', '')
        angle = d.get('angleCode', '')
        s3_prefix = d.get('s3Prefix', '')
        processed_games = d.get('processedGames', [])

        print(f"Session: {sid}")
        print(f"  {jetson} | {angle} | {seg}")
        print(f"  Current s3Prefix: {s3_prefix}")
        print(f"  Current processedGames: {len(processed_games)} games")

        if not s3_prefix and not processed_games:
            print("  -> Already clear (nothing to reset)")
        else:
            if args.dry_run:
                print("  -> Would clear s3Prefix and processedGames")
            else:
                updates = {}
                if s3_prefix:
                    updates['s3Prefix'] = firestore.DELETE_FIELD
                    updates['s3UploadedAt'] = firestore.DELETE_FIELD
                if processed_games:
                    updates['processedGames'] = []
                
                sessions_ref.document(sid).update(updates)
                print("  -> Cleared s3Prefix and processedGames ✓")
        
        print()

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Reset {len(docs)} sessions")
    print("\nThese sessions are now 'pending upload' and can be re-processed by the pipeline.")


if __name__ == '__main__':
    main()
