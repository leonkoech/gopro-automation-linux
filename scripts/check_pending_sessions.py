#!/usr/bin/env python3
"""
Check Firebase recording-sessions to understand why "No pending sessions found".

Run: python scripts/check_pending_sessions.py
"""

import os
import sys
from datetime import datetime

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


def format_ts(iso_str: str) -> str:
    """Parse ISO string and show as UTC."""
    if not iso_str:
        return "(none)"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return iso_str


def main():
    db = init_firebase()

    print("=" * 80)
    print("Firebase recording-sessions check (why 'No pending sessions found'?)")
    print("=" * 80)

    sessions_ref = db.collection('recording-sessions')
    
    # Get all sessions ordered by startedAt
    all_sessions = list(sessions_ref.order_by('startedAt', direction=firestore.Query.DESCENDING).limit(50).stream())
    
    print(f"\nTotal sessions in Firebase: {len(all_sessions)}")
    
    # Criteria for "pending upload" from firebase_service.py get_sessions_pending_upload:
    # 1. status == 'stopped'
    # 2. s3Prefix is None or empty
    # 3. endedAt is not None
    
    print("\n--- Checking which sessions are 'pending upload' ---")
    print("Criteria: status='stopped' AND s3Prefix is empty AND endedAt exists\n")
    
    pending_count = 0
    for doc in all_sessions:
        d = doc.to_dict()
        sid = doc.id
        seg = d.get('segmentSession', '')
        jetson = d.get('jetsonId', '')
        angle = d.get('angleCode', '')
        status = d.get('status', '')
        s3_prefix = d.get('s3Prefix', '')
        started = d.get('startedAt', '')
        ended = d.get('endedAt', '')
        
        # Check if pending
        is_pending = (
            status == 'stopped' and
            not s3_prefix and
            ended
        )
        
        if is_pending:
            pending_count += 1
        
        marker = "✓ PENDING" if is_pending else "✗ NOT PENDING"
        
        print(f"{marker} | {sid}")
        print(f"  segmentSession: {seg}")
        print(f"  jetsonId: {jetson}  angleCode: {angle}")
        print(f"  status: '{status}'  s3Prefix: '{s3_prefix}'")
        print(f"  startedAt: {format_ts(started)}")
        print(f"  endedAt:   {format_ts(ended)}")
        
        if not is_pending:
            reasons = []
            if status != 'stopped':
                reasons.append(f"status is '{status}' (not 'stopped')")
            if s3_prefix:
                reasons.append(f"s3Prefix is set: '{s3_prefix}'")
            if not ended:
                reasons.append("endedAt is missing/null")
            print(f"  Reason NOT pending: {', '.join(reasons)}")
        
        print()
    
    print("=" * 80)
    print(f"Summary: {pending_count} pending sessions out of {len(all_sessions)} total")
    print("=" * 80)
    
    # Show sessions by jetson
    print("\n--- Sessions by Jetson ---")
    jetson_sessions = {}
    for doc in all_sessions:
        d = doc.to_dict()
        jetson = d.get('jetsonId', 'unknown')
        status = d.get('status', '')
        s3_prefix = d.get('s3Prefix', '')
        ended = d.get('endedAt', '')
        
        is_pending = status == 'stopped' and not s3_prefix and ended
        
        if jetson not in jetson_sessions:
            jetson_sessions[jetson] = {'total': 0, 'pending': 0}
        jetson_sessions[jetson]['total'] += 1
        if is_pending:
            jetson_sessions[jetson]['pending'] += 1
    
    for jetson, counts in sorted(jetson_sessions.items()):
        print(f"  {jetson}: {counts['pending']} pending / {counts['total']} total")


if __name__ == '__main__':
    main()
