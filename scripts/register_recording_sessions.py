#!/usr/bin/env python3
"""
Register existing recording sessions in Firebase.

These recordings happened before the game-video integration was built,
so they were never registered in Firebase's recording-sessions collection.
This script creates the documents retroactively.

Usage:
    python scripts/register_recording_sessions.py [--dry-run] [--cleanup]
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

load_dotenv()

FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    '/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json'
)

# Recording session: 2026-01-20 19:50:30 UTC
# All 4 cameras started simultaneously across 2 Jetsons
# Each chapter is approximately 35 minutes

RECORDING_START = datetime(2026, 1, 20, 19, 50, 30, tzinfo=timezone.utc)
CHAPTER_DURATION_MINUTES = 35

# Camera assignments per Jetson:
#   Jetson 1 (100.87.190.71, JETSON_ID=jetson-1): NL (Near Left) + FR (Far Right)
#   Jetson 2 (100.106.30.98, JETSON_ID=jetson-2): NR (Near Right) + FL (Far Left)

SESSIONS = [
    # Jetson 2 (100.106.30.98, JETSON_ID=jetson-2): NR + FL cameras
    # These 1-chapter sessions are shorter test recordings
    {
        'jetsonId': 'jetson-2',
        'interfaceId': 'enxd43260ddac87',
        'segmentSession': 'enxd43260ddac87_20260120_195030',
        'cameraName': 'GoPro FL',
        'angleCode': 'FL',
        'chapters': [
            {'filename': 'chapter_001_GX010041.MP4', 'size_bytes': 2035548160},
        ],
    },
    {
        'jetsonId': 'jetson-2',
        'interfaceId': 'enxd43260ef4715',
        'segmentSession': 'enxd43260ef4715_20260120_195030',
        'cameraName': 'GoPro NR',
        'angleCode': 'NR',  # FIXED: Was incorrectly FR
        'chapters': [
            {'filename': 'chapter_001_GX010038.MP4', 'size_bytes': 1604911104},
        ],
    },
    # Jetson 1 (100.87.190.71, JETSON_ID=jetson-1): NL + FR cameras
    # These 3-chapter sessions are full recordings for test games
    {
        'jetsonId': 'jetson-1',
        'interfaceId': 'enxd43260ef4d38',
        'segmentSession': 'enxd43260ef4d38_20260120_195030',
        'cameraName': 'GoPro NL',
        'angleCode': 'NL',
        'chapters': [
            {'filename': 'chapter_001_GX018471.MP4', 'size_bytes': 10208434006},
            {'filename': 'chapter_002_GX028471.MP4', 'size_bytes': 9927537132},
            {'filename': 'chapter_003_GX038471.MP4', 'size_bytes': 9182518519},
        ],
    },
    {
        'jetsonId': 'jetson-1',
        'interfaceId': 'enxd43260dc857e',
        'segmentSession': 'enxd43260dc857e_20260120_195030',
        'cameraName': 'GoPro FR',
        'angleCode': 'FR',  # FIXED: Was incorrectly NR
        'chapters': [
            {'filename': 'chapter_001_GX010038.MP4', 'size_bytes': 9485512585},
            {'filename': 'chapter_002_GX020038.MP4', 'size_bytes': 9411373351},
            {'filename': 'chapter_003_GX030038.MP4', 'size_bytes': 8336145042},
        ],
    },
]


def to_js_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


def initialize_firebase():
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def create_recording_session(db, session_config, dry_run=False):
    num_chapters = len(session_config['chapters'])
    total_duration = timedelta(minutes=CHAPTER_DURATION_MINUTES * num_chapters)
    end_time = RECORDING_START + total_duration
    total_size = sum(ch['size_bytes'] for ch in session_config['chapters'])

    doc_data = {
        'jetsonId': session_config['jetsonId'],
        'cameraName': session_config['cameraName'],
        'angleCode': session_config['angleCode'],
        'startedAt': to_js_iso(RECORDING_START),
        'endedAt': to_js_iso(end_time),
        'segmentSession': session_config['segmentSession'],
        'interfaceId': session_config['interfaceId'],
        'totalChapters': num_chapters,
        'totalSizeBytes': total_size,
        'status': 'stopped',
        'processedGames': [],
        '_test_metadata': {
            'fabricated': True,
            'registered_retroactively': True,
        },
    }

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Creating recording-session:")
    print(f"  Jetson: {session_config['jetsonId']}")
    print(f"  Session: {session_config['segmentSession']}")
    print(f"  Camera: {session_config['cameraName']} ({session_config['angleCode']})")
    print(f"  Chapters: {num_chapters}")
    print(f"  Time: {to_js_iso(RECORDING_START)} -> {to_js_iso(end_time)}")
    print(f"  Size: {total_size / (1024**3):.2f} GB")

    if dry_run:
        return None

    doc_ref = db.collection('recording-sessions').document()
    doc_ref.set(doc_data)
    print(f"  Created: {doc_ref.id}")
    return doc_ref.id


def cleanup(db, dry_run=False):
    print("\nCleaning up fabricated recording-sessions...")
    count = 0
    for doc in db.collection('recording-sessions').stream():
        data = doc.to_dict()
        if data.get('_test_metadata', {}).get('fabricated'):
            print(f"  {'[DRY RUN] ' if dry_run else ''}Deleting: {doc.id} ({data.get('segmentSession')})")
            if not dry_run:
                doc.reference.delete()
            count += 1
    print(f"{'Would delete' if dry_run else 'Deleted'} {count} documents")


def list_sessions(db):
    print("\nAll recording-sessions in Firebase:")
    for doc in db.collection('recording-sessions').stream():
        data = doc.to_dict()
        fab = '[TEST]' if data.get('_test_metadata', {}).get('fabricated') else ''
        print(f"  {doc.id} {fab}")
        print(f"    {data.get('segmentSession')} | {data.get('angleCode')} | {data.get('jetsonId')}")
        print(f"    {data.get('startedAt')} -> {data.get('endedAt')}")


# Correct angle mapping by interface ID
# These are fixed hardware assignments
INTERFACE_ANGLE_MAP = {
    # Jetson 1 cameras (100.87.190.71)
    'enxd43260ef4d38': {'angleCode': 'NL', 'cameraName': 'GoPro NL'},
    'enxd43260dc857e': {'angleCode': 'FR', 'cameraName': 'GoPro FR'},
    # Jetson 2 cameras (100.106.30.98)
    'enxd43260ddac87': {'angleCode': 'FL', 'cameraName': 'GoPro FL'},
    'enxd43260ef4715': {'angleCode': 'NR', 'cameraName': 'GoPro NR'},
}


def fix_angles(db, dry_run=False):
    """Fix angle codes in existing Firebase recording sessions based on interface ID."""
    print("\nFixing angle codes in recording-sessions...")
    fixed_count = 0

    for doc in db.collection('recording-sessions').stream():
        data = doc.to_dict()
        interface_id = data.get('interfaceId')
        current_angle = data.get('angleCode')
        current_camera = data.get('cameraName')

        if interface_id in INTERFACE_ANGLE_MAP:
            correct = INTERFACE_ANGLE_MAP[interface_id]
            correct_angle = correct['angleCode']
            correct_camera = correct['cameraName']

            if current_angle != correct_angle or current_camera != correct_camera:
                print(f"\n  {'[DRY RUN] ' if dry_run else ''}Fixing: {doc.id}")
                print(f"    Session: {data.get('segmentSession')}")
                print(f"    Interface: {interface_id}")
                print(f"    Current: {current_camera} ({current_angle})")
                print(f"    Correct: {correct_camera} ({correct_angle})")

                if not dry_run:
                    doc.reference.update({
                        'angleCode': correct_angle,
                        'cameraName': correct_camera
                    })
                    print(f"    Updated!")

                fixed_count += 1
            else:
                print(f"  OK: {data.get('segmentSession')} - {current_angle}")
        else:
            print(f"  UNKNOWN INTERFACE: {interface_id} - skipping")

    print(f"\n{'Would fix' if dry_run else 'Fixed'} {fixed_count} sessions")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--cleanup', action='store_true')
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--fix-angles', action='store_true',
                        help='Fix angle codes in existing Firebase sessions based on interface ID')
    args = parser.parse_args()

    print("=" * 60)
    print("Recording Sessions Registration Tool")
    print("=" * 60)

    db = initialize_firebase()
    print("Firebase initialized")

    if args.list:
        list_sessions(db)
        return

    if args.fix_angles:
        fix_angles(db, dry_run=args.dry_run)
        return

    if args.cleanup:
        cleanup(db, dry_run=args.dry_run)
        return

    # Create sessions
    created = []
    for session in SESSIONS:
        doc_id = create_recording_session(db, session, dry_run=args.dry_run)
        if doc_id:
            created.append(doc_id)

    print(f"\n{'Would create' if args.dry_run else 'Created'} {len(created) if not args.dry_run else len(SESSIONS)} recording-session documents")

    if created:
        print("\nDocument IDs:")
        for did in created:
            print(f"  {did}")


if __name__ == '__main__':
    main()
