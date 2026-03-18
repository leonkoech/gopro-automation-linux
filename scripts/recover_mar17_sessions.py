#!/usr/bin/env python3
"""
Recovery script for Mar 17 2026 pipeline failure.

3 games were recorded (~20:33-23:49 UTC) but the pipeline failed:
- Jetson-2 (FL, NR): Pipeline ran but all 3 games failed with "Unknown error".
  Only tiny last chapter files were uploaded instead of the full recording.
- Jetson-1 (FR, NL): No pipeline ran — service had lost recording state.

Root cause: During the 3-hour recording, "Start All" was clicked again (or service
restarted). The stale entry cleanup re-captured pre_record_files which then included
ALL the recording files, so _fetch_gopro_chapters filtered them out as "pre-existing".

Chapter data was verified live from GoPro SD cards on 2026-03-18 via SSH:
  Jetson-1 NL: 3 chapters, 23.40 GB (GX018490-GX038490)
  Jetson-1 FR: 3 chapters, 27.06 GB (GX010060-GX030060)
  Jetson-2 FL: 5 chapters, 46.66 GB (GX010085-GX050085)
  Jetson-2 NR: 4 chapters, 33.76 GB (GX010078-GX040078)

This script:
1. For Jetson-2: Updates existing Firebase sessions with correct chapterFiles,
   resets status to 'stopped', removes s3Prefix
2. For Jetson-1: Creates new Firebase sessions (none exist)
3. Clears failed pipeline-run docs so reprocessing works cleanly

Can run from anywhere (macOS or Jetson) — only needs Firebase credentials.

Usage:
    python scripts/recover_mar17_sessions.py --dry-run     # Preview all changes
    python scripts/recover_mar17_sessions.py --execute      # Apply changes
"""

import os
import sys
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import DELETE_FIELD
from dotenv import load_dotenv

load_dotenv()

FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uball-gopro-fleet-firebase-adminsdk.json'),
)

# ─── Verified chapter data from GoPro SD cards (2026-03-18) ──────────────────
# Recording started at 2026-03-17 20:33:11 UTC on all 4 cameras simultaneously.
# Recording stopped at approximately 2026-03-17 23:49:05 UTC.
# Only files from the Mar 17 20:33+ recording are included (>500 MB each).
# Small test files from earlier in the day (12:55-15:47 UTC) are excluded.
# Old files from prior dates on Jetson-2 NR are excluded.

RECOVERY_DATA = {
    'jetson-1': {
        'cameras': [
            {
                'interfaceId': 'enxd43260dc857e',
                'cameraName': 'Backbone 2',
                'angleCode': 'FR',
                'chapters': [
                    {'directory': '100GOPRO', 'filename': 'GX010060.MP4', 'size': 11681787886},
                    {'directory': '100GOPRO', 'filename': 'GX020060.MP4', 'size': 11588671217},
                    {'directory': '100GOPRO', 'filename': 'GX030060.MP4', 'size': 5779868236},
                ],
            },
            {
                'interfaceId': 'enxd43260ef4d38',
                'cameraName': 'far Side - left',
                'angleCode': 'NL',
                'chapters': [
                    {'directory': '100GOPRO', 'filename': 'GX018490.MP4', 'size': 11477018197},
                    {'directory': '100GOPRO', 'filename': 'GX028490.MP4', 'size': 10176994806},
                    {'directory': '100GOPRO', 'filename': 'GX038490.MP4', 'size': 3471602732},
                ],
            },
        ],
    },
    'jetson-2': {
        'cameras': [
            {
                'interfaceId': 'enxd43260ddac87',
                'cameraName': 'Backbone 1',
                'angleCode': 'FL',
                'chapters': [
                    {'directory': '100GOPRO', 'filename': 'GX010085.MP4', 'size': 11646380998},
                    {'directory': '100GOPRO', 'filename': 'GX020085.MP4', 'size': 11793611403},
                    {'directory': '100GOPRO', 'filename': 'GX030085.MP4', 'size': 11692525652},
                    {'directory': '100GOPRO', 'filename': 'GX040085.MP4', 'size': 11794645144},
                    {'directory': '100GOPRO', 'filename': 'GX050085.MP4', 'size': 3168562556},
                ],
            },
            {
                'interfaceId': 'enxd43260ef4715',
                'cameraName': 'Near Side - left',
                'angleCode': 'NR',
                'chapters': [
                    {'directory': '100GOPRO', 'filename': 'GX010078.MP4', 'size': 11700123568},
                    {'directory': '100GOPRO', 'filename': 'GX020078.MP4', 'size': 11593631477},
                    {'directory': '100GOPRO', 'filename': 'GX030078.MP4', 'size': 11653560267},
                    {'directory': '100GOPRO', 'filename': 'GX040078.MP4', 'size': 1290790715},
                ],
            },
        ],
    },
}

# Recording window (from GoPro cre timestamps)
RECORDING_STARTED_AT = '2026-03-17T20:33:11Z'
RECORDING_ENDED_AT = '2026-03-17T23:49:05Z'
MAR17_DATE = '20260317'


def init_firebase():
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials not found: {FIREBASE_CREDENTIALS_PATH}")
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def find_existing_sessions(db, jetson_id, date_str='20260317'):
    """Find existing Firebase sessions for this Jetson from a specific date."""
    sessions = []
    results = (db.collection('recording-sessions')
               .where('jetsonId', '==', jetson_id)
               .stream())
    for doc in results:
        data = doc.to_dict()
        data['_id'] = doc.id
        segment = data.get('segmentSession', '')
        if date_str in segment:
            sessions.append(data)
    return sessions


def find_failed_pipeline_runs(db, jetson_id, date_str='2026-03-17'):
    """Find failed/errored pipeline runs for this Jetson from a specific date."""
    runs = []
    results = (db.collection('pipeline-runs')
               .where('jetsonId', '==', jetson_id)
               .stream())
    for doc in results:
        data = doc.to_dict()
        data['_id'] = doc.id
        started = data.get('startedAt', '')
        if hasattr(started, 'isoformat'):
            started = started.isoformat()
        if date_str in str(started):
            runs.append(data)
    return runs


def format_size(b):
    if b >= 1024**3:
        return f'{b / (1024**3):.2f} GB'
    elif b >= 1024**2:
        return f'{b / (1024**2):.1f} MB'
    else:
        return f'{b / 1024:.0f} KB'


def main():
    parser = argparse.ArgumentParser(
        description='Recover Mar 17 2026 recording sessions for both Jetsons'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing to Firebase')
    parser.add_argument('--execute', action='store_true',
                        help='Actually write changes to Firebase')
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: Must specify --dry-run or --execute")
        sys.exit(1)

    dry_run = args.dry_run

    print("=" * 80)
    print(f"RECOVER MAR 17 SESSIONS {'[DRY RUN]' if dry_run else '[EXECUTE]'}")
    print("=" * 80)
    print(f"  Recording: {RECORDING_STARTED_AT} → {RECORDING_ENDED_AT}")
    print(f"  Games: LOS RONAS vs AKATSUKI, PREMIER vs THE HIVE, BLESSED vs MIRACLE")

    db = init_firebase()

    total_updated = 0
    total_created = 0
    total_cleared = 0

    for jetson_id in ['jetson-2', 'jetson-1']:
        jetson_data = RECOVERY_DATA[jetson_id]

        print(f"\n{'='*80}")
        print(f"  {jetson_id.upper()}")
        print(f"{'='*80}")

        # ── Find existing sessions ────────────────────────────────────────────
        existing_sessions = find_existing_sessions(db, jetson_id, MAR17_DATE)

        if existing_sessions:
            print(f"\n  Existing Mar 17 sessions:")
            for s in existing_sessions:
                ch_files = s.get('chapterFiles', [])
                print(f"    {s['_id']}: {s.get('segmentSession', '?')} "
                      f"angle={s.get('angleCode', '?')} "
                      f"status={s.get('status', '?')} "
                      f"chapters={len(ch_files)} "
                      f"size={format_size(s.get('totalSizeBytes', 0))}")
        else:
            print(f"\n  No existing Mar 17 sessions (will create new ones)")

        # ── Process each camera ───────────────────────────────────────────────
        for cam_data in jetson_data['cameras']:
            interface_id = cam_data['interfaceId']
            angle = cam_data['angleCode']
            camera_name = cam_data['cameraName']
            chapters = cam_data['chapters']
            total_size = sum(ch['size'] for ch in chapters)

            print(f"\n  --- {angle} ({camera_name}, {interface_id}) ---")
            print(f"  Chapters: {len(chapters)} ({format_size(total_size)})")
            for ch in chapters:
                print(f"    {ch['filename']} ({format_size(ch['size'])})")

            # Find matching existing session
            matching = None
            for s in existing_sessions:
                if s.get('interfaceId') == interface_id or s.get('angleCode') == angle:
                    matching = s
                    break

            if matching:
                # ── Update existing session ───────────────────────────────────
                session_id = matching['_id']
                old_chapters = matching.get('chapterFiles', [])
                print(f"\n  ACTION: Update session {session_id}")
                print(f"    Old: {len(old_chapters)} chapters, {format_size(matching.get('totalSizeBytes', 0))}")
                print(f"    New: {len(chapters)} chapters, {format_size(total_size)}")

                update_data = {
                    'status': 'stopped',
                    'chapterFiles': chapters,
                    'totalChapters': len(chapters),
                    'totalSizeBytes': total_size,
                    's3Prefix': DELETE_FIELD,
                    '_recovery': {
                        'recovered_at': datetime.utcnow().isoformat(),
                        'reason': 'Mar 17 pipeline failure - stale pre_record_files bug',
                        'original_chapters': len(old_chapters),
                        'recovered_chapters': len(chapters),
                    },
                }

                if dry_run:
                    print(f"    [DRY RUN] Would update")
                else:
                    db.collection('recording-sessions').document(session_id).update(update_data)
                    print(f"    UPDATED")
                total_updated += 1

            else:
                # ── Create new session ────────────────────────────────────────
                segment_session = f"{interface_id}_{angle}_{MAR17_DATE}_203311"

                print(f"\n  ACTION: Create new session")
                print(f"    Segment: {segment_session}")
                print(f"    {len(chapters)} chapters, {format_size(total_size)}")

                new_session = {
                    'jetsonId': jetson_id,
                    'cameraName': camera_name,
                    'angleCode': angle,
                    'interfaceId': interface_id,
                    'segmentSession': segment_session,
                    'status': 'stopped',
                    'chapterFiles': chapters,
                    'totalChapters': len(chapters),
                    'totalSizeBytes': total_size,
                    'startedAt': RECORDING_STARTED_AT,
                    'endedAt': RECORDING_ENDED_AT,
                    'processedGames': [],
                    '_recovery': {
                        'recovered_at': datetime.utcnow().isoformat(),
                        'reason': f'Mar 17 pipeline failure - {jetson_id} had no sessions',
                        'recovered_chapters': len(chapters),
                    },
                }

                if dry_run:
                    print(f"    [DRY RUN] Would create")
                else:
                    doc_ref = db.collection('recording-sessions').document()
                    doc_ref.set(new_session)
                    print(f"    CREATED: {doc_ref.id}")
                total_created += 1

        # ── Clear failed pipeline runs ────────────────────────────────────────
        failed_runs = find_failed_pipeline_runs(db, jetson_id, '2026-03-17')
        if failed_runs:
            print(f"\n  Pipeline runs to clear:")
            for run in failed_runs:
                run_id = run['_id']
                status = run.get('status', '')
                if status in ('completed_with_errors', 'failed'):
                    if dry_run:
                        print(f"    [DRY RUN] Would delete {run_id} (status={status})")
                    else:
                        db.collection('pipeline-runs').document(run_id).delete()
                        print(f"    DELETED {run_id} (status={status})")
                    total_cleared += 1
                else:
                    print(f"    SKIP {run_id} (status={status})")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"SUMMARY {'[DRY RUN]' if dry_run else '[EXECUTED]'}")
    print(f"{'='*80}")
    print(f"  Sessions updated:      {total_updated}")
    print(f"  Sessions created:      {total_created}")
    print(f"  Pipeline runs cleared: {total_cleared}")

    if dry_run:
        print(f"\nRun with --execute to apply changes.")
    else:
        print(f"\nDone! All sessions are now in 'stopped' state with correct chapter files.")
        print(f"Next steps:")
        print(f"  1. Deploy bug fixes to both Jetsons (git pull + restart service)")
        print(f"  2. Click 'Process' on the frontend dashboard")
        print(f"  3. Pipeline will upload chapters to S3 and process all 3 games")


if __name__ == '__main__':
    main()
