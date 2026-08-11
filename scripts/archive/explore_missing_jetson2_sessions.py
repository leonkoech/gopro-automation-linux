#!/usr/bin/env python3
"""
Explore missing jetson-2 recording sessions by comparing against jetson-1 data,
checking S3 for chapters, and reviewing AWS Batch job history.

Usage:
    python scripts/explore_missing_jetson2_sessions.py
    python scripts/explore_missing_jetson2_sessions.py --limit 200

This script is READ-ONLY. It does not write anything to Firebase or S3.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore
import boto3
from dotenv import load_dotenv

load_dotenv()

FIREBASE_CREDENTIALS_PATH = os.getenv(
    'FIREBASE_CREDENTIALS_PATH',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'uball-gopro-fleet-firebase-adminsdk.json'),
)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('UPLOAD_REGION', 'us-east-1')
S3_BUCKET = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')

# Known interface -> angle/camera mapping (hardware-fixed)
INTERFACE_ANGLE_MAP = {
    # Jetson-1 cameras
    'enxd43260ef4d38': {'angleCode': 'NL', 'cameraName': 'GoPro NL', 'jetsonId': 'jetson-1'},
    'enxd43260dc857e': {'angleCode': 'FR', 'cameraName': 'GoPro FR', 'jetsonId': 'jetson-1'},
    # Jetson-2 cameras
    'enxd43260ddac87': {'angleCode': 'FL', 'cameraName': 'GoPro FL', 'jetsonId': 'jetson-2'},
    'enxd43260ef4715': {'angleCode': 'NR', 'cameraName': 'GoPro NR', 'jetsonId': 'jetson-2'},
}

# Jetson-2 counterpart for each jetson-1 interface (same recording, opposite angle)
JETSON1_TO_JETSON2_PAIRS = {
    'enxd43260ef4d38': 'enxd43260ddac87',  # NL -> FL (same session time)
    'enxd43260dc857e': 'enxd43260ef4715',  # FR -> NR (same session time)
}
JETSON2_TO_JETSON1_PAIRS = {v: k for k, v in JETSON1_TO_JETSON2_PAIRS.items()}


def init_firebase():
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials not found: {FIREBASE_CREDENTIALS_PATH}")
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def init_s3():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def init_batch():
    return boto3.client(
        'batch',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def format_ts(iso_str):
    if not iso_str:
        return "(none)"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return iso_str


def extract_date_from_segment(segment_session: str) -> str:
    """Extract YYYYMMDD from segmentSession like 'enxd43260ef4d38_20260210_140000'."""
    parts = segment_session.split('_')
    for p in parts:
        if len(p) == 8 and p.isdigit():
            return p
    return 'unknown'


def extract_timestamp_from_segment(segment_session: str) -> str:
    """Extract YYYYMMDD_HHMMSS from segmentSession."""
    parts = segment_session.split('_')
    date_part = None
    time_part = None
    for p in parts:
        if len(p) == 8 and p.isdigit():
            date_part = p
        elif len(p) == 6 and p.isdigit() and date_part:
            time_part = p
    if date_part and time_part:
        return f"{date_part}_{time_part}"
    return 'unknown'


def list_s3_chapters(s3_client, prefix):
    """List all objects under an S3 prefix. Returns list of (key, size)."""
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            objects.append({
                'key': obj['Key'],
                'size_bytes': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
            })
    return objects


def list_s3_prefixes(s3_client, prefix):
    """List top-level 'directories' (common prefixes) under an S3 prefix."""
    prefixes = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            prefixes.append(cp['Prefix'])
    return prefixes


def get_batch_jobs(batch_client, queue_name, limit=100):
    """Get recent Batch jobs from a queue (all statuses)."""
    jobs = []
    for status in ['SUCCEEDED', 'FAILED', 'RUNNING', 'PENDING', 'STARTING', 'SUBMITTED']:
        try:
            response = batch_client.list_jobs(jobQueue=queue_name, jobStatus=status, maxResults=limit)
            for job in response.get('jobSummaryList', []):
                jobs.append({'status': status, **job})
        except Exception as e:
            print(f"  Warning: Could not list {status} jobs: {e}")
    return jobs


def main():
    parser = argparse.ArgumentParser(description='Explore missing jetson-2 recording sessions')
    parser.add_argument('--limit', type=int, default=200, help='Max sessions to fetch from Firebase')
    parser.add_argument('--no-s3', action='store_true', help='Skip S3 checks')
    parser.add_argument('--no-batch', action='store_true', help='Skip AWS Batch checks')
    args = parser.parse_args()

    print("=" * 80)
    print("EXPLORE MISSING JETSON-2 SESSIONS")
    print("=" * 80)

    # ─── 1. Firebase ─────────────────────────────────────────────────────────
    print("\n[1/4] Connecting to Firebase...")
    db = init_firebase()

    # Get ALL recording sessions (both jetsons)
    sessions_ref = db.collection('recording-sessions')
    all_session_docs = list(
        sessions_ref.order_by('startedAt', direction=firestore.Query.DESCENDING).limit(args.limit).stream()
    )
    print(f"  Total recording-sessions fetched: {len(all_session_docs)}")

    sessions_by_jetson = defaultdict(list)
    for doc in all_session_docs:
        d = doc.to_dict()
        d['_id'] = doc.id
        jetson = d.get('jetsonId', 'unknown')
        sessions_by_jetson[jetson].append(d)

    for jetson, sessions in sorted(sessions_by_jetson.items()):
        print(f"  {jetson}: {len(sessions)} sessions")

    # Group jetson-1 sessions by timestamp (YYYYMMDD_HHMMSS)
    j1_by_ts = defaultdict(dict)  # ts -> {angleCode: session}
    for s in sessions_by_jetson.get('jetson-1', []):
        seg = s.get('segmentSession', '')
        ts = extract_timestamp_from_segment(seg)
        angle = s.get('angleCode', 'UNK')
        j1_by_ts[ts][angle] = s

    j2_by_ts = defaultdict(dict)  # ts -> {angleCode: session}
    for s in sessions_by_jetson.get('jetson-2', []):
        seg = s.get('segmentSession', '')
        ts = extract_timestamp_from_segment(seg)
        angle = s.get('angleCode', 'UNK')
        j2_by_ts[ts][angle] = s

    # ─── 2. Basketball games ──────────────────────────────────────────────────
    print("\n[2/4] Fetching basketball games...")
    games_ref = db.collection('basketball-games')
    game_docs = list(
        games_ref.order_by('createdAt', direction=firestore.Query.DESCENDING).limit(100).stream()
    )
    games = []
    for doc in game_docs:
        d = doc.to_dict()
        d['_id'] = doc.id
        games.append(d)

    print(f"  Total games: {len(games)}")

    # Group games by date
    games_by_date = defaultdict(list)
    for g in games:
        created = g.get('createdAt', '')
        date = created[:10] if created else 'unknown'
        games_by_date[date].append(g)

    # ─── 3. Compare jetson-1 vs jetson-2 ────────────────────────────────────
    print("\n[3/4] Comparing jetson-1 vs jetson-2 sessions...")
    print()

    all_timestamps = sorted(set(list(j1_by_ts.keys()) + list(j2_by_ts.keys())), reverse=True)

    missing_j2 = []  # List of dicts describing missing sessions

    for ts in all_timestamps:
        j1_angles = j1_by_ts.get(ts, {})
        j2_angles = j2_by_ts.get(ts, {})
        date = ts[:8] if ts != 'unknown' else 'unknown'
        date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:8]}" if len(date) == 8 else date

        print(f"Timestamp: {ts}  (date: {date_fmt})")
        print(f"  Jetson-1: {sorted(j1_angles.keys()) or '(none)'}")
        print(f"  Jetson-2: {sorted(j2_angles.keys()) or '(none)'}")

        # What's expected on jetson-2 based on jetson-1
        for j1_angle in ['NL', 'FR']:
            if j1_angle in j1_angles:
                j1_session = j1_angles[j1_angle]
                # Corresponding jetson-2 angle: NL<->FL, FR<->NR
                j2_expected_angle = 'FL' if j1_angle == 'NL' else 'NR'
                j1_iface = j1_session.get('interfaceId', '')
                j2_expected_iface = JETSON1_TO_JETSON2_PAIRS.get(j1_iface, '')

                if j2_expected_angle not in j2_angles:
                    j1_seg = j1_session.get('segmentSession', '')
                    expected_j2_seg = j2_expected_iface + '_' + ts if j2_expected_iface else '?_' + ts

                    print(f"  *** MISSING jetson-2 {j2_expected_angle} session! ***")
                    print(f"      Expected segmentSession: {expected_j2_seg}")
                    print(f"      Based on jetson-1 {j1_angle}: {j1_seg}")
                    print(f"      jetson-1 startedAt: {format_ts(j1_session.get('startedAt', ''))}")
                    print(f"      jetson-1 endedAt:   {format_ts(j1_session.get('endedAt', ''))}")
                    print(f"      jetson-1 chapters:  {j1_session.get('totalChapters', 0)}")
                    print(f"      jetson-1 s3Prefix:  {j1_session.get('s3Prefix', '(none)')}")

                    missing_j2.append({
                        'timestamp': ts,
                        'date': date_fmt,
                        'j2_angle': j2_expected_angle,
                        'j2_camera': 'GoPro FL' if j2_expected_angle == 'FL' else 'GoPro NR',
                        'j2_iface': j2_expected_iface,
                        'expected_segment': expected_j2_seg,
                        'j1_session': j1_session,
                    })
                else:
                    j2_session = j2_angles[j2_expected_angle]
                    print(f"  OK: jetson-2 {j2_expected_angle} exists: {j2_session.get('segmentSession', '')} (s3Prefix: {j2_session.get('s3Prefix', '(none)')})")

        # Also show any standalone jetson-2 sessions with no jetson-1 counterpart
        for j2_angle in j2_angles:
            j1_counterpart = 'NL' if j2_angle == 'FL' else 'FR' if j2_angle == 'NR' else None
            if j1_counterpart and j1_counterpart not in j1_angles:
                j2_session = j2_angles[j2_angle]
                print(f"  NOTE: jetson-2 {j2_angle} has no jetson-1 counterpart for this timestamp")

        # Show relevant games for this date
        date_games = games_by_date.get(date_fmt, [])
        if date_games:
            print(f"  Games on {date_fmt}: {len(date_games)}")
            for g in date_games:
                left = g.get('leftTeam', {}).get('name', '?')
                right = g.get('rightTeam', {}).get('name', '?')
                created = g.get('createdAt', '')
                ended = g.get('endedAt', '')
                print(f"    [{g['_id']}] {left} vs {right}")
                print(f"      {format_ts(created)} -> {format_ts(ended)}")

        print()

    # ─── 4. S3 Chapter Check ─────────────────────────────────────────────────
    if not args.no_s3 and missing_j2:
        print("\n[4a/4] Checking S3 for chapter files of missing sessions...")
        try:
            s3 = init_s3()

            # Also list all raw-chapters/ prefixes to see what's there
            print(f"\n  Listing all prefixes in s3://{S3_BUCKET}/raw-chapters/")
            all_prefixes = list_s3_prefixes(s3, 'raw-chapters/')
            print(f"  Found {len(all_prefixes)} session prefixes in S3:")
            for p in sorted(all_prefixes):
                print(f"    {p}")

            print()

            for miss in missing_j2:
                seg = miss['expected_segment']
                prefix = f"raw-chapters/{seg}/"
                print(f"  Checking s3://{S3_BUCKET}/{prefix}")

                # Also check the corresponding jetson-1 prefix
                j1_seg = miss['j1_session'].get('segmentSession', '')
                j1_prefix = f"raw-chapters/{j1_seg}/"

                chapters = list_s3_chapters(s3, prefix)
                if chapters:
                    total_size = sum(c['size_bytes'] for c in chapters)
                    print(f"  FOUND {len(chapters)} chapter(s) for missing session {seg}:")
                    for c in chapters:
                        size_gb = c['size_bytes'] / (1024**3)
                        print(f"    {c['key']}  ({size_gb:.2f} GB)  modified: {c['last_modified'][:19]}")
                    print(f"  Total size: {total_size / (1024**3):.2f} GB")
                    miss['s3_chapters'] = chapters
                    miss['s3_found'] = True
                else:
                    print(f"  NOT FOUND in S3 at {prefix}")
                    # Check if jetson-2 chapters might be uploaded under a variant prefix
                    # Sometimes the folder might have slightly different naming
                    miss['s3_chapters'] = []
                    miss['s3_found'] = False

                # Also show jetson-1 S3 info for reference
                j1_s3_prefix = miss['j1_session'].get('s3Prefix', '')
                if j1_s3_prefix:
                    j1_chapters = list_s3_chapters(s3, j1_s3_prefix)
                    print(f"  Jetson-1 counterpart ({j1_seg}) has {len(j1_chapters)} chapters in S3 at {j1_s3_prefix}")
                print()

        except Exception as e:
            print(f"  S3 error: {e}")
            import traceback
            traceback.print_exc()

    # ─── 5. AWS Batch Job History ─────────────────────────────────────────────
    if not args.no_batch:
        print("\n[4b/4] Checking AWS Batch job history...")
        try:
            batch = init_batch()
            queue_names = [
                os.getenv('AWS_BATCH_JOB_QUEUE', 'gpu-transcode-queue'),
                os.getenv('AWS_BATCH_JOB_QUEUE_LARGE', 'gpu-transcode-queue-large'),
            ]

            for queue_name in queue_names:
                print(f"\n  Queue: {queue_name}")
                jobs = get_batch_jobs(batch, queue_name, limit=50)
                if not jobs:
                    print("  (no jobs found)")
                    continue

                # Sort by creation time
                jobs.sort(key=lambda j: j.get('createdAt', 0), reverse=True)

                print(f"  Total jobs: {len(jobs)}")
                for job in jobs[:30]:  # Show most recent 30
                    name = job.get('jobName', 'unknown')
                    status = job.get('status', 'unknown')
                    created_ms = job.get('createdAt', 0)
                    started_ms = job.get('startedAt', 0)
                    stopped_ms = job.get('stoppedAt', 0)

                    created_str = datetime.utcfromtimestamp(created_ms / 1000).strftime('%Y-%m-%d %H:%M UTC') if created_ms else '?'
                    print(f"  [{status:12s}] {name}  created: {created_str}")

        except Exception as e:
            print(f"  Batch error: {e}")

    # ─── Summary ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    print(f"\nTotal jetson-1 sessions: {len(sessions_by_jetson.get('jetson-1', []))}")
    print(f"Total jetson-2 sessions: {len(sessions_by_jetson.get('jetson-2', []))}")
    print(f"Missing jetson-2 sessions: {len(missing_j2)}")

    if missing_j2:
        print("\nMissing sessions that need to be recreated in Firebase:")
        for i, miss in enumerate(missing_j2, 1):
            j2_found = miss.get('s3_found')
            s3_info = f" [S3: {'FOUND' if j2_found else 'NOT FOUND' if j2_found is not None else 'NOT CHECKED'}]"
            print(f"  {i}. Date: {miss['date']}  Angle: {miss['j2_angle']}  Segment: {miss['expected_segment']}{s3_info}")
            j1_s = miss['j1_session']
            print(f"     Based on jetson-1 {j1_s.get('angleCode')}: startedAt={j1_s.get('startedAt', '')[:19]}  endedAt={j1_s.get('endedAt', '')[:19]}  chapters={j1_s.get('totalChapters')}")

    print("\nDone. Run scripts/recreate_missing_jetson2_sessions.py to recreate missing sessions.")
    print("=" * 80)

    # Output JSON for use by recreate script
    output_file = os.path.join(os.path.dirname(__file__), 'missing_sessions_report.json')
    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'missing_j2_sessions': [
            {
                'timestamp': m['timestamp'],
                'date': m['date'],
                'j2_angle': m['j2_angle'],
                'j2_camera': m['j2_camera'],
                'j2_iface': m['j2_iface'],
                'expected_segment': m['expected_segment'],
                's3_found': m.get('s3_found'),
                's3_chapters': m.get('s3_chapters', []),
                'j1_session_id': m['j1_session']['_id'],
                'j1_segment': m['j1_session'].get('segmentSession', ''),
                'j1_startedAt': m['j1_session'].get('startedAt', ''),
                'j1_endedAt': m['j1_session'].get('endedAt', ''),
                'j1_totalChapters': m['j1_session'].get('totalChapters', 0),
                'j1_totalSizeBytes': m['j1_session'].get('totalSizeBytes', 0),
                'j1_s3Prefix': m['j1_session'].get('s3Prefix', ''),
            }
            for m in missing_j2
        ],
    }
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to: {output_file}")


if __name__ == '__main__':
    main()
