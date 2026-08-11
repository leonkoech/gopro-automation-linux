#!/usr/bin/env python3
"""
Investigate FL/FR sync discrepancy for "Akatsuki vs Hustle +30" game.
Queries Firebase sessions + game data, S3 chapter metadata, and AWS Batch jobs.
"""

import json
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore

# ── Firebase setup ──
CRED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'uball-gopro-fleet-firebase-adminsdk.json'
)
if not firebase_admin._apps:
    cred = credentials.Certificate(CRED_PATH)
    firebase_admin.initialize_app(cred)
db = firestore.client()

# ── AWS setup ──
import boto3
from botocore.config import Config as BotoConfig

boto_cfg = BotoConfig(retries={'max_attempts': 3, 'mode': 'adaptive'})
s3 = boto3.client('s3', config=boto_cfg)
batch = boto3.client('batch', region_name='us-east-1', config=boto_cfg)

BUCKET = 'uball-videos-production'
# Note: user mentioned 87adecc2-5a96-4b92-be35 but that's actually "Blessed vs Ortega" game
# The Akatsuki vs Hustle game has uballGameId: adf42c15-1607-41ee-9e45-cfbb402f8e53
GAME_FOLDER_ID = None  # Will be set from game data

def ts(dt):
    """Format datetime for display."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-3] if dt else 'N/A'

def sec_to_hms(s):
    if s is None:
        return 'N/A'
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:06.3f}"

# ═══════════════════════════════════════════════════════════════
# 1. FIREBASE: Find the game
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("1. FIREBASE GAME DATA")
print("=" * 80)

# Search for the game by listing recent games (avoids composite index requirement)
games_ref = db.collection('basketball-games')
game_doc = None
game_data = None

print("Listing recent games (Mar 24-25)...")
query = games_ref.order_by('createdAt', direction=firestore.Query.DESCENDING).limit(10)
for doc in query.stream():
    d = doc.to_dict()
    lt = d.get('leftTeam', {})
    rt = d.get('rightTeam', {})
    t1 = lt.get('name', lt.get('displayName', '?')) if isinstance(lt, dict) else '?'
    t2 = rt.get('name', rt.get('displayName', '?')) if isinstance(rt, dict) else '?'
    name = f"{t1} vs {t2}"
    created = d.get('createdAt', 'N/A')
    uball_id = d.get('uballGameId', 'N/A')
    print(f"  [{doc.id}] {name} - {created} (uball: {uball_id})")

    # Match the Akatsuki vs Hustle +30 game
    if 'Akatsuki' in name and 'Hustle' in name:
        game_doc = doc
        game_data = d

if game_data:
    lt = game_data.get('leftTeam', {})
    rt = game_data.get('rightTeam', {})
    print(f"\n>>> FOUND GAME: {game_doc.id}")
    print(f"    Left Team:  {lt.get('name', '?') if isinstance(lt, dict) else lt}")
    print(f"    Right Team: {rt.get('name', '?') if isinstance(rt, dict) else rt}")
    print(f"    createdAt:  {ts(game_data.get('createdAt'))}")
    print(f"    endedAt:    {ts(game_data.get('endedAt'))}")
    print(f"    startTime:  {ts(game_data.get('startTime'))}")
    print(f"    endTime:    {ts(game_data.get('endTime'))}")
    print(f"    uballGameId: {game_data.get('uballGameId')}")
    print(f"    gameNumber: {game_data.get('gameNumber')}")

    # Show all relevant fields
    for k in sorted(game_data.keys()):
        if k not in ('team1Name', 'team2Name', 'createdAt', 'endedAt', 'startTime', 'endTime', 'uballGameId', 'gameNumber'):
            v = game_data[k]
            if isinstance(v, (str, int, float, bool, type(None))):
                print(f"    {k}: {v}")

    game_start_str = game_data.get('createdAt')
    game_end_str = game_data.get('endedAt')
    game_start = datetime.fromisoformat(game_start_str.replace('Z', '+00:00'))
    game_end = datetime.fromisoformat(game_end_str.replace('Z', '+00:00'))
    game_duration = (game_end - game_start).total_seconds()
    GAME_FOLDER_ID = game_data.get('uballGameId', '')
    print(f"\n    Game Duration: {sec_to_hms(game_duration)} ({game_duration:.1f}s)")
    print(f"    uballGameId (game folder): {GAME_FOLDER_ID}")
else:
    print("ERROR: Could not find the game!")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# 2. FIREBASE: Find FL and FR recording sessions
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("2. FIREBASE RECORDING SESSIONS")
print("=" * 80)

sessions_ref = db.collection('recording-sessions')

# Get sessions from both jetsons around the game time
# Widen the window to catch sessions
search_start = game_start - timedelta(hours=6)
search_end = game_end + timedelta(hours=2)

# Query for jetson-1 (FR) and jetson-2 (FL) sessions
fl_sessions = []
fr_sessions = []

for jetson_id in ['jetson-1', 'jetson-2']:
    query = (sessions_ref
             .where('jetsonId', '==', jetson_id)
             .order_by('startedAt', direction=firestore.Query.DESCENDING)
             .limit(20))

    for doc in query.stream():
        d = doc.to_dict()
        d['id'] = doc.id
        started = d.get('startedAt', '')
        ended = d.get('endedAt', '')
        angle = d.get('angleCode', '?')

        if not started:
            continue

        s_start = datetime.fromisoformat(started.replace('Z', '+00:00'))
        s_end = datetime.fromisoformat(ended.replace('Z', '+00:00')) if ended else datetime.now(timezone.utc)

        # Check if session overlaps with game (with generous buffer)
        if s_start < game_end + timedelta(hours=1) and s_end > game_start - timedelta(hours=1):
            print(f"\n[{jetson_id}] Session: {doc.id}")
            print(f"  angleCode: {angle}")
            print(f"  startedAt: {ts(started)}")
            print(f"  endedAt:   {ts(ended)}")
            print(f"  segmentSession: {d.get('segmentSession')}")
            print(f"  s3Prefix: {d.get('s3Prefix')}")
            print(f"  status: {d.get('status')}")
            print(f"  totalChapters: {d.get('totalChapters')}")
            print(f"  totalSizeBytes: {d.get('totalSizeBytes')}")

            # Show chapter files if present
            chapter_files = d.get('chapterFiles', [])
            if chapter_files:
                print(f"  chapterFiles ({len(chapter_files)}):")
                for cf in chapter_files:
                    if isinstance(cf, dict):
                        print(f"    - {cf.get('filename', '?')} ({cf.get('size_mb', '?')} MB)")
                    else:
                        print(f"    - {cf}")

            # Show processed games
            processed = d.get('processedGames', [])
            if processed:
                print(f"  processedGames ({len(processed)}):")
                for pg in processed:
                    if isinstance(pg, dict):
                        print(f"    - {json.dumps({k: v for k, v in pg.items()}, default=str)}")
                    else:
                        print(f"    - {pg}")

            if angle == 'FL':
                fl_sessions.append(d)
            elif angle == 'FR':
                fr_sessions.append(d)

print(f"\nFL sessions found: {len(fl_sessions)}")
print(f"FR sessions found: {len(fr_sessions)}")

# ═══════════════════════════════════════════════════════════════
# 3. S3 CHAPTER METADATA
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("3. S3 CHAPTER METADATA (via ffprobe)")
print("=" * 80)

def get_chapter_metadata_from_s3(s3_prefix, label):
    """List chapters in S3 and get duration/creation_time via ffprobe."""
    print(f"\n--- {label}: s3://{BUCKET}/{s3_prefix} ---")

    chapters = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=s3_prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            fname = key.split('/')[-1]
            if not fname.lower().endswith('.mp4'):
                continue
            chapters.append({
                'key': key,
                'filename': fname,
                'size_bytes': obj['Size'],
                'size_mb': round(obj['Size'] / (1024*1024), 2)
            })

    # Sort by filename
    chapters.sort(key=lambda c: c['filename'])

    print(f"Found {len(chapters)} chapters")

    total_duration = 0
    for ch in chapters:
        # Generate presigned URL and run ffprobe
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET, 'Key': ch['key']},
            ExpiresIn=3600
        )

        try:
            result = subprocess.run(
                ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                 '-show_entries', 'format=duration:format_tags=creation_time',
                 url],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                fmt = data.get('format', {})
                duration = float(fmt.get('duration', 0))
                creation_time = fmt.get('tags', {}).get('creation_time', 'N/A')
                ch['duration_seconds'] = duration
                ch['creation_time'] = creation_time
                total_duration += duration
            else:
                ch['duration_seconds'] = None
                ch['creation_time'] = 'ERROR'
                print(f"  ffprobe error for {ch['filename']}: {result.stderr[:200]}")
        except Exception as e:
            ch['duration_seconds'] = None
            ch['creation_time'] = f'ERROR: {e}'

    # Print results
    cumulative = 0
    for i, ch in enumerate(chapters):
        dur = ch.get('duration_seconds')
        ct = ch.get('creation_time', 'N/A')
        dur_str = sec_to_hms(dur) if dur else 'N/A'
        print(f"  [{i+1}] {ch['filename']:40s} {ch['size_mb']:>8.1f} MB  dur={dur_str}  creation_time={ct}  cumulative={sec_to_hms(cumulative)}")
        if dur:
            cumulative += dur

    print(f"  TOTAL duration: {sec_to_hms(total_duration)} ({total_duration:.3f}s)")
    return chapters, total_duration

# Get chapters for FL and FR
fl_chapters_data = None
fr_chapters_data = None

for s in fl_sessions:
    prefix = s.get('s3Prefix', '')
    if prefix:
        fl_chapters_data = get_chapter_metadata_from_s3(prefix, f"FL ({s.get('segmentSession')})")

for s in fr_sessions:
    prefix = s.get('s3Prefix', '')
    if prefix:
        fr_chapters_data = get_chapter_metadata_from_s3(prefix, f"FR ({s.get('segmentSession')})")

# ═══════════════════════════════════════════════════════════════
# 4. AWS BATCH JOBS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("4. AWS BATCH JOBS")
print("=" * 80)

def find_batch_jobs(queue_name, game_folder):
    """Find batch jobs for this game in the given queue."""
    jobs_found = []

    for status in ['SUCCEEDED', 'FAILED', 'RUNNING', 'SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING']:
        try:
            response = batch.list_jobs(
                jobQueue=queue_name,
                jobStatus=status,
                maxResults=100
            )
            for job_summary in response.get('jobSummaryList', []):
                job_name = job_summary.get('jobName', '')
                if 'extract-transcode' in job_name and ('FL' in job_name or 'FR' in job_name):
                    # Check timestamp range (around March 24-25, 2026)
                    created = job_summary.get('createdAt', 0) / 1000  # ms to s
                    if created > 0:
                        job_time = datetime.fromtimestamp(created, tz=timezone.utc)
                        if job_time > game_start - timedelta(hours=12) and job_time < game_end + timedelta(hours=24):
                            jobs_found.append(job_summary)
        except Exception as e:
            print(f"  Error querying {queue_name}/{status}: {e}")

    return jobs_found

# Search both queues
all_jobs = []
for queue in ['gpu-transcode-queue-large', 'gpu-transcode-queue']:
    print(f"\nSearching queue: {queue}")
    found = find_batch_jobs(queue, GAME_FOLDER_ID)
    print(f"  Found {len(found)} matching jobs")
    all_jobs.extend(found)

# Get detailed info for each job
print(f"\nTotal matching jobs: {len(all_jobs)}")

for job_summary in all_jobs:
    job_id = job_summary['jobId']
    job_name = job_summary.get('jobName', 'N/A')
    status = job_summary.get('status', '?')
    created_ms = job_summary.get('createdAt', 0)
    created_dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc) if created_ms else None

    print(f"\n  Job: {job_name}")
    print(f"  ID: {job_id}")
    print(f"  Status: {status}")
    print(f"  Created: {ts(created_dt)}")

    # Get full job details including environment variables
    try:
        detail_resp = batch.describe_jobs(jobs=[job_id])
        if detail_resp['jobs']:
            job = detail_resp['jobs'][0]

            # Extract environment variables
            env_vars = {}
            container = job.get('container', {})
            for env in container.get('environment', []):
                env_vars[env['name']] = env['value']

            print(f"  Job Definition: {job.get('jobDefinition', 'N/A')}")
            print(f"  Queue: {job.get('jobQueue', 'N/A')}")

            if 'startedAt' in job:
                print(f"  Started: {ts(datetime.fromtimestamp(job['startedAt']/1000, tz=timezone.utc))}")
            if 'stoppedAt' in job:
                print(f"  Stopped: {ts(datetime.fromtimestamp(job['stoppedAt']/1000, tz=timezone.utc))}")

            # Key environment variables
            print(f"  Environment Variables:")
            for key in ['CHAPTERS_JSON', 'CHAPTER_DURATIONS_JSON', 'OFFSET_SECONDS', 'DURATION_SECONDS',
                        'ADD_BUFFER_SECONDS', 'OUTPUT_S3_KEY', 'GAME_ID', 'ANGLE', 'BUCKET']:
                val = env_vars.get(key, 'N/A')
                if key == 'CHAPTERS_JSON':
                    try:
                        chapters_list = json.loads(val)
                        print(f"    {key}: [{len(chapters_list)} chapters]")
                        for ck in chapters_list:
                            print(f"      - {ck}")
                    except:
                        print(f"    {key}: {val[:200]}")
                elif key == 'CHAPTER_DURATIONS_JSON':
                    try:
                        durations = json.loads(val)
                        print(f"    {key}: {durations}")
                        print(f"      Total chapter duration: {sum(durations):.3f}s = {sec_to_hms(sum(durations))}")
                    except:
                        print(f"    {key}: {val[:200]}")
                else:
                    print(f"    {key}: {val}")

            # Log stream
            log_stream = container.get('logStreamName')
            if log_stream:
                print(f"  Log Stream: {log_stream}")

    except Exception as e:
        print(f"  Error getting details: {e}")

# ═══════════════════════════════════════════════════════════════
# 5. CALCULATE EXPECTED vs ACTUAL OFFSETS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("5. OFFSET ANALYSIS: EXPECTED vs ACTUAL")
print("=" * 80)

if fl_sessions and fr_sessions and game_data:
    fl_session = fl_sessions[0]
    fr_session = fr_sessions[0]

    fl_start_str = fl_session.get('startedAt')
    fr_start_str = fr_session.get('startedAt')

    fl_start = datetime.fromisoformat(fl_start_str.replace('Z', '+00:00'))
    fr_start = datetime.fromisoformat(fr_start_str.replace('Z', '+00:00'))

    print(f"\nGame start (createdAt):   {ts(game_start)}")
    print(f"Game end (endedAt):       {ts(game_end)}")
    print(f"Game duration:            {sec_to_hms(game_duration)}")
    print(f"")
    print(f"FL recording start:       {ts(fl_start)}")
    print(f"FR recording start:       {ts(fr_start)}")
    print(f"FL-FR start difference:   {(fl_start - fr_start).total_seconds():.3f}s")
    print(f"  (FL starts {'later' if fl_start > fr_start else 'earlier'} than FR by {abs((fl_start - fr_start).total_seconds()):.3f}s)")

    # Calculate expected offsets
    fl_expected_offset = (game_start - fl_start).total_seconds()
    fr_expected_offset = (game_start - fr_start).total_seconds()

    print(f"\n--- Expected Offsets (from recording start) ---")
    print(f"FL expected offset: {sec_to_hms(fl_expected_offset)} ({fl_expected_offset:.3f}s)")
    print(f"FR expected offset: {sec_to_hms(fr_expected_offset)} ({fr_expected_offset:.3f}s)")
    print(f"FL-FR offset difference: {abs(fl_expected_offset - fr_expected_offset):.3f}s")

    # Now simulate chapter filtering and offset calculation like the code does
    print(f"\n--- Simulating calculate_extraction_params() ---")

    # FL chapters simulation
    if fl_chapters_data:
        fl_chapters, fl_total_dur = fl_chapters_data
        print(f"\nFL Session:")
        print(f"  Total chapters before filtering: {len(fl_chapters)}")

        # Simulate creation_time filtering
        from datetime import timedelta as td
        parsed_times = []
        for ch in fl_chapters:
            ct = ch.get('creation_time')
            if ct and ct != 'N/A' and not ct.startswith('ERROR'):
                try:
                    parsed = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                    parsed_times.append((ch['filename'], parsed))
                except:
                    parsed_times.append((ch['filename'], None))
            else:
                parsed_times.append((ch['filename'], None))

        if parsed_times:
            valid = [(n, t) for n, t in parsed_times if t]
            if valid:
                last_time = valid[-1][1]
                threshold = td(hours=6)
                kept = [(n, t) for n, t in valid if abs(t - last_time) <= threshold]
                removed = [(n, t) for n, t in valid if abs(t - last_time) > threshold]

                if removed:
                    print(f"  Chapters filtered out (old creation_time):")
                    for n, t in removed:
                        dur_ch = next((c.get('duration_seconds', 0) for c in fl_chapters if c['filename'] == n), 0)
                        print(f"    - {n} (creation_time={ts(t)}, duration={dur_ch:.3f}s)")

                    total_removed_dur = sum(
                        next((c.get('duration_seconds', 0) for c in fl_chapters if c['filename'] == n), 0)
                        for n, t in removed
                    )
                    print(f"  Total duration of removed chapters: {sec_to_hms(total_removed_dur)} ({total_removed_dur:.3f}s)")
                    print(f"  *** THIS IS KEY: if chapters are wrongly kept/removed, the offset calculation breaks ***")

                print(f"  Chapters kept ({len(kept)}):")
                cum = 0
                for n, t in kept:
                    dur_ch = next((c.get('duration_seconds', 0) for c in fl_chapters if c['filename'] == n), 0)
                    print(f"    - {n} (creation_time={ts(t)}, duration={dur_ch:.3f}s, cumulative={cum:.3f}s)")
                    cum += dur_ch

        # Simulate offset calculation with KEPT chapters only
        kept_chapters = []
        if parsed_times:
            valid = [(n, t) for n, t in parsed_times if t]
            if valid:
                last_time = valid[-1][1]
                threshold = td(hours=6)
                kept_names = set(n for n, t in valid if abs(t - last_time) <= threshold)
                kept_chapters = [c for c in fl_chapters if c['filename'] in kept_names]

        if not kept_chapters:
            kept_chapters = fl_chapters

        fl_offset_from_rec = (game_start - fl_start).total_seconds()
        if fl_offset_from_rec < 0:
            fl_offset_from_rec = 0

        current_time = 0
        first_needed_start = 0
        chapters_needed_fl = []
        for i, ch in enumerate(kept_chapters):
            ch_dur = ch.get('duration_seconds', 0) or 900
            ch_end = current_time + ch_dur
            game_start_in_rec = fl_offset_from_rec
            game_end_in_rec = fl_offset_from_rec + game_duration

            if current_time < game_end_in_rec and ch_end > game_start_in_rec:
                if not chapters_needed_fl:
                    first_needed_start = current_time
                chapters_needed_fl.append(ch)
            current_time = ch_end

        fl_offset_relative = fl_offset_from_rec - first_needed_start
        if fl_offset_relative < 0:
            fl_offset_relative = 0

        print(f"\n  FL offset from recording start: {sec_to_hms(fl_offset_from_rec)} ({fl_offset_from_rec:.3f}s)")
        print(f"  FL first needed chapter starts at: {sec_to_hms(first_needed_start)} ({first_needed_start:.3f}s)")
        print(f"  FL offset relative to chapters: {sec_to_hms(fl_offset_relative)} ({fl_offset_relative:.3f}s)")
        print(f"  FL chapters needed: {len(chapters_needed_fl)}")

    # FR chapters simulation
    if fr_chapters_data:
        fr_chapters, fr_total_dur = fr_chapters_data
        print(f"\nFR Session:")
        print(f"  Total chapters before filtering: {len(fr_chapters)}")

        parsed_times = []
        for ch in fr_chapters:
            ct = ch.get('creation_time')
            if ct and ct != 'N/A' and not ct.startswith('ERROR'):
                try:
                    parsed = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                    parsed_times.append((ch['filename'], parsed))
                except:
                    parsed_times.append((ch['filename'], None))
            else:
                parsed_times.append((ch['filename'], None))

        if parsed_times:
            valid = [(n, t) for n, t in parsed_times if t]
            if valid:
                last_time = valid[-1][1]
                threshold = td(hours=6)
                kept = [(n, t) for n, t in valid if abs(t - last_time) <= threshold]
                removed = [(n, t) for n, t in valid if abs(t - last_time) > threshold]

                if removed:
                    print(f"  Chapters filtered out (old creation_time):")
                    for n, t in removed:
                        dur_ch = next((c.get('duration_seconds', 0) for c in fr_chapters if c['filename'] == n), 0)
                        print(f"    - {n} (creation_time={ts(t)}, duration={dur_ch:.3f}s)")

                    total_removed_dur = sum(
                        next((c.get('duration_seconds', 0) for c in fr_chapters if c['filename'] == n), 0)
                        for n, t in removed
                    )
                    print(f"  Total duration of removed chapters: {sec_to_hms(total_removed_dur)} ({total_removed_dur:.3f}s)")

                print(f"  Chapters kept ({len(kept)}):")
                cum = 0
                for n, t in kept:
                    dur_ch = next((c.get('duration_seconds', 0) for c in fr_chapters if c['filename'] == n), 0)
                    print(f"    - {n} (creation_time={ts(t)}, duration={dur_ch:.3f}s, cumulative={cum:.3f}s)")
                    cum += dur_ch

        # Simulate offset calculation
        kept_chapters_fr = []
        if parsed_times:
            valid = [(n, t) for n, t in parsed_times if t]
            if valid:
                last_time = valid[-1][1]
                threshold = td(hours=6)
                kept_names = set(n for n, t in valid if abs(t - last_time) <= threshold)
                kept_chapters_fr = [c for c in fr_chapters if c['filename'] in kept_names]

        if not kept_chapters_fr:
            kept_chapters_fr = fr_chapters

        fr_offset_from_rec = (game_start - fr_start).total_seconds()
        if fr_offset_from_rec < 0:
            fr_offset_from_rec = 0

        current_time = 0
        first_needed_start = 0
        chapters_needed_fr = []
        for i, ch in enumerate(kept_chapters_fr):
            ch_dur = ch.get('duration_seconds', 0) or 900
            ch_end = current_time + ch_dur
            game_start_in_rec = fr_offset_from_rec
            game_end_in_rec = fr_offset_from_rec + game_duration

            if current_time < game_end_in_rec and ch_end > game_start_in_rec:
                if not chapters_needed_fr:
                    first_needed_start = current_time
                chapters_needed_fr.append(ch)
            current_time = ch_end

        fr_offset_relative = fr_offset_from_rec - first_needed_start
        if fr_offset_relative < 0:
            fr_offset_relative = 0

        print(f"\n  FR offset from recording start: {sec_to_hms(fr_offset_from_rec)} ({fr_offset_from_rec:.3f}s)")
        print(f"  FR first needed chapter starts at: {sec_to_hms(first_needed_start)} ({first_needed_start:.3f}s)")
        print(f"  FR offset relative to chapters: {sec_to_hms(fr_offset_relative)} ({fr_offset_relative:.3f}s)")
        print(f"  FR chapters needed: {len(chapters_needed_fr)}")

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("6. SUMMARY & ROOT CAUSE ANALYSIS")
    print("=" * 80)

    print(f"\nRecording Start Times:")
    print(f"  FL: {ts(fl_start)}")
    print(f"  FR: {ts(fr_start)}")
    print(f"  Difference: {abs((fl_start - fr_start).total_seconds()):.3f}s")

    print(f"\nGame Window:")
    print(f"  Start: {ts(game_start)}")
    print(f"  End:   {ts(game_end)}")

    print(f"\nExpected Offsets (game_start - recording_start):")
    print(f"  FL: {fl_expected_offset:.3f}s")
    print(f"  FR: {fr_expected_offset:.3f}s")
    print(f"  Difference: {abs(fl_expected_offset - fr_expected_offset):.3f}s")

    if fl_chapters_data and fr_chapters_data:
        print(f"\nComputed Relative Offsets (after chapter selection):")
        print(f"  FL: {fl_offset_relative:.3f}s")
        print(f"  FR: {fr_offset_relative:.3f}s")
        print(f"  Difference: {abs(fl_offset_relative - fr_offset_relative):.3f}s")

        print(f"\n  The relative difference between FL and FR offsets should match the")
        print(f"  difference in recording start times ({abs((fl_start - fr_start).total_seconds()):.3f}s).")
        print(f"  If it doesn't, chapter filtering or duration data is causing the discrepancy.")

print("\n" + "=" * 80)
print("INVESTIGATION COMPLETE")
print("=" * 80)
