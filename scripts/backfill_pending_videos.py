#!/usr/bin/env python3
"""
Backfill missing video_metadata registrations — the safety net for the
batch-poller registration gap.

Background
----------
The pipeline encodes each game via AWS Batch, writing the result to S3 under:

    s3://<UPLOAD_BUCKET>/court-a/<date>/<game-uuid>/<date>_<uuid>_<ANGLE>.mp4

where ANGLE is one of FL, FR, NL, NR. A background poller
(`poll_and_register_batch_jobs` in video_processing.py) then registers those
videos into the annotation tool's `video_metadata` table (angle LEFT/RIGHT).

Two failure modes leave games unregistered even though their S3 outputs exist:

  1. The live poller times out before the (sometimes 4+ hour) AWS Batch GPU
     queue drains, so it never sees the job SUCCEED.
  2. The backend process restarts and the in-flight poller thread is lost.

When that happens the annotation tool shows "No cloud videos available" and a
human has had to backfill by hand. This script is that backfill, automated and
idempotent: it scans S3 for finished `court-a/` outputs and registers anything
the annotation tool is still missing.

Angle selection mirrors the live poller:

    LEFT  side: FL if present, else NL  (full preferred, near fallback)
    RIGHT side: FR if present, else NR

Usage
-----
    # Dry run for a date (default — shows what WOULD be registered):
    python3 scripts/backfill_pending_videos.py --date 2026-06-10

    # Actually register for a date:
    python3 scripts/backfill_pending_videos.py --date 2026-06-10 --apply

    # Target a single game by its FULL Uball UUID (most reliable):
    python3 scripts/backfill_pending_videos.py --game <full-uuid> --apply

Notes on the truncated UUID
---------------------------
The S3 folder name is the TRUNCATED game UUID (first 4 hyphen groups), but
`register_video` matches on the FULL UUID. For the `--date` sweep this script
resolves the full UUID by fetching the games list from the Uball backend and
building a {truncated -> full} map (same approach as register_s3_videos.py).
If a truncated folder cannot be resolved to a full UUID it is logged as a
WARNING and skipped — re-run with `--game <full-uuid>` to register it directly.

Credentials are read from the environment / .env (UBALL_BACKEND_URL,
UBALL_AUTH_EMAIL, UBALL_AUTH_PASSWORD), exactly like main.py / UballClient.
"""

import argparse
import os
import sys

# Add parent directory to path for imports (same convention as other scripts/).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import requests
from dotenv import load_dotenv

from uball_client import UballClient

load_dotenv()

DEFAULT_BUCKET = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')

# Annotation-tool side per S3 angle suffix; full preferred, near fallback.
SIDE_BY_ANGLE = {'FL': 'LEFT', 'NL': 'LEFT', 'FR': 'RIGHT', 'NR': 'RIGHT'}
PREFERRED_ANGLE = {'LEFT': 'FL', 'RIGHT': 'FR'}
FALLBACK_ANGLE = {'LEFT': 'NL', 'RIGHT': 'NR'}
ALL_ANGLES = ('FL', 'FR', 'NL', 'NR')


def log(msg):
    print(msg, flush=True)


def truncate_uuid(full_uuid):
    """Return the first 4 hyphen groups of a UUID (the S3 folder form)."""
    if not full_uuid:
        return None
    return '-'.join(str(full_uuid).split('-')[:4])


def build_uball_client():
    """Instantiate UballClient from env (raises if creds are missing)."""
    client = UballClient()
    if not client._ensure_authenticated():
        raise RuntimeError("Failed to authenticate with Uball backend")
    return client


def fetch_truncated_to_full_map(uball):
    """Fetch all games from Uball and map {truncated_uuid -> full_uuid}.

    Mirrors scripts/register_s3_videos.py so the --date sweep can turn the
    truncated S3 folder name back into the full UUID register_video needs.
    """
    response = requests.get(
        f"{uball.backend_url}/api/games/",
        headers=uball._get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        games = data
    elif isinstance(data, dict) and 'games' in data:
        games = data['games']
    else:
        games = []

    mapping = {}
    for game in games:
        full_id = str(game.get('id', ''))
        truncated = truncate_uuid(full_id)
        if truncated:
            mapping[truncated] = full_id
    return mapping


def list_game_folders(s3, bucket, date):
    """List the game-UUID prefixes under court-a/<date>/ for a date."""
    prefix = f"court-a/{date}/"
    paginator = s3.get_paginator('list_objects_v2')
    folders = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].rstrip('/').split('/')[-1]
            if folder:
                folders.append(folder)
    return folders


def list_angle_objects(s3, bucket, date, game_folder):
    """Return {angle -> {'s3_key', 'filename', 'size'}} for one game folder."""
    prefix = f"court-a/{date}/{game_folder}/"
    paginator = s3.get_paginator('list_objects_v2')
    by_angle = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.mp4'):
                continue
            filename = key.split('/')[-1]
            for angle in ALL_ANGLES:
                if filename.endswith(f"_{angle}.mp4"):
                    by_angle[angle] = {
                        's3_key': key,
                        'filename': filename,
                        'size': obj.get('Size', 0),
                    }
                    break
    return by_angle


def choose_sides(by_angle):
    """Pick the LEFT and RIGHT object per side (full preferred, near fallback).

    Returns {'LEFT': obj_or_None, 'RIGHT': obj_or_None}.
    """
    chosen = {}
    for side in ('LEFT', 'RIGHT'):
        chosen[side] = by_angle.get(PREFERRED_ANGLE[side]) or by_angle.get(FALLBACK_ANGLE[side])
    return chosen


def existing_sides(uball, full_game_id):
    """Return the set of sides ({'LEFT','RIGHT'}) already registered for a game."""
    try:
        videos = uball.get_videos_for_game(full_game_id)
    except Exception as e:
        log(f"    WARN: could not read existing videos for {full_game_id}: {e}")
        return set()
    sides = set()
    for v in videos or []:
        angle = str(v.get('angle', '')).upper()
        if angle in ('LEFT', 'RIGHT'):
            sides.add(angle)
    return sides


def register_game(uball, full_game_id, chosen, apply_changes):
    """Register the chosen LEFT/RIGHT videos for one game. Returns counts dict.

    Idempotent: skips sides already present in the annotation tool. Never raises
    — a per-game failure is logged and counted, not propagated.
    """
    counts = {'registered': 0, 'skipped': 0, 'errors': 0}
    already = existing_sides(uball, full_game_id) if apply_changes else set()

    for side in ('LEFT', 'RIGHT'):
        obj = chosen.get(side)
        if not obj:
            log(f"    {side}: no S3 object present — skipping")
            continue

        if side in already:
            log(f"    {side}: already registered in Uball — skipping")
            counts['skipped'] += 1
            continue

        if not apply_changes:
            log(f"    {side}: WOULD register {obj['filename']} "
                f"(s3://.../{obj['s3_key']}, {obj['size']} bytes)")
            continue

        try:
            result = uball.register_video(
                game_id=full_game_id,
                s3_key=obj['s3_key'],
                angle=side,
                filename=obj['filename'],
                duration=0.0,  # Frontend backfills the real duration
                file_size=obj.get('size'),
            )
            if result:
                log(f"    {side}: registered {obj['filename']} "
                    f"(video_id={result.get('id', 'ok')})")
                counts['registered'] += 1
            else:
                log(f"    {side}: FAILED to register {obj['filename']} (no result)")
                counts['errors'] += 1
        except Exception as e:
            log(f"    {side}: ERROR registering {obj['filename']}: {e}")
            counts['errors'] += 1

    return counts


def run_for_game(s3, uball, bucket, date, game_folder, full_game_id, apply_changes):
    """Process a single game folder. Wrapped so one failure can't abort the run."""
    log(f"\nGame folder: {game_folder}  ->  game_id: {full_game_id or '(unresolved)'}")

    if not full_game_id:
        log(f"  WARNING: could not resolve full UUID for truncated folder "
            f"'{game_folder}'. Re-run with --game <full-uuid> to register it.")
        return {'registered': 0, 'skipped': 0, 'errors': 0, 'unresolved': 1}

    try:
        by_angle = list_angle_objects(s3, bucket, date, game_folder)
    except Exception as e:
        log(f"  ERROR listing S3 objects for {game_folder}: {e}")
        return {'registered': 0, 'skipped': 0, 'errors': 1}

    if not by_angle:
        log(f"  No *_FL/_FR/_NL/_NR.mp4 objects found — nothing to backfill")
        return {'registered': 0, 'skipped': 0, 'errors': 0}

    log(f"  Angles present in S3: {', '.join(sorted(by_angle))}")
    chosen = choose_sides(by_angle)
    return register_game(uball, full_game_id, chosen, apply_changes)


def find_game_folder_for_full_id(s3, bucket, date, full_game_id):
    """For --game on a known date, find the S3 folder matching the full UUID."""
    truncated = truncate_uuid(full_game_id)
    folders = list_game_folders(s3, bucket, date)
    if truncated in folders:
        return truncated
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing video_metadata registrations from S3 "
                    "(safety net for the batch-poller registration gap)."
    )
    parser.add_argument('--date', help='Date to sweep, YYYY-MM-DD (e.g. 2026-06-10)')
    parser.add_argument('--game', help='Full Uball game UUID to target directly')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET,
                        help=f'S3 upload bucket (default: {DEFAULT_BUCKET})')
    parser.add_argument('--apply', action='store_true',
                        help='Actually register videos (default: dry run)')
    args = parser.parse_args()

    if not args.date and not args.game:
        parser.error("provide --date YYYY-MM-DD and/or --game <full-uuid>")

    mode = 'APPLY' if args.apply else 'DRY RUN'
    log("=" * 64)
    log(f"Backfill pending videos  [{mode}]")
    log(f"  bucket: {args.bucket}")
    if args.date:
        log(f"  date:   {args.date}")
    if args.game:
        log(f"  game:   {args.game}")
    log("=" * 64)

    s3 = boto3.client('s3')

    try:
        uball = build_uball_client()
        log(f"Uball client ready (backend: {uball.backend_url})")
    except Exception as e:
        log(f"ERROR: could not initialize Uball client: {e}")
        return 1

    totals = {'registered': 0, 'skipped': 0, 'errors': 0, 'unresolved': 0}

    def add(counts):
        for k in totals:
            totals[k] += counts.get(k, 0)

    # --- Single game by full UUID (most reliable path) ---------------------
    if args.game:
        full_game_id = args.game
        # Prefer the explicit date's folder; otherwise derive truncated folder.
        game_folder = None
        if args.date:
            try:
                game_folder = find_game_folder_for_full_id(s3, args.bucket, args.date, full_game_id)
            except Exception as e:
                log(f"WARN: could not list folders for {args.date}: {e}")
        if not game_folder:
            game_folder = truncate_uuid(full_game_id)

        # When no date is given we still need a date to locate the S3 path.
        if not args.date:
            log("NOTE: --game without --date cannot locate the S3 folder "
                "(court-a/<date>/...). Provide --date to register from S3.")
            log("Summary: nothing to do (no date for S3 lookup)")
            return 0

        add(run_for_game(s3, uball, args.bucket, args.date, game_folder,
                         full_game_id, args.apply))

    # --- Date sweep --------------------------------------------------------
    elif args.date:
        try:
            truncated_map = fetch_truncated_to_full_map(uball)
            log(f"Resolved {len(truncated_map)} games from Uball backend")
        except Exception as e:
            log(f"WARN: could not fetch games for UUID resolution: {e}")
            truncated_map = {}

        try:
            folders = list_game_folders(s3, args.bucket, args.date)
        except Exception as e:
            log(f"ERROR: could not list S3 folders for {args.date}: {e}")
            return 1

        log(f"Found {len(folders)} game folder(s) under court-a/{args.date}/")
        for game_folder in folders:
            full_game_id = truncated_map.get(game_folder, '')
            add(run_for_game(s3, uball, args.bucket, args.date, game_folder,
                             full_game_id, args.apply))

    # --- Summary -----------------------------------------------------------
    log("\n" + "=" * 64)
    log("SUMMARY")
    log("=" * 64)
    verb = 'Registered' if args.apply else 'Would register'
    log(f"  {verb}: {totals['registered']}")
    log(f"  Skipped (already present): {totals['skipped']}")
    log(f"  Unresolved folders: {totals['unresolved']}")
    log(f"  Errors: {totals['errors']}")
    if not args.apply:
        log("\n(dry run — re-run with --apply to register)")

    return 0 if totals['errors'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
