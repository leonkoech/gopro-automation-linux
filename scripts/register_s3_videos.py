#!/usr/bin/env python3
"""
Script to register S3 videos in Uball backend.

Scans court-a/ S3 prefix for FL/FR videos and registers them
in the Uball video_metadata table.
"""

import boto3
import os
import sys
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from uball_client import UballClient

def get_partial_uuid(full_uuid):
    """Get first 4 segments of UUID for matching."""
    if full_uuid:
        parts = full_uuid.split('-')[:4]
        return '-'.join(parts)
    return None


def main():
    # Initialize
    print("Initializing...")
    s3 = boto3.client('s3')
    bucket = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')

    try:
        uball = UballClient()
        print(f"✓ Uball client initialized")
        print(f"  Backend URL: {uball.backend_url}")
    except Exception as e:
        print(f"✗ Failed to initialize Uball client: {e}")
        return 1

    # First, fetch all games from Uball to build partial -> full UUID mapping
    print("\nFetching games from Uball backend...")
    import requests

    # Authenticate
    if not uball._ensure_authenticated():
        print("✗ Failed to authenticate with Uball")
        return 1

    # Get all games
    try:
        response = requests.get(
            f"{uball.backend_url}/api/games/",
            headers=uball._get_headers(),
            timeout=30
        )
        if response.status_code == 200:
            games_data = response.json()
            # Handle both list and paginated response
            if isinstance(games_data, list):
                games = games_data
            elif isinstance(games_data, dict) and 'games' in games_data:
                games = games_data['games']
            else:
                games = []
        else:
            print(f"✗ Failed to fetch games: {response.status_code}")
            return 1
    except Exception as e:
        print(f"✗ Error fetching games: {e}")
        return 1

    print(f"Found {len(games)} games in Uball backend")

    # Build mapping: partial_uuid -> full game info
    game_map = {}
    for game in games:
        full_id = str(game.get('id', ''))
        partial_id = get_partial_uuid(full_id)
        if partial_id:
            game_map[partial_id] = {
                'full_id': full_id,
                'date': game.get('date'),
                'firebase_game_id': game.get('firebase_game_id')
            }

    print(f"Built mapping for {len(game_map)} games\n")

    # Scan S3 for FL/FR videos
    print(f"\nScanning s3://{bucket}/court-a/ for FL/FR videos...")

    paginator = s3.get_paginator('list_objects_v2')
    videos = []

    for page in paginator.paginate(Bucket=bucket, Prefix='court-a/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.mp4') and obj['Size'] > 1000:
                # Only FL and FR angles
                if '_FL.mp4' in key or '_FR.mp4' in key:
                    videos.append({
                        'key': key,
                        'size': obj['Size'],
                        'filename': key.split('/')[-1]
                    })

    print(f"Found {len(videos)} FL/FR videos to register\n")

    # Process each video
    registered = 0
    skipped = 0
    errors = []

    for v in videos:
        key = v['key']
        parts = key.split('/')

        if len(parts) < 4:
            print(f"SKIP (bad path): {key}")
            skipped += 1
            continue

        date = parts[1]
        game_folder = parts[2]  # Partial uball_game_id (first 4 UUID segments)
        filename = parts[3]

        # Skip unknown games
        if 'unknown' in game_folder or 'game1' in game_folder:
            print(f"SKIP (unknown): {key}")
            skipped += 1
            continue

        # Determine angle
        angle_code = 'FL' if '_FL.mp4' in filename else 'FR'
        uball_angle = 'LEFT' if angle_code == 'FL' else 'RIGHT'

        # Look up full game ID from partial
        game_info = game_map.get(game_folder)
        if not game_info:
            print(f"SKIP (game not found in Uball): {key}")
            print(f"  Partial ID: {game_folder}")
            skipped += 1
            continue

        full_game_id = game_info['full_id']

        print(f"Registering: {filename}")
        print(f"  S3 key: {key}")
        print(f"  Game: {game_folder} -> {full_game_id}")
        print(f"  Angle: {uball_angle}")

        try:
            # Use full game ID for registration
            result = uball.register_video(
                game_id=full_game_id,
                s3_key=key,
                angle=uball_angle,
                filename=filename,
                file_size=v['size']
            )

            if result:
                print(f"  ✓ Registered: video_id={result.get('id', 'ok')}")
                registered += 1
            else:
                print(f"  ✗ Failed (no result returned)")
                errors.append({'key': key, 'error': 'No result'})
        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors.append({'key': key, 'error': str(e)})

        print()

    # Summary
    print("=" * 50)
    print(f"SUMMARY")
    print("=" * 50)
    print(f"Total videos found: {len(videos)}")
    print(f"Successfully registered: {registered}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {len(errors)}")

    if errors:
        print("\nFailed videos:")
        for e in errors:
            print(f"  - {e['key']}: {e['error']}")

    return 0 if len(errors) == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
