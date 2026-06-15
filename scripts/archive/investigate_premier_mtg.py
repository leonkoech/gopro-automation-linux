#!/usr/bin/env python3
"""
Investigate "Team Music vs Premier Mtg (C League)" duration mismatch.
Find the game, batch jobs, and check extraction params.
"""

import boto3
import firebase_admin
from firebase_admin import credentials, firestore
import json

CRED_PATH = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"
cred = credentials.Certificate(CRED_PATH)
app = firebase_admin.initialize_app(cred)
db = firestore.client()

# Find the game
print("=" * 80)
print("FINDING GAME: Premier Mtg (C League)")
print("=" * 80)

games_ref = db.collection('basketball-games')
# Check Apr 2-3 games
all_games = games_ref.where('createdAt', '>=', '2026-04-02T00:00:00.000Z').where('createdAt', '<=', '2026-04-04T00:00:00.000Z').stream()

game_id = None
for doc in all_games:
    data = doc.to_dict()
    left = data.get('leftTeam', {}).get('name', '?')
    right = data.get('rightTeam', {}).get('name', '?')
    if 'premier' in left.lower() or 'premier' in right.lower():
        game_id = doc.id
        created = data.get('createdAt', '?')
        ended = data.get('endedAt', '?')
        print(f"\n  Game ID: {doc.id}")
        print(f"  Matchup: {left} vs {right}")
        print(f"  Created: {created}")
        print(f"  Ended:   {ended}")
        print(f"  Status:  {data.get('status')}")

# Find pipeline runs that processed this game
print("\n" + "=" * 80)
print("PIPELINE RUNS WITH THIS GAME")
print("=" * 80)

pipelines_ref = db.collection('pipeline-runs')
all_pipelines = pipelines_ref.stream()
batch_job_ids = []
for doc in all_pipelines:
    data = doc.to_dict()
    games = data.get('games', {})
    for gid, gdata in games.items():
        if game_id and game_id in gid:
            print(f"\n  Pipeline: {doc.id} ({data.get('jetson_id')})")
            print(f"  Game: {gdata.get('video_name')}")
            print(f"  Status: {gdata.get('status')}")
            print(f"  Batch jobs: {gdata.get('batch_jobs', [])}")
            batch_job_ids.extend(gdata.get('batch_jobs', []))
            angles = gdata.get('angles_processed', {})
            for angle, ainfo in angles.items():
                print(f"    {angle}: {ainfo.get('status')} -> {ainfo.get('s3_key', '?')}")

firebase_admin.delete_app(app)

# Check batch job details
print("\n" + "=" * 80)
print("BATCH JOB DETAILS")
print("=" * 80)

batch = boto3.client('batch', region_name='us-east-1')

for job_id in batch_job_ids:
    try:
        details = batch.describe_jobs(jobs=[job_id])
        for d in details.get('jobs', []):
            env = {e['name']: e['value'] for e in d.get('container', {}).get('environment', [])}
            print(f"\n  Job: {d.get('jobName')} ({job_id})")
            print(f"  Status: {d.get('status')}")
            print(f"  Angle: {env.get('ANGLE')}")
            print(f"  Offset: {env.get('OFFSET_SECONDS')}s")
            print(f"  Duration: {env.get('DURATION_SECONDS')}s")
            print(f"  Buffer: {env.get('ADD_BUFFER_SECONDS')}s")
            print(f"  Chapters: {env.get('CHAPTERS_JSON')}")
            print(f"  Chapter durations: {env.get('CHAPTER_DURATIONS_JSON')}")
            print(f"  Output: {env.get('OUTPUT_S3_KEY')}")
    except Exception as e:
        print(f"  Error getting {job_id}: {e}")

print("\nDone.")
