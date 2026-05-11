#!/usr/bin/env python3
"""
Investigate why Team Music vs Ronselli Ballers has different durations per angle.
Find the game, sessions, and pipeline run details.
"""

import firebase_admin
from firebase_admin import credentials, firestore

CRED_PATH = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"
cred = credentials.Certificate(CRED_PATH)
app = firebase_admin.initialize_app(cred)
db = firestore.client()

# Find the game
print("=" * 80)
print("FINDING GAME: Team Music vs Ronselli Ballers")
print("=" * 80)

games_ref = db.collection('basketball-games')
all_games = games_ref.where('createdAt', '>=', '2026-04-02T00:00:00.000Z').where('createdAt', '<=', '2026-04-03T00:00:00.000Z').stream()

game_id = None
for doc in all_games:
    data = doc.to_dict()
    left = data.get('leftTeam', {}).get('name', '?')
    right = data.get('rightTeam', {}).get('name', '?')
    created = data.get('createdAt', '?')
    ended = data.get('endedAt', '?')
    status = data.get('status', '?')
    print(f"\n  Game ID: {doc.id}")
    print(f"  Matchup: {left} vs {right}")
    print(f"  Status:  {status}")
    print(f"  Created: {created}")
    print(f"  Ended:   {ended}")
    if 'music' in left.lower() or 'music' in right.lower() or 'ronselli' in left.lower() or 'ronselli' in right.lower():
        game_id = doc.id
        print(f"  >>> THIS IS THE GAME <<<")

# Find Apr 2 sessions
print("\n" + "=" * 80)
print("APR 2 RECORDING SESSIONS")
print("=" * 80)

sessions_ref = db.collection('recording-sessions')
for jetson_id in ['jetson-1', 'jetson-2']:
    print(f"\n--- {jetson_id} ---")
    sessions = sessions_ref.where('jetsonId', '==', jetson_id).stream()
    for doc in sessions:
        data = doc.to_dict()
        started = data.get('startedAt', '')
        if not started or not started.startswith('2026-04-02'):
            continue
        status = data.get('status', '?')
        ended = data.get('endedAt', 'NOT ENDED')
        angle = data.get('angleCode', '?')
        camera = data.get('cameraName', '?')
        processed = data.get('processedGames', [])
        print(f"\n  Session ID: {doc.id}")
        print(f"  Camera:    {camera} ({angle})")
        print(f"  Status:    {status}")
        print(f"  Started:   {started}")
        print(f"  Ended:     {ended}")
        if processed:
            for pg in processed:
                print(f"  Processed: {pg.get('extractedFilename', '?')} -> {pg.get('s3Key', '?')}")

# Find pipeline runs for Apr 2
print("\n" + "=" * 80)
print("APR 2 PIPELINE RUNS")
print("=" * 80)

pipelines_ref = db.collection('pipeline-runs')
all_pipelines = pipelines_ref.stream()
for doc in all_pipelines:
    data = doc.to_dict()
    started = data.get('started_at', '')
    if not started or not (started.startswith('2026-04-02') or started.startswith('2026-04-03')):
        continue
    print(f"\n  Pipeline ID: {doc.id}")
    print(f"  Jetson: {data.get('jetson_id')}")
    print(f"  Status: {data.get('status')}")
    print(f"  Stage: {data.get('stage')}")
    print(f"  Started: {started}")
    print(f"  Recording window: {data.get('recording_start')} -> {data.get('recording_end')}")

    games = data.get('games', {})
    for gid, gdata in games.items():
        print(f"\n    Game: {gdata.get('video_name', '?')}")
        print(f"    Status: {gdata.get('status', '?')}")
        print(f"    Batch jobs: {gdata.get('batch_jobs', [])}")
        angles = gdata.get('angles_processed', {})
        for angle, ainfo in angles.items():
            print(f"      {angle}: status={ainfo.get('status', '?')}, s3_key={ainfo.get('s3_key', '?')}")

firebase_admin.delete_app(app)
