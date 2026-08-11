#!/usr/bin/env python3
"""
1. Update Black Team -> Team Music in Firebase game
2. Find and cancel running AWS Batch job for this game
3. Ensure FL session is stopped
"""

import boto3
import firebase_admin
from firebase_admin import credentials, firestore

CRED_PATH = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"
cred = credentials.Certificate(CRED_PATH)
app = firebase_admin.initialize_app(cred)
db = firestore.client()

GAME_ID = 'u8VVIVFhBnh8H88j9Np0'
SESSION_ID = 'yRVGwCD3s2sKIPqO86tb'

# Step 1: Update team name in Firebase
print("STEP 1: Update team name Black Team -> Team Music")
game_ref = db.collection('basketball-games').document(GAME_ID)
game_data = game_ref.get().to_dict()
left = game_data.get('leftTeam', {})
right = game_data.get('rightTeam', {})
print(f"  Current: {left.get('name')} vs {right.get('name')}")

# Check which team is "Black Team"
if 'black' in left.get('name', '').lower():
    left['name'] = 'Team Music'
    game_ref.update({'leftTeam': left})
    print(f"  Updated leftTeam -> Team Music")
elif 'black' in right.get('name', '').lower():
    right['name'] = 'Team Music'
    game_ref.update({'rightTeam': right})
    print(f"  Updated rightTeam -> Team Music")

updated = game_ref.get().to_dict()
print(f"  Now: {updated['leftTeam']['name']} vs {updated['rightTeam']['name']}")

# Step 2: Find and cancel running Batch jobs for this game
print("\nSTEP 2: Cancel AWS Batch jobs")
batch = boto3.client('batch', region_name='us-east-1')

# Check all active job queues for running/submitted jobs
for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
    for queue in ['gpu-transcode-queue', 'gpu-transcode-queue-large']:
        try:
            jobs = batch.list_jobs(jobQueue=queue, jobStatus=status)
            for job in jobs.get('jobSummaryList', []):
                job_name = job.get('jobName', '')
                job_id = job.get('jobId', '')
                # Check if this job is related to our game (FL angle)
                if 'FL' in job_name:
                    print(f"  Found: {job_name} ({job_id}) - {status} in {queue}")
                    # Get job details to check game ID
                    details = batch.describe_jobs(jobs=[job_id])
                    for d in details.get('jobs', []):
                        env = {e['name']: e['value'] for e in d.get('container', {}).get('environment', [])}
                        if env.get('GAME_ID', '') and ('7921b0a2' in env.get('GAME_ID', '') or env.get('OUTPUT_S3_KEY', '').find('7921b0a2') >= 0):
                            print(f"    -> This is our game! Cancelling...")
                            batch.cancel_job(jobId=job_id, reason='Reprocessing with correct team name')
                            print(f"    -> Cancelled: {job_id}")
                        else:
                            print(f"    -> Different game ({env.get('GAME_ID', '?')}), skipping")
        except Exception as e:
            pass  # Queue might not exist

# Also try to terminate any running jobs
print("  Checking for running jobs to terminate...")
for queue in ['gpu-transcode-queue', 'gpu-transcode-queue-large']:
    try:
        jobs = batch.list_jobs(jobQueue=queue, jobStatus='RUNNING')
        for job in jobs.get('jobSummaryList', []):
            job_id = job.get('jobId', '')
            job_name = job.get('jobName', '')
            details = batch.describe_jobs(jobs=[job_id])
            for d in details.get('jobs', []):
                env = {e['name']: e['value'] for e in d.get('container', {}).get('environment', [])}
                if '7921b0a2' in env.get('GAME_ID', '') or '7921b0a2' in env.get('OUTPUT_S3_KEY', ''):
                    print(f"    -> Terminating running job: {job_name} ({job_id})")
                    batch.terminate_job(jobId=job_id, reason='Reprocessing with correct team name')
                    print(f"    -> Terminated")
    except Exception as e:
        pass

# Step 3: Ensure session is stopped
print("\nSTEP 3: Ensure FL session is stopped")
ref = db.collection('recording-sessions').document(SESSION_ID)
doc = ref.get().to_dict()
current_status = doc.get('status')
print(f"  Current status: {current_status}")

if current_status != 'stopped':
    # Also clean processedGames entry for this game if it exists
    processed = doc.get('processedGames', [])
    new_processed = [pg for pg in processed if pg.get('firebaseGameId') != GAME_ID]
    ref.update({'status': 'stopped', 'processedGames': new_processed})
    print(f"  Set to stopped, processedGames: {len(processed)} -> {len(new_processed)}")
else:
    print(f"  Already stopped")

# Also delete the FL output from S3 if it exists
print("\nSTEP 4: Clean up any FL output on S3")
s3 = boto3.client('s3', region_name='us-east-1')
BAD_S3_KEY = 'court-a/2026-04-02/7921b0a2-6983-4e76-8b44/2026-04-02_7921b0a2-6983-4e76-8b44_FL.mp4'
try:
    s3.head_object(Bucket='uball-videos-production', Key=BAD_S3_KEY)
    s3.delete_object(Bucket='uball-videos-production', Key=BAD_S3_KEY)
    print(f"  Deleted FL output from S3")
except:
    print(f"  FL output already deleted from S3")

firebase_admin.delete_app(app)
print("\nDone! Ready to retrigger pipeline.")
