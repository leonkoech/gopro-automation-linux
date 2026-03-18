"""
One-time fix: Set all old Jetson-1 sessions with s3Prefix to status='uploaded',
except the 2 Mar 17 recovery sessions which should remain 'stopped'.

This prevents the pipeline from picking up all 25 historical sessions
when only the 2 Mar 17 sessions need processing.
"""
import firebase_admin
from firebase_admin import credentials, firestore

# --- Init Firebase ---
cred = credentials.Certificate("uball-gopro-fleet-firebase-adminsdk.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

RECOVERY_IDS = {'atF7h1qrb34tEp2TpLho', 'KigTorelRgqRDsCdDiK3'}

q = (
    db.collection('recording-sessions')
    .where('jetsonId', '==', 'jetson-1')
    .where('status', '==', 'stopped')
)

docs = list(q.stream())
print(f"Found {len(docs)} stopped J1 sessions")

updated = 0
skipped_recovery = 0
skipped_no_prefix = 0

for doc in docs:
    data = doc.to_dict()
    if doc.id in RECOVERY_IDS:
        skipped_recovery += 1
        print(f"  SKIP (recovery): {doc.id}")
        continue
    if not data.get('s3Prefix'):
        skipped_no_prefix += 1
        print(f"  SKIP (no s3Prefix): {doc.id}")
        continue
    doc.reference.update({'status': 'uploaded'})
    updated += 1
    print(f"  UPDATED -> uploaded: {doc.id}")

print(f"\nDone: {updated} updated, {skipped_recovery} recovery skipped, {skipped_no_prefix} no-prefix skipped")
