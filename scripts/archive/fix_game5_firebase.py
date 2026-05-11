#!/usr/bin/env python3
"""
Set FR (jetson-1) and FL (jetson-2) sessions back to 'stopped'
so the pipeline will pick them up and process the newly fixed game.
"""

import firebase_admin
from firebase_admin import credentials, firestore

CRED_PATH = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"
cred = credentials.Certificate(CRED_PATH)
app = firebase_admin.initialize_app(cred)
db = firestore.client()

sessions_to_fix = {
    'URHYvV9pKA1O9GjDP5Or': 'Jetson-1 FR (Backbone 2)',
    'kZJutSGySipSA4NaU60i': 'Jetson-2 FL (Backbone 1)',
}

for session_id, label in sessions_to_fix.items():
    ref = db.collection('recording-sessions').document(session_id)
    doc = ref.get().to_dict()
    print(f"{label}")
    print(f"  Current status: {doc.get('status')}")

    ref.update({'status': 'stopped'})

    updated = ref.get().to_dict()
    print(f"  New status:     {updated.get('status')}")
    print()

firebase_admin.delete_app(app)
print("Done. FR and FL sessions are now 'stopped'. Pipeline will pick them up.")
