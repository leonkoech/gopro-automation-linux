#!/usr/bin/env python3
"""
Create missing "Akatsuki vs Miracle Leaf" game for Apr 1.
- Game start: camera start time (00:04:37Z)
- Game end: 2 min before game 2 started (01:08:40Z)
- Set FR and FL sessions to stopped for reprocessing
"""

import firebase_admin
from firebase_admin import credentials, firestore

CRED_PATH = "/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"
cred = credentials.Certificate(CRED_PATH)
app = firebase_admin.initialize_app(cred)
db = firestore.client()

# Times
GAME_START = '2026-04-01T00:04:37.000Z'  # Camera start
GAME_END = '2026-04-01T01:08:40.000Z'    # ~2 min before game 2 (01:10:40)

print("STEP 1: Create basketball game document")
print("=" * 80)

game_data = {
    'createdAt': GAME_START,
    'endedAt': GAME_END,
    'status': 'completed',
    'finalPeriod': '4th',
    'createdBy': 'timmyshields31@gmail.com',
    'leftTeam': {
        'name': 'Akatsuki',
        'jerseyColor': '#1a1a1a',
        'jerseyColorName': 'Black',
        'finalScore': 0,
        'finalFouls': 0
    },
    'rightTeam': {
        'name': 'Miracle Leaf',
        'jerseyColor': '#9ca3af',
        'jerseyColorName': 'Grey',
        'finalScore': 0,
        'finalFouls': 0
    },
    'logs': [
        {
            'timestamp': GAME_START,
            'loggedBy': 'system-fix',
            'actionType': 'game_started',
            'team': None,
            'teamName': '',
            'payload': {},
            'gameTime': 600000,
            'gameTimeFormatted': '10:00',
            'shotClock': 24000,
            'period': '1st'
        },
        {
            'timestamp': GAME_END,
            'loggedBy': 'system-fix',
            'actionType': 'game_ended',
            'team': None,
            'teamName': '',
            'payload': {},
            'gameTime': 0,
            'gameTimeFormatted': '00:00',
            'shotClock': 0,
            'period': '4th'
        }
    ]
}

# Create the game
game_ref = db.collection('basketball-games').add(game_data)
new_game_id = game_ref[1].id
print(f"  Created game: {new_game_id}")
print(f"  Matchup: Akatsuki vs Miracle Leaf")
print(f"  Start:   {GAME_START}")
print(f"  End:     {GAME_END}")

# Verify
created_doc = db.collection('basketball-games').document(new_game_id).get().to_dict()
print(f"  Verified status: {created_doc.get('status')}")
print(f"  Verified endedAt: {created_doc.get('endedAt')}")

print(f"\nSTEP 2: Set FR and FL sessions to 'stopped'")
print("=" * 80)

sessions_to_fix = {
    'XrmCOgkPmI2yGAEXBiw7': 'Jetson-1 FR (Backbone 2)',
    'D8iTtit03PaVN53j8erK': 'Jetson-2 FL (Backbone 1)',
}

for session_id, label in sessions_to_fix.items():
    ref = db.collection('recording-sessions').document(session_id)
    doc = ref.get().to_dict()
    print(f"\n  {label}")
    print(f"  Current status: {doc.get('status')}")
    ref.update({'status': 'stopped'})
    updated = ref.get().to_dict()
    print(f"  New status:     {updated.get('status')}")

firebase_admin.delete_app(app)
print(f"\nDone!")
print(f"  New game ID: {new_game_id}")
print(f"  FR and FL sessions set to 'stopped'")
print(f"  Ready to trigger pipeline - it will find 3 games, skip the 2 already processed, and batch process Akatsuki vs Miracle Leaf")
