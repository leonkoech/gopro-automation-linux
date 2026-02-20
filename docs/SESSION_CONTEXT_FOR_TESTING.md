# Game-Video Integration - Session Context for Testing

## Overview
This document captures all context from the implementation session for use in testing.

---

## SSH Access to Jetsons

### Jetson 1 (via jetson-2.uai.tech - names are swapped)
```bash
# SSH Key location
SSH_KEY="/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/id_rsa"

# SSH Command
ssh -o StrictHostKeyChecking=no -i $SSH_KEY developer@100.106.30.98

# Tailscale IP: 100.106.30.98
# Hostname: jetson-nano-001
# User: developer
# Sudo password: <SUDO_PASSWORD>
```

### Jetson 2 (offline during session)
```bash
# Tailscale IP: 100.87.190.71
# Hostname: jetson-nano-002
```

### Check Tailscale Status
```bash
tailscale status
```

---

## Deployment Commands

### Pull and Restart Service on Jetson
```bash
SSH_KEY="/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/id_rsa"

ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=no -i $SSH_KEY developer@100.106.30.98 \
  "cd ~/Development/gopro-automation-linux && \
   git pull origin main && \
   echo '<SUDO_PASSWORD>' | sudo -S systemctl restart gopro-controller.service && \
   sleep 3 && echo 'Restarted'"
```

### Deploy .env to Jetson
```bash
ssh -o StrictHostKeyChecking=no -i $SSH_KEY developer@100.106.30.98 "cat > ~/Development/gopro-automation-linux/.env << 'EOF'
FLASK_APP=main.py
FLASK_ENV=production

# AWS Credentials
AWS_ACCESS_KEY_ID=<YOUR_AWS_ACCESS_KEY>
AWS_SECRET_ACCESS_KEY=<YOUR_AWS_SECRET_KEY>

# Upload settings
UPLOAD_ENABLED=true
UPLOAD_LOCATION=default-location
UPLOAD_DEVICE_NAME=jetson-nano-01
UPLOAD_BUCKET=jetson-videos-uball
UPLOAD_REGION=us-east-1
DELETE_AFTER_UPLOAD=false

# Firebase Admin SDK settings
FIREBASE_CREDENTIALS_PATH=/home/developer/Development/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json
JETSON_ID=jetson-1
CAMERA_ANGLE_MAP={\"GoPro FL\": \"FL\", \"GoPro FR\": \"FR\", \"GoPro NL\": \"NL\", \"GoPro NR\": \"NR\"}

# Uball Backend settings
UBALL_BACKEND_URL=https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run
UBALL_AUTH_EMAIL=rohit@cellstrat.com
UBALL_AUTH_PASSWORD=<UBALL_PASSWORD>
EOF"
```

### Copy Firebase Credentials to Jetson
```bash
scp -o StrictHostKeyChecking=no -i $SSH_KEY \
  /Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json \
  developer@100.106.30.98:~/Development/gopro-automation-linux/
```

---

## API Endpoints (Jetson via https://jetson-2.uai.tech)

### System
```bash
# NTP Status
curl -s "https://jetson-2.uai.tech/api/system/ntp"

# Uball Status
curl -s "https://jetson-2.uai.tech/api/uball/status"
```

### Games
```bash
# List games from Firebase
curl -s "https://jetson-2.uai.tech/api/games/list"

# Sync game (auto-creates teams)
curl -s -X POST "https://jetson-2.uai.tech/api/games/sync" \
  -H "Content-Type: application/json" \
  -d '{"firebase_game_id":"FIREBASE_GAME_ID_HERE"}'

# Get game videos
curl -s "https://jetson-2.uai.tech/api/games/FIREBASE_GAME_ID/videos"

# Preview extraction (dry run)
curl -s "https://jetson-2.uai.tech/api/games/FIREBASE_GAME_ID/preview-extraction"

# Process videos (extract & upload)
curl -s -X POST "https://jetson-2.uai.tech/api/games/process-videos" \
  -H "Content-Type: application/json" \
  -d '{
    "firebase_game_id": "YOUR_FIREBASE_GAME_ID",
    "game_number": 1,
    "location": "court-a"
  }'
```

### Teams
```bash
# List teams
curl -s "https://jetson-2.uai.tech/api/uball/teams"
```

### Recording Sessions
```bash
# List sessions
curl -s "https://jetson-2.uai.tech/api/recording/sessions"
```

---

## Uball Backend API (Direct)

### Get Auth Token
```bash
TOKEN=$(curl -s -X POST "https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"rohit@cellstrat.com","password":"<UBALL_PASSWORD>"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

### List Games
```bash
curl -s "https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run/api/games/" \
  -H "Authorization: Bearer $TOKEN"
```

### Create Team
```bash
curl -s -X POST "https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run/api/teams/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"TEST TEAM"}'
```

### Update Game
```bash
curl -s -X PATCH "https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run/api/games/GAME_UUID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"video_name":"Team A vs Team B"}'
```

---

## Supabase Direct Access (For Testing/Cleanup)

### Credentials
```
SUPABASE_URL=https://mhbrsftxvxxtfgbajrlc.supabase.co
SUPABASE_ANON_KEY=<SUPABASE_ANON_KEY>
SUPABASE_SERVICE_ROLE_KEY=<SUPABASE_SERVICE_ROLE_KEY>
SUPABASE_JWT_SECRET=<SUPABASE_JWT_SECRET>
```

### Delete Game via Supabase REST API
```bash
curl -X DELETE "https://mhbrsftxvxxtfgbajrlc.supabase.co/rest/v1/games?id=eq.GAME_UUID" \
  -H "apikey: $SUPABASE_SERVICE_ROLE_KEY_HERE" \
  -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY_HERE"
```

### Delete Team via Supabase REST API
```bash
curl -X DELETE "https://mhbrsftxvxxtfgbajrlc.supabase.co/rest/v1/teams?id=eq.TEAM_UUID" \
  -H "apikey: $SUPABASE_SERVICE_ROLE_KEY_HERE" \
  -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY_HERE"
```

---

## Key Files Modified in This Session

### gopro-automation-linux (Jetson Backend)
- `main.py` - Added all game sync endpoints, auto team creation, video_name
- `uball_client.py` - Uball Backend API client with login, create_team, create_game
- `firebase_service.py` - Firebase Admin SDK wrapper
- `video_processing.py` - FFmpeg extraction logic
- `videoupload.py` - Added upload_video_with_key()

### Uball_annotation_tool-Backend
- `app/api/v1/endpoints/auth.py` - Added POST /api/auth/login endpoint
- `app/api/v1/endpoints/games.py` - Added firebase_game_id filter
- `app/schemas/auth.py` - Added LoginRequest, LoginResponse
- `app/schemas/game.py` - Added firebase_game_id, start_time, end_time, source fields

---

## Current State

### What's Working
1. Firebase service initialization
2. Uball Backend authentication via /api/auth/login
3. Auto-create teams from Firebase game data
4. Game sync with firebase_game_id linkage
5. video_name set as "TEAM1 vs TEAM2"

### Sample Video Segments on Jetson
```
Session: enxd43260ef4d38_20260120_195030
Date: 2026-01-20 19:50:30
Files:
  - chapter_001_GX018471.MP4 (9.51 GB)
  - chapter_002_GX028471.MP4 (9.25 GB)
  - chapter_003_GX038471.MP4 (8.55 GB)
Total: 27.30 GB
```

---

## Next Steps: Testing Plan

### 1. Create Test Script (test_game_video_flow.py)
Script should:
- Get video segment metadata (timestamps, duration)
- Fabricate 3 test games that span across segments
- Create games in Firebase with proper timestamps
- Unsync any previously synced games
- Clean up test data from Supabase (teams, games)

### 2. S3 Upload Structure (Updated)
```
{location}/{date}/game{N}-{UUID}/{date}_game{N}_{angle}.mp4

Example:
court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_FL.mp4
court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_FR.mp4
court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_NL.mp4
court-a/2026-01-20/game1-95efaeaa-8475-4db4/2026-01-20_game1_NR.mp4
```

### 3. Test Scenarios
- Game 1: Entirely within segment 1
- Game 2: Spans segment 1 and segment 2 (complex extraction)
- Game 3: Entirely within segment 2

### 4. Unsync Logic
Before testing, need to:
1. Remove uballGameId from Firebase game document
2. Delete game from Supabase games table
3. Delete associated teams from Supabase teams table

---

## Repository Locations

```
Local:
  gopro-automation-linux: /Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux
  Uball_annotation_tool-Backend: /Users/rohitkale/Cellstrat/GitHub_Repositories/Uball_annotation_tool-Backend

Jetson:
  gopro-automation-linux: /home/developer/Development/gopro-automation-linux
```

---

## GitHub Repos

- gopro-automation-linux: github.com:leonkoech/gopro-automation-linux.git
- Uball_annotation_tool-Backend: github.com:rohitmk523/Uball_annotation_tool-Backend.git

---

## Important Notes

1. **Jetson names are swapped**: jetson-2.uai.tech points to jetson-nano-001 (IP: 100.106.30.98)
2. **API paths**: Use `/api/` not `/api/v1/` and add trailing slashes
3. **Teams**: Each game sync creates NEW teams (even if same name exists) because rosters differ
4. **video_name**: Auto-set as "TEAM1 vs TEAM2" from Firebase leftTeam/rightTeam names
5. **Firebase credentials**: Must be copied to Jetson separately (not in git)
