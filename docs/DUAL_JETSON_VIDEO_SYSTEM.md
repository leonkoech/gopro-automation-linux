# Dual-Jetson GoPro Video Processing System

## Overview

This system manages 4 GoPro cameras across 2 Jetson Nano devices for recording basketball games at a sports facility. Videos are extracted based on game timestamps, optionally compressed to 1080p, uploaded to S3, and registered with the Uball annotation tool.

---

## Architecture

### Jetson Configuration

| Jetson | DNS | IP (Tailscale) | JETSON_ID | Hostname | Cameras | Angles |
|--------|-----|----------------|-----------|----------|---------|--------|
| Jetson-1 | jetson-1.uai.tech | 100.87.190.71 | jetson-1 | jetson-nano-002 | Backbone 2, far Side - left | FR, NL |
| Jetson-2 | jetson-2.uai.tech | 100.106.30.98 | jetson-2 | JETSON-NANO-001 | Backbone 1, Near Side - left | FL, NR |

### Camera Angle Mapping

```
Court Layout (bird's eye view):

    END A (Jetson-1)          END B (Jetson-2)
    ┌─────────────────────────────────────────┐
    │  NL ←─────────────────────────────→ NR  │  Near Side
    │                                         │
    │               COURT                     │
    │                                         │
    │  FL ←─────────────────────────────→ FR  │  Far Side
    └─────────────────────────────────────────┘

Jetson-1 (END A): FR (far right), NL (near left)
Jetson-2 (END B): FL (far left), NR (near right)
```

### Video Registration (Uball Annotation Tool)
- **FL** → Registered as **LEFT** angle
- **FR** → Registered as **RIGHT** angle
- **NL, NR** → Uploaded to S3 but NOT registered (used as supplementary angles)

---

## SSH Access

```bash
SSH_KEY="/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/id_rsa"

# Jetson-1
ssh -i "$SSH_KEY" developer@100.87.190.71

# Jetson-2
ssh -i "$SSH_KEY" developer@100.106.30.98

# Sudo password: tigerballlily
```

---

## Environment Variables (.env)

### Jetson-1 (.env)
```bash
JETSON_ID=jetson-1
CAMERA_ANGLE_MAP={"Backbone 2": "FR", "far Side - left": "NL", "GoPro-4d38": "NL", "GoPro-857e": "FR"}
```

### Jetson-2 (.env)
```bash
JETSON_ID=jetson-2
CAMERA_ANGLE_MAP={"Backbone 1": "FL", "Near Side - left": "NR"}
```

---

## Key File Locations

### On Jetsons
| Path | Purpose |
|------|---------|
| `~/Development/gopro-automation-linux/` | Main codebase |
| `~/gopro_videos/segments/` | Raw chapter storage |
| `~/gopro_videos/extracted/` | Game-extracted clips |
| `/tmp/gopro.log` | Application logs |

### Local Development
| Path | Purpose |
|------|---------|
| `gopro-automation-linux/` | Backend (Flask API) |
| `gopro-automation-wb/` | Frontend (Next.js) |

---

## Core Components

### 1. Recording Flow (`main.py`)

#### Start Recording
```python
@app.route('/api/gopro/<gopro_id>/record/start', methods=['POST'])
def start_recording(gopro_id):
    # Get camera info and angle code
    camera_name = camera_info.get('ap_ssid', '')
    angle_code = _get_angle_code_from_name(camera_name)

    # Generate session ID with angle code
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_id = f"{gopro_id}_{angle_code}_{timestamp}"

    # Create Firebase recording session
    firebase_service.register_recording_start(
        session_id=session_id,
        camera_serial=gopro_id,
        camera_name=camera_name,
        jetson_id=jetson_id,
        angle_code=angle_code
    )
```

#### Angle Code Detection
```python
def _get_angle_code_from_name(camera_name: str) -> str:
    """Extract angle code from camera name using CAMERA_ANGLE_MAP."""
    import json

    angle_map_str = os.getenv('CAMERA_ANGLE_MAP', '{}')
    try:
        angle_map = json.loads(angle_map_str)
        if camera_name in angle_map:
            return angle_map[camera_name]
    except:
        pass

    # Fallback: check if angle code is in the name
    for code in ['FL', 'FR', 'NL', 'NR']:
        if code in camera_name.upper():
            return code

    return 'UNK'  # Unknown
```

### 2. Video Processing (`video_processing.py`)

#### Extract Game Segment
```python
def extract_segment_for_game(
    chapter_path: str,
    chapter_start_time: datetime,
    game_start_time: datetime,
    game_end_time: datetime,
    output_path: str,
    compress_to_1080p: bool = False
) -> Optional[str]:
    """
    Extract the portion of a video chapter that overlaps with game time.
    Optionally compress to 1080p if source is higher resolution.
    """
    # Calculate overlap
    overlap_start = max(chapter_start_time, game_start_time)
    overlap_end = min(chapter_end_time, game_end_time)

    # Calculate seek position within chapter
    start_offset = (overlap_start - chapter_start_time).total_seconds()
    duration = (overlap_end - overlap_start).total_seconds()

    # Build FFmpeg command
    cmd = ['ffmpeg', '-y', '-ss', str(start_offset), '-i', chapter_path]

    if compress_to_1080p:
        # Check if compression needed
        height = get_video_height(chapter_path)
        if height > 1080:
            cmd.extend([
                '-vf', 'scale=-2:1080',
                '-c:v', 'libx264',
                '-preset', 'slow',
                '-crf', '18',
                '-c:a', 'aac', '-b:a', '192k'
            ])
        else:
            cmd.extend(['-c', 'copy'])  # No re-encode needed
    else:
        cmd.extend(['-c', 'copy'])

    cmd.extend(['-t', str(duration), output_path])

    subprocess.run(cmd, check=True)
    return output_path
```

#### Get Video Resolution
```python
def get_video_height(video_path: str) -> int:
    """Get the height (vertical resolution) of a video file."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=height',
        '-of', 'csv=p=0',
        video_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return int(result.stdout.strip())
```

### 3. Compression Settings (`videoupload.py`)

```python
def compress_to_1080p(self, input_path: str, output_path: str = None, crf: int = 18) -> str:
    """
    Compress video to 1080p using high-quality settings.

    Settings:
    - Scale: -2:1080 (maintain aspect ratio, height 1080)
    - Codec: libx264 (H.264)
    - Preset: slow (better compression, more time)
    - CRF: 18 (visually lossless)
    - Audio: AAC 192kbps
    """
    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_1080p{ext}"

    cmd = [
        'ffmpeg', '-i', input_path,
        '-vf', 'scale=-2:1080',
        '-c:v', 'libx264',
        '-preset', 'slow',
        '-crf', str(crf),
        '-c:a', 'aac', '-b:a', '192k',
        '-movflags', '+faststart',
        '-y', output_path
    ]

    subprocess.run(cmd, check=True)
    return output_path
```

### 4. S3 Upload (`videoupload.py`)

```python
def upload_to_s3(self, local_path: str, s3_key: str) -> str:
    """Upload video to S3 and return the URL."""
    bucket = 'uball-videos-production'

    self.s3_client.upload_file(
        local_path,
        bucket,
        s3_key,
        ExtraArgs={'ContentType': 'video/mp4'}
    )

    return f"https://{bucket}.s3.amazonaws.com/{s3_key}"
```

### 5. Uball Registration (`videoupload.py`)

```python
def register_with_uball(self, game_id: str, s3_url: str, angle: str) -> bool:
    """
    Register video with Uball annotation tool.
    Only FL (LEFT) and FR (RIGHT) are registered.
    """
    if angle not in ['FL', 'FR']:
        logger.info(f"Skipping Uball registration for angle {angle}")
        return True

    uball_angle = 'LEFT' if angle == 'FL' else 'RIGHT'

    response = requests.post(
        f"{UBALL_API_URL}/api/videos/register",
        json={
            'gameId': game_id,
            'videoUrl': s3_url,
            'angle': uball_angle
        }
    )
    return response.status_code == 200
```

---

## API Endpoints

### Recording Control
```bash
# Start recording on a camera
curl -X POST "https://jetson-2.uai.tech/api/gopros/{gopro_id}/record/start"

# Stop recording
curl -X POST "https://jetson-2.uai.tech/api/gopros/{gopro_id}/record/stop"

# List connected GoPros
curl "https://jetson-2.uai.tech/api/gopros"

# Start all cameras
curl -X POST "https://jetson-2.uai.tech/api/gopros/start-all"

# Stop all cameras
curl -X POST "https://jetson-2.uai.tech/api/gopros/stop-all"
```

### Video Processing
```bash
# Preview extraction (shows overlapping sessions/chapters)
curl -X POST "https://jetson-2.uai.tech/api/games/{game_id}/preview-extraction" \
  -H "Content-Type: application/json" \
  -d '{"game_start_time": "2026-01-28T13:20:00Z", "game_end_time": "2026-01-28T13:50:00Z"}'

# Start async video processing
curl -X POST "https://jetson-2.uai.tech/api/games/process-videos/async" \
  -H "Content-Type: application/json" \
  -d '{
    "firebase_game_id": "GAME_ID",
    "game_start_time": "2026-01-28T13:20:00Z",
    "game_end_time": "2026-01-28T13:50:00Z",
    "team1_name": "Team A",
    "team2_name": "Team B"
  }'

# Check job status
curl "https://jetson-2.uai.tech/api/games/process-videos/{job_id}/status"

# List all jobs
curl "https://jetson-2.uai.tech/api/games/process-videos/jobs"
```

---

## Frontend Integration

### Dual-Jetson Processing (`page.tsx`)

```typescript
const JETSON_CONFIGS = [
  { url: 'https://jetson-1.uai.tech', name: 'jetson-1' },
  { url: 'https://jetson-2.uai.tech', name: 'jetson-2' }
];

const handleProcessVideos = async (game: Game) => {
  const jobs: ProcessingJobInfo[] = [];

  // Start processing on BOTH Jetsons in parallel
  const startPromises = JETSON_CONFIGS.map(async ({ url, name }) => {
    try {
      const result = await JetsonAPI.startVideoProcessingAsync(url, game.id, {
        game_start_time: game.createdAt,
        game_end_time: game.endedAt,
        team1_name: game.team1.name,
        team2_name: game.team2.name
      });
      if (result.success && result.job_id) {
        return { jobId: result.job_id, jetsonUrl: url, jetsonName: name };
      }
    } catch (error) {
      console.warn(`Failed to start on ${name}:`, error);
    }
    return null;
  });

  const results = await Promise.all(startPromises);
  results.forEach(r => r && jobs.push(r));

  // Store jobs for progress tracking
  localStorage.setItem('activeVideoJobs', JSON.stringify({
    gameId: game.id,
    jobs,
    startedAt: new Date().toISOString()
  }));
};
```

### Preview Extraction (Merge from Both Jetsons)

```typescript
const handlePreviewExtraction = async (game: Game) => {
  // Call BOTH Jetsons in parallel
  const previewPromises = JETSON_CONFIGS.map(async ({ url, name }) => {
    try {
      const result = await JetsonAPI.previewGameExtraction(url, game.id);
      return { ...result, source_jetson: name };
    } catch {
      return null;
    }
  });

  const results = await Promise.all(previewPromises);

  // Merge sessions - dedupe by session_id, prefer local sessions
  const sessionMap = new Map();
  results.filter(Boolean).forEach(result => {
    result.sessions.forEach(session => {
      const existing = sessionMap.get(session.session_id);
      if (!existing || (session.is_local && !existing.is_local)) {
        sessionMap.set(session.session_id, session);
      }
    });
  });

  return Array.from(sessionMap.values());
};
```

---

## Deployment Commands

```bash
SSH_KEY="/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/id_rsa"

# Deploy to Jetson-1
ssh -i "$SSH_KEY" developer@100.87.190.71 \
  "cd ~/Development/gopro-automation-linux && git pull origin main && \
   echo 'tigerballlily' | sudo -S systemctl restart gopro-controller.service"

# Deploy to Jetson-2
ssh -i "$SSH_KEY" developer@100.106.30.98 \
  "cd ~/Development/gopro-automation-linux && git pull origin main && \
   echo 'tigerballlily' | sudo -S systemctl restart gopro-controller.service"

# Check service status
ssh -i "$SSH_KEY" developer@100.87.190.71 "systemctl status gopro-controller.service"

# View logs
ssh -i "$SSH_KEY" developer@100.87.190.71 "tail -f /tmp/gopro.log"
```

---

## Complete Processing Flow

```
1. START RECORDING
   ├── Frontend: Click "Start All" on dashboard
   ├── Backend: Each Jetson starts its connected GoPros
   └── Firebase: Creates recording-sessions with jetsonId, angleCode

2. PLAY GAME
   ├── Basketball Timer UI: Creates game with start time
   ├── Score events recorded
   └── Game ends with end time

3. STOP RECORDING
   ├── Frontend: Click "Stop All"
   ├── Backend: Downloads chapters from GoPros to segments/
   └── Firebase: Updates recording-sessions with totalChapters

4. SYNC TO ANNOTATION TOOL
   ├── Frontend: Click "Sync" button
   └── Game synced to Uball annotation tool

5. PREVIEW EXTRACTION
   ├── Frontend: Click "Process Videos" → Preview
   ├── Calls BOTH Jetsons for overlapping sessions
   └── Shows which chapters will be extracted

6. PROCESS VIDEOS
   ├── Frontend: Sends to BOTH Jetsons in parallel
   ├── Each Jetson processes its LOCAL sessions only
   ├── Extract game segments (compress if >1080p)
   ├── Upload to S3 (uball-videos-production)
   └── Register FL/FR with Uball annotation tool

7. ANNOTATION
   └── Videos available in Uball annotation tool with LEFT/RIGHT angles
```

---

## Troubleshooting

### Check Jetson Connectivity
```bash
# Ping via Tailscale
ping 100.87.190.71  # Jetson-1
ping 100.106.30.98  # Jetson-2

# Test API
curl https://jetson-1.uai.tech/api/health
curl https://jetson-2.uai.tech/api/health
```

### Check Camera Detection
```bash
# On Jetson, check detected cameras
curl localhost:5000/api/gopros

# Check CAMERA_ANGLE_MAP
grep CAMERA_ANGLE_MAP ~/Development/gopro-automation-linux/.env
```

### Check Segments
```bash
# List segments on Jetson
ls -la ~/gopro_videos/segments/

# Check segment sizes
du -sh ~/gopro_videos/segments/*
```

### View Processing Logs
```bash
# Real-time logs
tail -f /tmp/gopro.log | grep -E "(ProcessGame|Extract|Upload)"

# Search for errors
grep -i error /tmp/gopro.log | tail -20
```

### Firebase Queries
```python
# Check recording sessions for a Jetson
from firebase_admin import firestore
db = firestore.client()
sessions = db.collection('recording-sessions')\
    .where('jetsonId', '==', 'jetson-2')\
    .order_by('startTime', direction=firestore.Query.DESCENDING)\
    .limit(10).get()
for s in sessions:
    print(s.id, s.to_dict().get('angleCode'))
```

---

## Auto-Compression During Extraction

The system automatically checks video resolution before extraction and compresses to 1080p if needed:

### How It Works

1. **Resolution Check**: Uses `ffprobe` to get video height before extraction
2. **Compression Decision**: If height > 1080p, applies compression during extraction
3. **Stream Copy**: If 1080p or lower, uses fast stream copy (no re-encoding)

### Compression Settings

```python
# Applied when video height > 1080p
if needs_compression:
    cmd.extend([
        '-vf', 'scale=-2:1080',  # Scale to 1080p, maintain aspect ratio
        '-c:v', 'libx264',       # H.264 codec
        '-preset', 'slow',       # Better quality, longer encode time
        '-crf', '18',            # Visually lossless quality
        '-c:a', 'aac', '-b:a', '192k',  # High-quality audio
        '-movflags', '+faststart',       # Enable streaming
    ])
else:
    cmd.extend(['-c', 'copy'])  # No re-encoding (fast)
```

### Impact on Processing Time

| Source Resolution | Processing Mode | ~Time for 30min video |
|-------------------|-----------------|----------------------|
| 1080p | Stream copy | ~1-2 minutes |
| 4K (2160p) | Compress to 1080p | ~15-30 minutes |

### Relevant Code

- `video_processing.py:_get_video_height()` - Get video resolution
- `video_processing.py:_needs_compression()` - Check if compression needed
- `video_processing.py:_extract_from_single_file()` - Single chapter extraction with compression
- `video_processing.py:_extract_from_multiple_files()` - Multi-chapter extraction with compression

---

## Known Issues & Solutions

### 1. "UNK" Angle Code
**Cause**: Camera name not in CAMERA_ANGLE_MAP
**Fix**: Update .env with correct camera names from `curl /api/gopros`

### 2. Preview Shows 0 Chapters
**Cause**: Frontend only calling one Jetson
**Fix**: Frontend calls BOTH Jetsons and merges results

### 3. Large Video Files (4K)
**Cause**: GoPro recording in 4K instead of 1080p
**Fix**: Extraction now auto-compresses to 1080p if height > 1080

### 4. Processing Fails on One Jetson
**Cause**: No local sessions for that Jetson
**Fix**: Backend returns `skipped: true` instead of error

---

## Version History

| Date | Changes |
|------|---------|
| 2026-01-28 | Fixed dual-Jetson processing, graceful skip for no sessions |
| 2026-01-29 | Added auto-compression to 1080p during extraction |
| 2026-01-29 | Fixed preview to call both Jetsons and merge results |
