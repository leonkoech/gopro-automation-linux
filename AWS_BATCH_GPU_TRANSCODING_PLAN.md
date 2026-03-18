# AWS Batch GPU Transcoding Implementation Plan

## Executive Summary

**Goal:** Reduce video processing time from **2h 35min → 20-30 min** by offloading 4K→1080p transcoding to AWS GPU instances with NVENC hardware encoding.

**Solution:** AWS Batch with Spot g4dn.xlarge instances (~$0.05-0.10/game)

---

## Current Architecture (What Works Today)

### System Overview
- **2 Jetson Orin Nano devices** processing GoPro basketball game recordings
- **4 camera angles**: FL, FR, NL, NR (2 per Jetson)
- Videos stored in Firebase (metadata) + S3 (files)
- Annotation tool at Uball Backend for video review

### Jetson Configuration

| Jetson | IP (Tailscale) | JETSON_ID | Cameras/Angles |
|--------|----------------|-----------|----------------|
| Jetson-1 | 100.87.190.71 | jetson-1 | FR, NL |
| Jetson-2 | 100.106.30.98 | jetson-2 | FL, NR |

### Current Processing Flow (SLOW - 2h 35min)
```
Jetson:
  4K HEVC source (GoPro)
    → HW decode (hevc_nvv4l2dec)
    → CPU encode (libx264 ultrafast) [BOTTLENECK: ~1h 20min per angle]
    → Stream to S3 via boto3 multipart upload
```

### Why It's Slow
- **Jetson Orin Nano has NO hardware encoder (NVENC)**
- CPU-only libx264 encoding at ~0.75x realtime
- 48 min video × 2 angles = ~2h 35min total

### Current Code Locations

| File | Purpose |
|------|---------|
| `video_processing.py` | FFmpeg extraction, S3 streaming, main processing logic |
| `videoupload.py` | S3 upload service with boto3 |
| `main.py` | Flask API endpoints |
| `firebase_service.py` | Firebase Firestore operations |
| `uball_client.py` | Uball annotation tool API client |

### Key Functions in video_processing.py
- `_extract_from_single_file()` - Single chapter extraction
- `_extract_from_multiple_files()` - Multi-chapter concat + extraction
- `_stream_ffmpeg_to_s3()` - Pipes FFmpeg stdout to S3 multipart upload
- `process_game_videos()` - Main orchestration function (line ~700+)
- `extract_game_clip()` - Entry point for extraction (line ~340)

---

## New Architecture (FAST - 20-30 min)

### High-Level Flow
```
┌─────────────────────────────────────────────────────────────────────────┐
│  JETSON (minimal work - just extract 4K, no encode)                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ 4K HEVC source → Stream copy (-c copy) → Upload 4K to S3 "raw/"   │ │
│  │ Time: ~5 min extract + ~10 min upload = ~15 min                   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (S3 event trigger or API call)
┌─────────────────────────────────────────────────────────────────────────┐
│  AWS BATCH (GPU transcoding with NVENC)                                 │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Instance: g4dn.xlarge Spot (~$0.15/hr, NVIDIA T4 GPU)             │ │
│  │ Input: s3://uball-videos-production/raw/{game_id}/{angle}_4k.mp4  │ │
│  │ FFmpeg: -hwaccel cuda -c:v h264_nvenc (8x realtime)               │ │
│  │ Output: s3://uball-videos-production/{game_id}/{angle}.mp4        │ │
│  │ Time: ~6 min encode                                                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ Cleanup: Delete 4K source from "raw/" prefix                      │ │
│  │ Notify: Update Firebase, register in Uball                        │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘

Total Time: ~15-20 min (vs 2h 35min currently)
Cost: ~$0.05-0.10 per game
```

---

## AWS Infrastructure to Create

### 1. IAM Role for Batch Jobs
```json
{
  "Role": "BatchGPUTranscodeRole",
  "Policies": [
    "AmazonS3FullAccess (or scoped to uball-videos-production bucket)",
    "CloudWatchLogsFullAccess"
  ],
  "TrustRelationship": "ecs-tasks.amazonaws.com"
}
```

### 2. AWS Batch Compute Environment
```yaml
Name: gpu-transcode-spot-env
Type: MANAGED
ComputeResources:
  type: SPOT
  allocationStrategy: SPOT_CAPACITY_OPTIMIZED
  maxvCpus: 16
  instanceTypes:
    - g4dn.xlarge  # 1 GPU, 4 vCPU, 16GB RAM, ~$0.15/hr spot
  subnets: [your-subnet-ids]
  securityGroupIds: [your-sg-id]
  instanceRole: ecsInstanceRole
  spotIamFleetRole: AmazonEC2SpotFleetRole
```

### 3. AWS Batch Job Queue
```yaml
Name: gpu-transcode-queue
State: ENABLED
Priority: 1
ComputeEnvironments:
  - gpu-transcode-spot-env
```

### 4. AWS Batch Job Definition
```yaml
Name: ffmpeg-nvenc-transcode
Type: container
ContainerProperties:
  image: jrottenberg/ffmpeg:5.1-nvidia  # or custom image
  vcpus: 4
  memory: 14000
  resourceRequirements:
    - type: GPU
      value: "1"
  command:
    - "-hwaccel"
    - "cuda"
    - "-hwaccel_output_format"
    - "cuda"
    - "-i"
    - "Ref::input_s3_uri"
    - "-vf"
    - "scale_cuda=-2:1080"
    - "-c:v"
    - "h264_nvenc"
    - "-preset"
    - "p4"
    - "-rc"
    - "vbr"
    - "-cq"
    - "23"
    - "-c:a"
    - "aac"
    - "-b:a"
    - "128k"
    - "-movflags"
    - "+faststart"
    - "Ref::output_s3_uri"
  environment:
    - name: AWS_DEFAULT_REGION
      value: us-east-1
  jobRoleArn: arn:aws:iam::ACCOUNT:role/BatchGPUTranscodeRole
```

### 5. Docker Image (if custom needed)
```dockerfile
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
    ffmpeg \
    awscli \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# FFmpeg with NVENC support
# The nvidia/cuda base + ffmpeg package includes NVENC

ENTRYPOINT ["/bin/bash", "-c"]
```

---

## Code Changes Required

### 1. New File: `aws_batch_transcode.py`

```python
"""
AWS Batch GPU Transcoding Service

Submits 4K→1080p transcoding jobs to AWS Batch with NVENC hardware encoding.
"""

import boto3
import os
import time
from typing import Optional, Dict, Any
from logging_service import get_logger

logger = get_logger('gopro.aws_batch')

class AWSBatchTranscoder:
    def __init__(
        self,
        job_queue: str = 'gpu-transcode-queue',
        job_definition: str = 'ffmpeg-nvenc-transcode',
        region: str = 'us-east-1'
    ):
        self.batch_client = boto3.client('batch', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.bucket = os.getenv('UPLOAD_BUCKET', 'uball-videos-production')

    def submit_transcode_job(
        self,
        input_s3_key: str,
        output_s3_key: str,
        game_id: str,
        angle: str
    ) -> Dict[str, Any]:
        """
        Submit a transcoding job to AWS Batch.

        Args:
            input_s3_key: S3 key of 4K source (e.g., "raw/game123/NL_4k.mp4")
            output_s3_key: S3 key for 1080p output (e.g., "court-a/2026-01-29/game123/NL.mp4")
            game_id: Firebase game ID for tracking
            angle: Camera angle (FL, FR, NL, NR)

        Returns:
            Job submission response with jobId
        """
        input_uri = f"s3://{self.bucket}/{input_s3_key}"
        output_uri = f"s3://{self.bucket}/{output_s3_key}"

        job_name = f"transcode-{game_id[:8]}-{angle}-{int(time.time())}"

        response = self.batch_client.submit_job(
            jobName=job_name,
            jobQueue=self.job_queue,
            jobDefinition=self.job_definition,
            parameters={
                'input_s3_uri': input_uri,
                'output_s3_uri': output_uri,
            },
            containerOverrides={
                'environment': [
                    {'name': 'GAME_ID', 'value': game_id},
                    {'name': 'ANGLE', 'value': angle},
                    {'name': 'INPUT_KEY', 'value': input_s3_key},
                    {'name': 'OUTPUT_KEY', 'value': output_s3_key},
                ]
            },
            tags={
                'game_id': game_id,
                'angle': angle,
                'service': 'uball-video-processing'
            }
        )

        logger.info(f"Submitted Batch job {response['jobId']} for {angle}")
        return response

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a transcoding job."""
        response = self.batch_client.describe_jobs(jobs=[job_id])
        if response['jobs']:
            job = response['jobs'][0]
            return {
                'job_id': job_id,
                'status': job['status'],  # SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING, SUCCEEDED, FAILED
                'status_reason': job.get('statusReason', ''),
                'started_at': job.get('startedAt'),
                'stopped_at': job.get('stoppedAt'),
            }
        return {'job_id': job_id, 'status': 'NOT_FOUND'}

    def wait_for_job(self, job_id: str, timeout: int = 1800) -> Dict[str, Any]:
        """Wait for job to complete (polling)."""
        start = time.time()
        while time.time() - start < timeout:
            status = self.get_job_status(job_id)
            if status['status'] in ['SUCCEEDED', 'FAILED']:
                return status
            time.sleep(10)
        return {'job_id': job_id, 'status': 'TIMEOUT'}

    def delete_raw_file(self, s3_key: str):
        """Delete the 4K source file after successful transcode."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)
            logger.info(f"Deleted raw 4K file: {s3_key}")
        except Exception as e:
            logger.warning(f"Failed to delete {s3_key}: {e}")
```

### 2. Modify `video_processing.py`

Add new extraction mode for 4K stream copy (no encode):

```python
def _extract_4k_stream_copy(
    self,
    input_path: str,
    offset: float,
    duration: float,
    output_path: str
) -> Optional[str]:
    """
    Extract clip with stream copy (no re-encoding).
    Ultra-fast: just cuts the file at keyframes.
    Output is 4K, same as source.
    """
    logger.info(f"Extracting 4K with stream copy: {input_path}")
    logger.info(f"  Offset: {self._format_duration(offset)}, Duration: {self._format_duration(duration)}")

    cmd = [
        'ffmpeg', '-y',
        '-ss', str(offset),  # Input-level seek (fast)
        '-i', input_path,
        '-t', str(duration),
        '-c', 'copy',  # No re-encoding
        '-avoid_negative_ts', 'make_zero',
        output_path
    ]

    # Stream copy is very fast - 5 min timeout is plenty
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        logger.error(f"FFmpeg error: {result.stderr}")
        return None

    if os.path.exists(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"Extracted 4K: {output_path} ({size_mb:.1f} MB)")
        return output_path

    return None
```

### 3. New Processing Flow in `process_game_videos()`

```python
# Option to use AWS Batch GPU transcoding
use_aws_gpu = os.getenv('USE_AWS_GPU_TRANSCODE', 'false').lower() == 'true'

if use_aws_gpu:
    # FAST PATH: Extract 4K with stream copy, upload, trigger Batch
    from aws_batch_transcode import AWSBatchTranscoder
    batch_transcoder = AWSBatchTranscoder()

    # 1. Extract 4K with stream copy (fast, ~2-5 min)
    raw_s3_key = f"raw/{uball_game_id}/{angle_code}_4k.mp4"
    extracted_path = video_processor._extract_4k_stream_copy(...)

    # 2. Upload 4K to S3 "raw/" prefix (~5-10 min for 4GB)
    upload_service.upload_video_with_key(extracted_path, raw_s3_key)

    # 3. Submit Batch job for GPU transcoding
    final_s3_key = video_processor.generate_s3_key(location, game_date, angle_code, uball_game_id)
    job = batch_transcoder.submit_transcode_job(raw_s3_key, final_s3_key, firebase_game_id, angle_code)

    # 4. Store job ID for status tracking
    results['batch_jobs'].append({
        'job_id': job['jobId'],
        'angle': angle_code,
        'status': 'SUBMITTED'
    })

    # 5. Clean up local 4K file
    os.remove(extracted_path)
else:
    # SLOW PATH: Current Jetson CPU encoding (existing code)
    ...
```

---

## Environment Variables to Add

### On Jetsons (.env)
```bash
# Enable AWS Batch GPU transcoding
USE_AWS_GPU_TRANSCODE=true

# AWS Batch settings
AWS_BATCH_JOB_QUEUE=gpu-transcode-queue
AWS_BATCH_JOB_DEFINITION=ffmpeg-nvenc-transcode
```

### AWS Credentials (already in .env)
```bash
# AWS credentials stored in .env file on Jetsons (not in repo)
AWS_ACCESS_KEY_ID=<redacted>
AWS_SECRET_ACCESS_KEY=<redacted>
AWS_DEFAULT_REGION=us-east-1
```

---

## S3 Bucket Structure

```
uball-videos-production/
├── raw/                          # Temporary 4K uploads (deleted after transcode)
│   └── {game_uuid}/
│       ├── FL_4k.mp4
│       ├── FR_4k.mp4
│       ├── NL_4k.mp4
│       └── NR_4k.mp4
│
└── court-a/                      # Final 1080p outputs
    └── {date}/
        └── {game_uuid}/
            ├── {date}_{game_uuid}_FL.mp4
            ├── {date}_{game_uuid}_FR.mp4
            ├── {date}_{game_uuid}_NL.mp4
            └── {date}_{game_uuid}_NR.mp4
```

---

## Time & Cost Comparison

| Metric | Current (Jetson CPU) | New (AWS Batch GPU) |
|--------|---------------------|---------------------|
| Extract | 1h 20min (encode) | **5 min** (stream copy) |
| Upload | Included (streamed) | **10 min** (4K upload) |
| Transcode | N/A | **6 min** (NVENC 8x) |
| **Total per Jetson** | **1h 20min × 2** | **~20 min** |
| **Both Jetsons parallel** | Sequential | **Parallel** |
| **TOTAL** | **2h 35min** | **~20-25 min** |
| **Cost/game** | $0 | **~$0.10** |

---

## Implementation Steps

### Phase 1: AWS Infrastructure Setup
1. [ ] Create IAM role for Batch jobs
2. [ ] Create VPC/subnet if needed (or use default)
3. [ ] Create Batch Compute Environment (Spot g4dn.xlarge)
4. [ ] Create Batch Job Queue
5. [ ] Create/push Docker image with FFmpeg+NVENC
6. [ ] Create Batch Job Definition
7. [ ] Test manually with AWS CLI

### Phase 2: Backend Code Changes
1. [ ] Create `aws_batch_transcode.py`
2. [ ] Add `_extract_4k_stream_copy()` to VideoProcessor
3. [ ] Modify `process_game_videos()` with USE_AWS_GPU_TRANSCODE flag
4. [ ] Add Batch job status tracking endpoint
5. [ ] Add cleanup logic (delete raw 4K after success)
6. [ ] Update environment variables

### Phase 3: Frontend Changes (Optional)
1. [ ] Show "Uploading 4K..." then "GPU Transcoding..." stages
2. [ ] Poll Batch job status instead of Jetson job status
3. [ ] Handle multi-stage progress

### Phase 4: Testing & Deployment
1. [ ] Test with one game end-to-end
2. [ ] Verify 1080p output quality
3. [ ] Verify Uball registration works
4. [ ] Deploy to both Jetsons
5. [ ] Monitor costs

---

## FFmpeg Commands Reference

### Current (Jetson CPU - SLOW)
```bash
ffmpeg -ss 418 -c:v hevc_nvv4l2dec -i input_4k.mp4 \
  -t 2960 -vf scale=-2:1080 \
  -c:v libx264 -preset ultrafast -crf 23 \
  -c:a aac -b:a 128k \
  -movflags frag_keyframe+empty_moov \
  -f mp4 pipe:1
```

### New Step 1: Jetson Stream Copy (FAST)
```bash
ffmpeg -ss 418 -i input_4k.mp4 \
  -t 2960 -c copy \
  -avoid_negative_ts make_zero \
  output_4k.mp4
```

### New Step 2: AWS Batch NVENC (FAST)
```bash
ffmpeg -hwaccel cuda -hwaccel_output_format cuda \
  -i s3://bucket/raw/game/angle_4k.mp4 \
  -vf scale_cuda=-2:1080 \
  -c:v h264_nvenc -preset p4 -rc vbr -cq 23 \
  -c:a aac -b:a 128k \
  -movflags +faststart \
  s3://bucket/final/game/angle.mp4
```

---

## Credentials & Access

### AWS
- Access Key: `<stored in .env>`
- Region: `us-east-1`
- Bucket: `uball-videos-production`

### Firebase
- Credentials: `/home/developer/Development/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json`

### Uball Backend
- URL: `https://p01--uball-annotation-tool-backend--k7t2r7hvzsxg.code.run`
- Auth: `rohit@cellstrat.com` / `admin12345`

### SSH to Jetsons
```bash
SSH_KEY="/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/id_rsa"
# Jetson-1
ssh -i "$SSH_KEY" developer@100.87.190.71
# Jetson-2
ssh -i "$SSH_KEY" developer@100.106.30.98
# Password: tigerballlily
```

---

## Repositories

| Repo | Path | Purpose |
|------|------|---------|
| gopro-automation-linux | `/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux` | Jetson backend |
| gopro-automation-wb | `/Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-wb` | Frontend (Next.js) |
| Uball_annotation_tool-Backend | `/Users/rohitkale/Cellstrat/GitHub_Repositories/Uball_annotation_tool-Backend` | Annotation tool API |

---

## Notes & Gotchas

1. **Spot instances can be interrupted** — Batch handles retries automatically
2. **g4dn.xlarge has 1 GPU, 4 vCPU** — can encode 1 video at a time per instance
3. **NVENC preset p4** — balanced speed/quality (p1=fastest, p7=slowest)
4. **scale_cuda** — does scaling on GPU, faster than CPU scale filter
5. **S3 direct input/output** — FFmpeg can read/write S3 URIs directly with proper IAM
6. **4K raw files are ~4GB each** — ensure S3 lifecycle policy to clean up if jobs fail
7. **Job timeout** — set to 30 min max, NVENC should finish in ~6 min

---

## Open Questions for Implementation

1. Should we wait for Batch job completion before returning to frontend, or use async polling?
2. Should cleanup (delete 4K) happen in Batch job or in Jetson after job success?
3. Do we need CloudWatch alarms for failed jobs?
4. Should we add S3 lifecycle rule to auto-delete `raw/` after 24 hours as failsafe?

---

## WORKING CONFIGURATION (Updated 2026-02-04)

### Status: ✅ OPERATIONAL

AWS Batch GPU transcoding is now working with dual-queue setup for optimal storage handling.

---

### AWS Infrastructure (Deployed)

#### Compute Environments
```yaml
# Standard compute environment (30GB EBS - for files < 14GB)
Name: gpu-transcode-ondemand
Type: MANAGED (EC2)
State: ENABLED
Status: VALID
ComputeResources:
  type: EC2
  allocationStrategy: BEST_FIT
  minvCpus: 0
  maxvCpus: 8
  instanceTypes:
    - g4dn.xlarge  # NVIDIA T4 GPU, 4 vCPU, 16GB RAM
  instanceRole: arn:aws:iam::840102831548:instance-profile/ecsInstanceRole
  ec2Configuration:
    - imageType: ECS_AL2_NVIDIA
  # Default 30GB EBS root volume

---
# Large file compute environment (100GB EBS - for files >= 14GB)
Name: gpu-transcode-100gb
Type: MANAGED (EC2)
State: ENABLED
Status: VALID
ComputeResources:
  type: EC2
  allocationStrategy: BEST_FIT_PROGRESSIVE
  minvCpus: 0
  maxvCpus: 8
  instanceTypes:
    - g4dn.xlarge
  instanceRole: arn:aws:iam::840102831548:instance-profile/ecsInstanceRole
  launchTemplate:
    launchTemplateId: lt-01eed0837a69327db  # 100GB gp3 EBS
  ec2Configuration:
    - imageType: ECS_AL2_NVIDIA
```

#### Job Queues (Dual-Queue Setup)
```yaml
# Standard queue for files < 14GB
Name: gpu-transcode-queue
State: ENABLED
Status: VALID
Priority: 1
ComputeEnvironments:
  - gpu-transcode-ondemand  # 30GB EBS

---
# Large file queue for files >= 14GB
Name: gpu-transcode-queue-large
State: ENABLED
Status: VALID
Priority: 1
ComputeEnvironments:
  - gpu-transcode-100gb  # 100GB EBS
```

#### File Size Threshold
```python
# In aws_batch_transcode.py
LARGE_FILE_THRESHOLD = 14 * 1024 * 1024 * 1024  # 14GB

# Queue selection logic:
# - Files < 14GB  → gpu-transcode-queue (30GB EBS)
# - Files >= 14GB → gpu-transcode-queue-large (100GB EBS)
```

#### Job Definition (Working: Revision 17)
```yaml
Name: ffmpeg-nvenc-transcode
Revision: 17  # USE THIS VERSION - h264_nvenc GPU encoding
Type: container
ContainerProperties:
  image: nvidia/cuda:12.2.0-runtime-ubuntu22.04
  vcpus: 4
  memory: 14000
  resourceRequirements:
    - type: GPU
      value: "1"
  jobRoleArn: arn:aws:iam::840102831548:role/BatchGPUTranscodeRole
  executionRoleArn: arn:aws:iam::840102831548:role/BatchGPUTranscodeRole
  environment:
    - name: AWS_DEFAULT_REGION
      value: us-east-1
  logConfiguration:
    logDriver: awslogs
    options:
      awslogs-group: /aws/batch/gpu-transcode
      awslogs-region: us-east-1
      awslogs-stream-prefix: transcode
```

#### Job Definition Command (Revision 17 - h264_nvenc GPU)
The job downloads the 4K file first, encodes with NVENC GPU, then uploads.

```bash
# Flow:
# 1. Check GPU (nvidia-smi)
# 2. Install Jellyfin FFmpeg (has NVENC support)
# 3. Download 4K from S3 to /tmp/input_4k.mp4
# 4. Transcode with NVENC: 4K HEVC → 1080p H.264
# 5. Upload 1080p to S3
# 6. Cleanup temp files

# FFmpeg command used:
ffmpeg -y \
  -hwaccel cuda -hwaccel_output_format cuda \
  -i /tmp/input_4k.mp4 \
  -vf scale_cuda=-2:1080 \
  -c:v h264_nvenc -preset p4 -rc vbr -cq 23 \
  -c:a aac -b:a 128k \
  -movflags +faststart \
  /tmp/output_1080p.mp4
```

#### Encoding Specifications
| Setting | Value | Notes |
|---------|-------|-------|
| Video Codec | h264_nvenc | GPU encoding (NVIDIA T4) |
| Preset | p4 | Balanced speed/quality |
| Rate Control | vbr -cq 23 | Variable bitrate, quality 23 |
| Scale | scale_cuda=-2:1080 | GPU-accelerated scaling |
| Audio Codec | aac | Standard |
| Audio Bitrate | 128k | Standard quality |
| movflags | +faststart | For disk files |

---

### Issues Encountered & Solutions

#### Issue 1: Docker Image Entrypoint
**Problem:** `linuxserver/ffmpeg` and `jrottenberg/ffmpeg` have `ffmpeg` as ENTRYPOINT.
When passing `/bin/bash -c "..."`, FFmpeg interprets `/bin/bash` as output file.
```
Error: Unable to find a suitable output format for '/bin/bash'
```

**Solution:** Use `nvidia/cuda:12.2.0-runtime-ubuntu22.04` base image which has bash as default entrypoint, then install FFmpeg inside the container.

#### Issue 2: NVENC Not Available in Static FFmpeg Builds
**Problem:** BtbN's static FFmpeg builds can't dynamically load NVIDIA libraries.
```
Cannot load libnvidia-encode.so.1
The minimum required Nvidia driver for nvenc is 570.0 or newer
```

**Solution:** Install Jellyfin FFmpeg which is compiled with proper NVENC support:
```bash
wget https://repo.jellyfin.org/files/ffmpeg/ubuntu/latest-6.x/amd64/jellyfin-ffmpeg6_6.0.1-8-jammy_amd64.deb
apt-get install -y /tmp/ffmpeg.deb
```

#### Issue 3: HTTP Streaming Bottleneck
**Problem:** When using presigned URLs directly with FFmpeg (`ffmpeg -i "https://..."` ), encoding speed is limited to ~1x realtime due to network buffering.

**Solution:** Download the file first to local NVMe storage, then encode:
```bash
aws s3 cp "$INPUT_S3_URI" /tmp/input_4k.mp4
ffmpeg -hwaccel cuda ... -i /tmp/input_4k.mp4 ... /tmp/output.mp4
aws s3 cp /tmp/output.mp4 "$OUTPUT_S3_URI"
```

---

### Job Definition Version History

| Revision | Image | Status | Notes |
|----------|-------|--------|-------|
| 15 | nvidia/cuda:12.2.0-runtime-ubuntu22.04 | ✅ WORKING | Download-first, Jellyfin FFmpeg, full NVENC |
| 14 | nvidia/cuda:12.2.0-runtime-ubuntu22.04 | ✅ WORKING | HTTP streaming (slow ~1x realtime) |
| 13 | linuxserver/ffmpeg:latest | ❌ FAILED | Entrypoint issue |
| 12 | linuxserver/ffmpeg:latest | ❌ FAILED | NVIDIA_VISIBLE_DEVICES reserved error |
| 11 | xychelsea/ffmpeg-nvidia:latest | ❌ FAILED | Entrypoint issue |
| 10 | hiwaymedia/nvenc-docker:latest | ❌ FAILED | Image pull auth error |
| 8-9 | nvcr.io/nvidia/cuda + BtbN FFmpeg | ❌ FAILED | NVENC library not found |
| 5-7 | jrottenberg/ffmpeg:5.1-nvidia2004 | ❌ FAILED | Entrypoint issue |

---

### Performance Benchmarks

#### Test: 56-minute 4K HEVC video (5.5GB input)

| Metric | HTTP Streaming (v14) | Download-First (v15) |
|--------|---------------------|----------------------|
| Download | N/A (streamed) | ~2-3 min |
| Encode Speed | ~1x realtime | ~4-6x realtime |
| Encode Time | ~56 min | ~10-14 min |
| Upload | Piped | ~1-2 min |
| **Total** | **~60 min** | **~15-20 min** |

---

### Environment Variables (Production)

```bash
# On Jetsons (.env)
USE_AWS_GPU_TRANSCODE=true
AWS_BATCH_JOB_QUEUE=gpu-transcode-queue
AWS_BATCH_JOB_DEFINITION=ffmpeg-nvenc-transcode:15

# AWS Credentials (stored in .env, not in repo)
AWS_ACCESS_KEY_ID=<redacted>
AWS_SECRET_ACCESS_KEY=<redacted>
AWS_DEFAULT_REGION=us-east-1
```

---

### Code Integration

The integration is complete in these files:

| File | Function | Purpose |
|------|----------|---------|
| `aws_batch_transcode.py` | `AWSBatchTranscoder` | Submit/monitor Batch jobs |
| `video_processing.py:1100` | `process_game_videos()` | Main orchestration with `force_local_transcode` flag |
| `video_processing.py:1303` | AWS Batch integration | Uploads 4K, submits job, tracks status |
| `main.py:1717` | `_run_video_processing_job()` | API handler with `force_local_transcode` param |
| `main.py:3557` | `batch_transcode_jobs` | Job tracking dict |

#### How to Trigger

```python
# From API
POST /api/games/process-videos/async
{
  "firebase_game_id": "GAME_ID",
  "game_start_time": "2026-02-03T01:00:00Z",
  "game_end_time": "2026-02-03T02:00:00Z",
  "force_local_transcode": false  # Use AWS Batch (default)
}
```

---

### CloudWatch Logs

View job logs at:
```
Log Group: /aws/batch/gpu-transcode
Log Stream: transcode/default/{task-id}
```

Or via CLI:
```bash
aws logs get-log-events \
  --log-group-name "/aws/batch/gpu-transcode" \
  --log-stream-name "transcode/default/{task-id}"
```

---

### Storage Considerations

- **g4dn.xlarge** has 125GB NVMe SSD at `/tmp`
- Max input file size: ~15GB (with ~10GB headroom for output + overhead)
- For larger files: Would need to attach additional EBS volume via launch template

---

### Cost Estimate

| Resource | Rate | Usage | Cost/Game |
|----------|------|-------|-----------|
| g4dn.xlarge | $0.526/hr | ~20 min/angle × 4 angles | ~$0.70 |
| Data Transfer (S3→EC2) | $0 | Same region | $0 |
| Data Transfer (EC2→S3) | $0 | Same region | $0 |
| **Total** | | | **~$0.70/game** |

With Spot instances (~70% discount): **~$0.20/game**

---

*Document updated: 2026-02-03*
*AWS Batch GPU transcoding verified working with revision 15*
