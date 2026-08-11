#!/bin/bash
# extract_transcode.sh - Combined extraction and transcoding script for AWS Batch
#
# This script runs in an AWS Batch container with GPU (g4dn.xlarge) and:
# 1. Reads chapter S3 keys from CHAPTERS_JSON environment variable
# 2. Generates presigned URLs for each chapter
# 3. Creates FFmpeg concat file
# 4. Runs FFmpeg in single pass: seek + extract + transcode to 1080p
# 5. Uploads result directly to final S3 path (court-a/)
#
# Environment Variables Required:
#   CHAPTERS_JSON     - JSON array of chapter S3 keys (e.g., '["chapters/2026-01-20/ch1.mp4", "chapters/2026-01-20/ch2.mp4"]')
#   BUCKET            - S3 bucket name
#   OFFSET_SECONDS    - Seek position in concatenated chapters (float)
#   DURATION_SECONDS  - Duration to extract (float)
#   OUTPUT_S3_KEY     - Final 1080p output path (e.g., "court-a/2026-01-20/uuid/video.mp4")
#   GAME_ID           - Game identifier for logging
#   ANGLE             - Camera angle code (FL, FR, NL, NR) for logging
#   ADD_BUFFER_SECONDS - Optional buffer to add to duration (default: 30)
#
# Output:
#   Uploads 1080p MP4 to s3://${BUCKET}/${OUTPUT_S3_KEY}

set -e
set -o pipefail  # Ensure a failed ffmpeg (not tee) propagates through the pipe

echo "=============================================="
echo "Extract + Transcode Script (Batch-Only Pipeline)"
echo "=============================================="
echo "Start time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

# Validate required environment variables
if [ -z "$CHAPTERS_JSON" ]; then
    echo "ERROR: CHAPTERS_JSON environment variable is required"
    exit 1
fi

if [ -z "$BUCKET" ]; then
    echo "ERROR: BUCKET environment variable is required"
    exit 1
fi

if [ -z "$OFFSET_SECONDS" ]; then
    echo "ERROR: OFFSET_SECONDS environment variable is required"
    exit 1
fi

if [ -z "$DURATION_SECONDS" ]; then
    echo "ERROR: DURATION_SECONDS environment variable is required"
    exit 1
fi

if [ -z "$OUTPUT_S3_KEY" ]; then
    echo "ERROR: OUTPUT_S3_KEY environment variable is required"
    exit 1
fi

# Optional variables with defaults
GAME_ID="${GAME_ID:-unknown}"
ANGLE="${ANGLE:-UNK}"
ADD_BUFFER_SECONDS="${ADD_BUFFER_SECONDS:-30}"

echo "Configuration:"
echo "  BUCKET: $BUCKET"
echo "  OFFSET_SECONDS: $OFFSET_SECONDS"
echo "  DURATION_SECONDS: $DURATION_SECONDS"
echo "  ADD_BUFFER_SECONDS: $ADD_BUFFER_SECONDS"
echo "  OUTPUT_S3_KEY: $OUTPUT_S3_KEY"
echo "  GAME_ID: $GAME_ID"
echo "  ANGLE: $ANGLE"
echo ""

# Calculate total duration with buffer
TOTAL_DURATION=$(echo "$DURATION_SECONDS + $ADD_BUFFER_SECONDS" | bc)
echo "Total duration with buffer: $TOTAL_DURATION seconds"
echo ""

# Create working directory
WORK_DIR="/tmp/extract_transcode_$$"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"
echo "Working directory: $WORK_DIR"
echo ""

# Parse chapters JSON and generate presigned URLs
echo "Generating presigned URLs for chapters..."
CONCAT_FILE="$WORK_DIR/concat.txt"
> "$CONCAT_FILE"

# Parse JSON array using jq
CHAPTER_COUNT=$(echo "$CHAPTERS_JSON" | jq -r 'length')
echo "Number of chapters: $CHAPTER_COUNT"

for i in $(seq 0 $((CHAPTER_COUNT - 1))); do
    CHAPTER_KEY=$(echo "$CHAPTERS_JSON" | jq -r ".[$i]")
    echo "  Chapter $((i+1)): $CHAPTER_KEY"

    # Generate presigned URL (valid for 1 hour)
    PRESIGNED_URL=$(aws s3 presign "s3://$BUCKET/$CHAPTER_KEY" --expires-in 3600)

    if [ -z "$PRESIGNED_URL" ]; then
        echo "ERROR: Failed to generate presigned URL for $CHAPTER_KEY"
        exit 1
    fi

    # Write to concat file
    echo "file '$PRESIGNED_URL'" >> "$CONCAT_FILE"
done

echo ""
echo "Concat file created with $CHAPTER_COUNT entries"
echo ""

# Check for NVIDIA GPU
echo "Checking GPU availability..."
if nvidia-smi > /dev/null 2>&1; then
    echo "NVIDIA GPU detected:"
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader
    GPU_AVAILABLE=true
else
    echo "WARNING: No NVIDIA GPU detected, falling back to CPU encoding"
    GPU_AVAILABLE=false
fi
echo ""

# Output file
OUTPUT_FILE="$WORK_DIR/output.mp4"

# Run FFmpeg with GPU encoding if available
echo "Starting FFmpeg extraction + transcoding..."
echo "  Input: concat list with $CHAPTER_COUNT chapters"
echo "  Seek: $OFFSET_SECONDS seconds"
echo "  Duration: $TOTAL_DURATION seconds"
echo "  Output: 1080p H.264"
echo ""

# Disable set -e around the pipeline so we can read ffmpeg's real exit code
# (via PIPESTATUS) and print a diagnostic before exiting. Note: $? after a
# pipe reflects the LAST command (tee, always 0), so PIPESTATUS[0] is required
# to observe an ffmpeg failure (OOM, corrupt chapter, GPU fault).
set +e
if [ "$GPU_AVAILABLE" = true ]; then
    echo "Using NVIDIA NVENC GPU encoding..."
    ffmpeg -y \
        -ss "$OFFSET_SECONDS" \
        -f concat -safe 0 -protocol_whitelist file,http,https,tcp,tls,crypto \
        -i "$CONCAT_FILE" \
        -t "$TOTAL_DURATION" \
        -vf "scale=-2:1080" \
        -c:v h264_nvenc -preset p4 -cq 23 \
        -c:a aac -b:a 128k \
        -movflags +faststart \
        "$OUTPUT_FILE" 2>&1 | tee ffmpeg.log
    FFMPEG_EXIT=${PIPESTATUS[0]}
else
    echo "Using CPU encoding (libx264)..."
    ffmpeg -y \
        -ss "$OFFSET_SECONDS" \
        -f concat -safe 0 -protocol_whitelist file,http,https,tcp,tls,crypto \
        -i "$CONCAT_FILE" \
        -t "$TOTAL_DURATION" \
        -vf "scale=-2:1080" \
        -c:v libx264 -preset medium -crf 23 \
        -c:a aac -b:a 128k \
        -movflags +faststart \
        "$OUTPUT_FILE" 2>&1 | tee ffmpeg.log
    FFMPEG_EXIT=${PIPESTATUS[0]}
fi
set -e

if [ "$FFMPEG_EXIT" -ne 0 ]; then
    echo "ERROR: FFmpeg failed with exit code $FFMPEG_EXIT"
    echo "FFmpeg log:"
    cat ffmpeg.log
    exit 1
fi

echo ""
echo "FFmpeg completed successfully"

# Verify output file exists
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "ERROR: Output file not created"
    exit 1
fi

OUTPUT_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE")
OUTPUT_SIZE_MB=$(echo "scale=2; $OUTPUT_SIZE / 1048576" | bc)
echo "Output file size: ${OUTPUT_SIZE_MB} MB"
echo ""

# Sanity check: reject a zero/tiny output that would otherwise be treated as a
# successful transcode. A partial file from a crashed encode can be non-empty
# but truncated, so we also probe the actual playable duration below.
MIN_OUTPUT_BYTES=$((100 * 1024))  # 100 KiB floor; a real 1080p clip is far larger
if [ "$OUTPUT_SIZE" -lt "$MIN_OUTPUT_BYTES" ]; then
    echo "ERROR: Output file is only ${OUTPUT_SIZE} bytes (< ${MIN_OUTPUT_BYTES}); transcode is not valid"
    echo "FFmpeg log:"
    cat ffmpeg.log
    exit 1
fi

# Sanity check: probe the output duration with ffprobe. A failed/partial encode
# often yields an unreadable or ~0s file. Require the playable duration to be a
# meaningful fraction of what we asked ffmpeg to produce before declaring
# success — otherwise the caller may delete the only raw 4K source (footage loss).
echo "Verifying output with ffprobe..."
OUTPUT_DURATION=$(ffprobe -v error -show_entries format=duration \
    -of default=noprint_wrappers=1:nokey=1 "$OUTPUT_FILE" 2>/dev/null || echo "")

if [ -z "$OUTPUT_DURATION" ] || [ "$OUTPUT_DURATION" = "N/A" ]; then
    echo "ERROR: ffprobe could not read a duration from the output; file is corrupt or incomplete"
    echo "FFmpeg log:"
    cat ffmpeg.log
    exit 1
fi

echo "Output duration: ${OUTPUT_DURATION} seconds"

# Require at least 50% of the requested extract window (accounts for clips that
# run slightly short at the tail of the source, while still catching a transcode
# that died early). DURATION_SECONDS is the real content window (excludes buffer).
MIN_DURATION=$(echo "scale=2; $DURATION_SECONDS * 0.5" | bc)
DURATION_OK=$(echo "$OUTPUT_DURATION >= $MIN_DURATION" | bc)
if [ "$DURATION_OK" -ne 1 ]; then
    echo "ERROR: Output duration ${OUTPUT_DURATION}s is below the ${MIN_DURATION}s minimum (50% of requested ${DURATION_SECONDS}s)"
    echo "The transcode likely failed partway through; refusing to report success so the raw source is preserved."
    echo "FFmpeg log:"
    cat ffmpeg.log
    exit 1
fi

echo "Output validation passed"
echo ""

# Upload to S3
echo "Uploading to S3..."
echo "  Destination: s3://$BUCKET/$OUTPUT_S3_KEY"

aws s3 cp "$OUTPUT_FILE" "s3://$BUCKET/$OUTPUT_S3_KEY" \
    --content-type "video/mp4" \
    --metadata "game_id=$GAME_ID,angle=$ANGLE,pipeline=batch-only"

UPLOAD_EXIT=$?
if [ $UPLOAD_EXIT -ne 0 ]; then
    echo "ERROR: S3 upload failed with exit code $UPLOAD_EXIT"
    exit 1
fi

echo "Upload completed successfully"
echo ""

# Cleanup
echo "Cleaning up working directory..."
rm -rf "$WORK_DIR"

echo ""
echo "=============================================="
echo "Extract + Transcode Complete"
echo "=============================================="
echo "Output: s3://$BUCKET/$OUTPUT_S3_KEY"
echo "Size: ${OUTPUT_SIZE_MB} MB"
echo "End time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

exit 0
