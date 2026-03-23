#!/bin/bash
# AWS Batch job definition inline command for ffmpeg-extract-transcode
#
# This script is embedded as the container command in the AWS Batch job definition.
# To update the job definition, register a new revision with this as the command.
#
# Environment variables (set via containerOverrides):
#   CHAPTERS_JSON          - JSON array of S3 keys for chapter files
#   CHAPTER_DURATIONS_JSON - JSON array of durations (seconds) for each chapter
#   BUCKET                 - S3 bucket name
#   OFFSET_SECONDS         - Seek position in concatenated chapters
#   DURATION_SECONDS       - Duration to extract
#   ADD_BUFFER_SECONDS     - Buffer to add to duration (default: 30)
#   OUTPUT_S3_KEY          - S3 key for output file
#   GAME_ID                - Game identifier for tracking
#   ANGLE                  - Camera angle code (FL, FR, NL, NR)
#
# Key changes from v1:
#   - concat.txt now includes 'duration' directives so the concat demuxer can
#     calculate chapter boundaries for accurate seeking over HTTP presigned URLs
#   - -hwaccel cuda moved before -i (was misplaced after -i in v1)
#   - Uses output-level seeking (-ss after -i) for reliable seeking with concat
#     demuxer. Input-level seeking with concat over HTTP can silently fail for
#     large offsets, causing the video to start from the wrong position.

set -e

echo '=== BATCH-ONLY EXTRACT+TRANSCODE v2 (h264_nvenc) ==='
echo "Chapters: $CHAPTERS_JSON"
echo "Durations: $CHAPTER_DURATIONS_JSON"
echo "Bucket: $BUCKET"
echo "Offset: ${OFFSET_SECONDS}s, Duration: ${DURATION_SECONDS}s (+${ADD_BUFFER_SECONDS:-30}s buffer)"
echo "Output: $OUTPUT_S3_KEY"
echo "Game: $GAME_ID | Angle: $ANGLE"
echo ''

echo '=== Step 1: Check GPU ==='
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv
echo ''

echo '=== Step 2: Install dependencies ==='
apt-get update -qq
apt-get install -y -qq --no-install-recommends wget python3-pip bc jq > /dev/null
pip3 install -q awscli
echo ''

echo '=== Step 3: Install FFmpeg (Jellyfin with NVENC) ==='
wget -q https://repo.jellyfin.org/files/ffmpeg/ubuntu/latest-6.x/amd64/jellyfin-ffmpeg6_6.0.1-8-jammy_amd64.deb -O /tmp/ffmpeg.deb
apt-get install -y -qq /tmp/ffmpeg.deb > /dev/null
rm /tmp/ffmpeg.deb
ln -sf /usr/lib/jellyfin-ffmpeg/ffmpeg /usr/local/bin/ffmpeg
ln -sf /usr/lib/jellyfin-ffmpeg/ffprobe /usr/local/bin/ffprobe
echo "FFmpeg: $(ffmpeg -version 2>&1 | head -1)"
echo ''

echo '=== Step 4: Generate presigned URLs and build concat file with durations ==='
mkdir -p /tmp/work
cd /tmp/work

CHAPTER_COUNT=$(echo "$CHAPTERS_JSON" | jq -r 'length')
echo "Processing $CHAPTER_COUNT chapters..."

for i in $(seq 0 $((CHAPTER_COUNT - 1))); do
    CHAPTER_KEY=$(echo "$CHAPTERS_JSON" | jq -r ".[$i]")
    CHAPTER_DUR=$(echo "$CHAPTER_DURATIONS_JSON" | jq -r ".[$i]" 2>/dev/null || echo "0")
    echo "  Chapter $((i+1)): $CHAPTER_KEY (duration: ${CHAPTER_DUR}s)"

    PRESIGNED_URL=$(aws s3 presign "s3://$BUCKET/$CHAPTER_KEY" --expires-in 7200)

    echo "file '$PRESIGNED_URL'" >> concat.txt
    # Add duration directive so concat demuxer can seek across chapters over HTTP
    if [ "$CHAPTER_DUR" != "0" ] && [ "$CHAPTER_DUR" != "null" ] && [ -n "$CHAPTER_DUR" ]; then
        echo "duration $CHAPTER_DUR" >> concat.txt
    fi
done

echo "Concat file ready with $CHAPTER_COUNT entries (with durations)"
echo ''

echo '=== Step 5: Calculate duration ==='
BUFFER=${ADD_BUFFER_SECONDS:-30}
TOTAL_DURATION=$(echo "$DURATION_SECONDS + $BUFFER" | bc)
echo "Total duration: $TOTAL_DURATION seconds (${DURATION_SECONDS}s + ${BUFFER}s buffer)"
echo ''

echo '=== Step 6: Extract + Transcode (output-level seeking with NVENC) ==='
START_ENC=$(date +%s)

# Use output-level seeking (-ss after -i) for reliable seeking with concat demuxer
# over HTTP presigned URLs. Input-level seeking can silently fail for large offsets.
# -hwaccel cuda must be before -i for proper GPU-accelerated HEVC decoding.
ffmpeg -y \
    -hwaccel cuda -hwaccel_output_format cuda \
    -f concat -safe 0 -protocol_whitelist file,http,https,tcp,tls,crypto \
    -i concat.txt \
    -ss "$OFFSET_SECONDS" \
    -t "$TOTAL_DURATION" \
    -vf scale_cuda=-2:1080 \
    -c:v h264_nvenc -preset p4 -rc vbr -cq 23 \
    -c:a aac -b:a 128k \
    -movflags +faststart /tmp/work/output.mp4 2>&1 | tail -30

END_ENC=$(date +%s)
ENC_TIME=$((END_ENC - START_ENC))
OUT_SIZE=$(stat -c%s /tmp/work/output.mp4)
OUT_MB=$((OUT_SIZE / 1048576))
echo "Encoded: ${OUT_MB}MB in ${ENC_TIME}s"
echo ''

echo '=== Step 7: Upload to S3 ==='
START_UL=$(date +%s)
aws s3 cp /tmp/work/output.mp4 "s3://$BUCKET/$OUTPUT_S3_KEY" --only-show-errors
END_UL=$(date +%s)
UL_TIME=$((END_UL - START_UL))
echo "Uploaded: ${OUT_MB}MB in ${UL_TIME}s"
echo ''

echo '=== Step 8: Cleanup ==='
rm -rf /tmp/work
echo ''

echo '=== COMPLETE ==='
TOTAL=$((END_UL - START_ENC))
echo "Total time: ${TOTAL}s (Encode: ${ENC_TIME}s, Upload: ${UL_TIME}s)"
echo "Output: ${OUT_MB}MB -> s3://$BUCKET/$OUTPUT_S3_KEY"
aws s3 ls "s3://$BUCKET/$OUTPUT_S3_KEY"
