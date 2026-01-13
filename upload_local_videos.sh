#!/bin/bash
#
# Local Video Upload Script
# Uploads videos from local storage to AWS S3
# Organized by: {location}/{date}/{device_name} - {camera_name}.mp4
#
# Usage: ./upload_local_videos.sh [--dry-run] [--delete-after]
#

set -e

# Configuration - Edit these values
LOCATION="${UPLOAD_LOCATION:-default-location}"
DEVICE_NAME="${UPLOAD_DEVICE_NAME:-$(hostname)}"
S3_BUCKET="${UPLOAD_BUCKET:-jetson-videos-uai}"
AWS_REGION="${UPLOAD_REGION:-us-east-1}"
VIDEO_DIR="${VIDEO_DIR:-/home/developer/gopro_videos}"
TEMP_DIR="/tmp/gopro_uploads"
COMPRESS_TO_1080P="${COMPRESS_VIDEOS:-true}"

# Parse arguments
DRY_RUN=false
DELETE_AFTER=false
NO_COMPRESS=false
for arg in "$@"; do
    case $arg in
        --dry-run)
            DRY_RUN=true
            ;;
        --delete-after)
            DELETE_AFTER=true
            ;;
        --no-compress)
            NO_COMPRESS=true
            COMPRESS_TO_1080P=false
            ;;
        --help)
            echo "Usage: $0 [--dry-run] [--delete-after] [--no-compress]"
            echo ""
            echo "Options:"
            echo "  --dry-run       Show what would be uploaded without actually uploading"
            echo "  --delete-after  Delete videos from local storage after successful upload"
            echo "  --no-compress   Skip compression, upload original files (much faster)"
            echo ""
            echo "Environment variables:"
            echo "  UPLOAD_LOCATION    - Location name for S3 path (default: default-location)"
            echo "  UPLOAD_DEVICE_NAME - Device name for filename (default: hostname)"
            echo "  UPLOAD_BUCKET      - S3 bucket name (default: jetson-videos)"
            echo "  UPLOAD_REGION      - AWS region (default: us-east-1)"
            echo "  VIDEO_DIR          - Local video directory (default: /home/developer/gopro_videos)"
            echo "  COMPRESS_VIDEOS    - Compress to 1080p before upload (default: true)"
            exit 0
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check dependencies
check_dependencies() {
    local missing=()

    command -v aws >/dev/null 2>&1 || missing+=("awscli")

    if [ "$COMPRESS_TO_1080P" = "true" ]; then
        command -v ffmpeg >/dev/null 2>&1 || missing+=("ffmpeg")
    fi

    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing dependencies: ${missing[*]}"
        log_info "Install with: sudo apt install ${missing[*]}"
        exit 1
    fi
}

# Extract date from filename like gopro_GoPro-857e_20251219_202807.mp4
extract_date_from_filename() {
    local filename=$1
    # Extract YYYYMMDD from filename pattern gopro_*_YYYYMMDD_HHMMSS.mp4
    local date_part=$(echo "$filename" | grep -oP '\d{8}(?=_\d{6})' | head -1)
    if [ -n "$date_part" ]; then
        # Convert YYYYMMDD to YYYY-MM-DD
        echo "${date_part:0:4}-${date_part:4:2}-${date_part:6:2}"
    else
        # Fallback to today's date
        date "+%Y-%m-%d"
    fi
}

# Extract camera name from filename like gopro_GoPro-857e_20251219_202807.mp4
extract_camera_from_filename() {
    local filename=$1
    # Extract GoPro-XXXX from filename
    local camera=$(echo "$filename" | grep -oP 'GoPro-[a-zA-Z0-9]+' | head -1)
    if [ -n "$camera" ]; then
        echo "$camera"
    else
        echo "Unknown"
    fi
}

# Compress video to 1080p
compress_video() {
    local input=$1
    local output=$2

    log_info "Compressing to 1080p: $(basename "$input")"

    ffmpeg -i "$input" \
        -vf "scale=1920:1080:flags=lanczos" \
        -c:v libx264 \
        -preset fast \
        -crf 23 \
        -c:a aac \
        -b:a 128k \
        -movflags +faststart \
        -progress pipe:1 \
        -y \
        "$output" 2>&1 | grep -E "^(frame|fps|speed)=" | tail -1

    if [ $? -eq 0 ]; then
        local orig_size=$(stat -c%s "$input" 2>/dev/null || stat -f%z "$input")
        local new_size=$(stat -c%s "$output" 2>/dev/null || stat -f%z "$output")
        local ratio=$(echo "scale=1; $new_size * 100 / $orig_size" | bc)
        log_success "Compressed: ${ratio}% of original size"
        return 0
    else
        log_error "Compression failed"
        return 1
    fi
}

# Upload file to S3
upload_to_s3() {
    local file=$1
    local s3_key=$2

    if [ "$DRY_RUN" = "true" ]; then
        log_info "[DRY-RUN] Would upload: $s3_key"
        return 0
    fi

    log_info "Uploading to s3://$S3_BUCKET/$s3_key"

    aws s3 cp "$file" "s3://$S3_BUCKET/$s3_key" \
        --region "$AWS_REGION" \
        --content-type "video/mp4" \
        --only-show-errors

    if [ $? -eq 0 ]; then
        log_success "Uploaded: $s3_key"
        return 0
    else
        log_error "Upload failed: $s3_key"
        return 1
    fi
}

# Process a single video
process_video() {
    local video_path=$1
    local filename=$(basename "$video_path")

    # Extract metadata from filename
    local video_date=$(extract_date_from_filename "$filename")
    local camera_name=$(extract_camera_from_filename "$filename")

    local file_size=$(stat -c%s "$video_path" 2>/dev/null || stat -f%z "$video_path")
    local file_size_mb=$(echo "scale=2; $file_size / 1024 / 1024" | bc)

    log_info "Processing: $filename ($file_size_mb MB)"
    log_info "  Date: $video_date, Camera: $camera_name"

    # Build S3 key
    local s3_filename="${DEVICE_NAME} - ${camera_name} - ${filename}"
    local s3_key="${LOCATION}/${video_date}/${s3_filename}"

    if [ "$DRY_RUN" = "true" ]; then
        log_info "[DRY-RUN] Would upload: $s3_key"
        return 0
    fi

    local upload_file="$video_path"

    # Compress if enabled
    if [ "$COMPRESS_TO_1080P" = "true" ]; then
        mkdir -p "$TEMP_DIR"
        local compressed_file="$TEMP_DIR/compressed_$filename"
        if compress_video "$video_path" "$compressed_file"; then
            upload_file="$compressed_file"
        else
            log_warn "Compression failed, uploading original"
        fi
    fi

    # Upload to S3
    if upload_to_s3 "$upload_file" "$s3_key"; then
        # Delete local file if requested
        if [ "$DELETE_AFTER" = "true" ]; then
            log_info "Deleting local file: $filename"
            rm -f "$video_path"
            log_success "Deleted: $filename"
        fi
    fi

    # Cleanup temp files
    if [ "$COMPRESS_TO_1080P" = "true" ]; then
        rm -f "$TEMP_DIR/compressed_$filename"
    fi
}

# Main function
main() {
    echo ""
    echo "========================================"
    echo "  Local Video Upload Script"
    echo "========================================"
    echo ""

    log_info "Location: $LOCATION"
    log_info "Device: $DEVICE_NAME"
    log_info "S3 Bucket: $S3_BUCKET"
    log_info "Video Directory: $VIDEO_DIR"
    log_info "Compress: $COMPRESS_TO_1080P"

    if [ "$DRY_RUN" = "true" ]; then
        log_warn "DRY-RUN MODE - No actual uploads will be performed"
    fi

    if [ "$DELETE_AFTER" = "true" ]; then
        log_warn "DELETE_AFTER enabled - Videos will be deleted after upload"
    fi

    echo ""

    # Check dependencies
    check_dependencies

    # Check AWS credentials
    if ! aws sts get-caller-identity > /dev/null 2>&1; then
        log_error "AWS credentials not configured or invalid"
        log_info "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
        exit 1
    fi
    log_success "AWS credentials valid"

    # Check video directory
    if [ ! -d "$VIDEO_DIR" ]; then
        log_error "Video directory not found: $VIDEO_DIR"
        exit 1
    fi

    # Find videos
    log_info "Searching for videos in $VIDEO_DIR..."
    local video_count=$(find "$VIDEO_DIR" -maxdepth 1 -name "*.mp4" -type f | wc -l)

    if [ "$video_count" -eq 0 ]; then
        log_error "No MP4 files found in $VIDEO_DIR"
        exit 1
    fi

    log_success "Found $video_count video(s)"
    echo ""

    # Process each video
    local processed=0
    for video_path in "$VIDEO_DIR"/*.mp4; do
        if [ -f "$video_path" ]; then
            processed=$((processed + 1))
            log_info "[$processed/$video_count] Processing..."
            process_video "$video_path"
            echo ""
        fi
    done

    # Cleanup
    rm -rf "$TEMP_DIR"

    echo "========================================"
    log_success "Upload complete! Processed $processed video(s)"
    echo "========================================"
}

# Run main function
main "$@"
