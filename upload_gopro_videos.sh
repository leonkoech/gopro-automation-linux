#!/bin/bash
#
# GoPro Video Upload Script
# Uploads all videos from connected GoPros to AWS S3
# Organized by: {location}/{date}/{device_name} - {camera_name}.mp4
#
# Usage: ./upload_gopro_videos.sh [--dry-run] [--delete-after]
#

set -e

# Configuration - Edit these values
LOCATION="${UPLOAD_LOCATION:-default-location}"
DEVICE_NAME="${UPLOAD_DEVICE_NAME:-$(hostname)}"
S3_BUCKET="${UPLOAD_BUCKET:-jetson-videos-uai}"
AWS_REGION="${UPLOAD_REGION:-us-east-1}"
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
            echo "  --delete-after  Delete videos from GoPro after successful upload"
            echo "  --no-compress   Skip compression, upload original files (much faster)"
            echo ""
            echo "Environment variables:"
            echo "  UPLOAD_LOCATION    - Location name for S3 path (default: default-location)"
            echo "  UPLOAD_DEVICE_NAME - Device name for filename (default: hostname)"
            echo "  UPLOAD_BUCKET      - S3 bucket name (default: jetson-videos)"
            echo "  UPLOAD_REGION      - AWS region (default: us-east-1)"
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

    command -v curl >/dev/null 2>&1 || missing+=("curl")
    command -v aws >/dev/null 2>&1 || missing+=("awscli")
    command -v jq >/dev/null 2>&1 || missing+=("jq")

    if [ "$COMPRESS_TO_1080P" = "true" ]; then
        command -v ffmpeg >/dev/null 2>&1 || missing+=("ffmpeg")
    fi

    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing dependencies: ${missing[*]}"
        log_info "Install with: sudo apt install ${missing[*]}"
        exit 1
    fi
}

# Find GoPro IP addresses
find_gopro_ips() {
    local gopro_ips=()

    for iface in $(ls /sys/class/net | grep enx); do
        local our_ip=$(ip addr show "$iface" 2>/dev/null | grep -oP 'inet \K[\d.]+' | head -1)
        if [ -z "$our_ip" ]; then
            continue
        fi

        local base=$(echo "$our_ip" | cut -d. -f1-3)
        local our_last=$(echo "$our_ip" | cut -d. -f4)

        # Try common GoPro IP patterns
        for last in 51 50 52 53 54 55 1; do
            if [ "$last" != "$our_last" ]; then
                local gopro_ip="$base.$last"
                if curl -s --connect-timeout 2 "http://$gopro_ip:8080/gopro/camera/state" > /dev/null 2>&1; then
                    gopro_ips+=("$gopro_ip")
                    # Log to stderr so it doesn't get captured in function output
                    echo -e "${GREEN}[SUCCESS]${NC} Found GoPro at $gopro_ip (via $iface)" >&2
                    break
                fi
            fi
        done
    done

    # Only output the IPs (to stdout)
    echo "${gopro_ips[@]}"
}

# Get camera name from GoPro
get_camera_name() {
    local gopro_ip=$1
    local camera_info=$(curl -s --connect-timeout 5 "http://$gopro_ip:8080/gopro/camera/info" 2>/dev/null)

    if [ -n "$camera_info" ]; then
        local ap_ssid=$(echo "$camera_info" | jq -r '.info.ap_ssid // empty' 2>/dev/null)
        if [ -n "$ap_ssid" ]; then
            echo "$ap_ssid"
            return
        fi
    fi

    # Fallback to IP-based name
    echo "GoPro-$(echo $gopro_ip | cut -d. -f4)"
}

# Get media list from GoPro
get_media_list() {
    local gopro_ip=$1
    curl -s --connect-timeout 10 "http://$gopro_ip:8080/gopro/media/list" 2>/dev/null
}

# Convert Unix timestamp to date string
timestamp_to_date() {
    local ts=$1
    date -d "@$ts" "+%Y-%m-%d" 2>/dev/null || date -r "$ts" "+%Y-%m-%d" 2>/dev/null
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

# Delete file from GoPro
delete_from_gopro() {
    local gopro_ip=$1
    local directory=$2
    local filename=$3

    if [ "$DRY_RUN" = "true" ]; then
        log_info "[DRY-RUN] Would delete from GoPro: $directory/$filename"
        return 0
    fi

    log_info "Deleting from GoPro: $directory/$filename"

    curl -s --connect-timeout 10 \
        "http://$gopro_ip:8080/gopro/media/delete/file?path=$directory/$filename" \
        > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        log_success "Deleted from GoPro: $filename"
        return 0
    else
        log_warn "Delete may have failed: $filename"
        return 1
    fi
}

# Process videos from a single GoPro
process_gopro() {
    local gopro_ip=$1
    local camera_name=$(get_camera_name "$gopro_ip")

    log_info "Processing GoPro: $camera_name ($gopro_ip)"

    local media_list=$(get_media_list "$gopro_ip")
    if [ -z "$media_list" ]; then
        log_error "Could not get media list from $gopro_ip"
        return 1
    fi

    # Parse media list and process each video
    local directories=$(echo "$media_list" | jq -r '.media[]?.d // empty')

    for dir in $directories; do
        log_info "Processing directory: $dir"

        # Get files in this directory
        local files=$(echo "$media_list" | jq -r ".media[] | select(.d == \"$dir\") | .fs[]? | @base64")

        for file_b64 in $files; do
            local file_json=$(echo "$file_b64" | base64 -d)
            local filename=$(echo "$file_json" | jq -r '.n')
            local file_size=$(echo "$file_json" | jq -r '.s')
            local file_cre=$(echo "$file_json" | jq -r '.cre')

            # Skip non-video files
            if [[ ! "$filename" =~ \.(MP4|mp4)$ ]]; then
                continue
            fi

            # Get date from creation timestamp
            local video_date=$(timestamp_to_date "$file_cre")
            if [ -z "$video_date" ]; then
                video_date=$(date "+%Y-%m-%d")
            fi

            local file_size_mb=$(echo "scale=2; $file_size / 1024 / 1024" | bc)
            log_info "Found: $filename ($file_size_mb MB) - Date: $video_date"

            # Build S3 key
            local s3_filename="${DEVICE_NAME} - ${camera_name}.mp4"
            # Add original filename to make unique if multiple videos same day
            local s3_filename="${DEVICE_NAME} - ${camera_name} - ${filename}"
            local s3_key="${LOCATION}/${video_date}/${s3_filename}"

            if [ "$DRY_RUN" = "true" ]; then
                log_info "[DRY-RUN] Would upload: $s3_key"
                continue
            fi

            # Create temp directory
            mkdir -p "$TEMP_DIR"

            # Download from GoPro
            local download_url="http://$gopro_ip:8080/videos/DCIM/$dir/$filename"
            local temp_file="$TEMP_DIR/$filename"

            log_info "Downloading: $filename ($file_size_mb MB)"

            # Use longer timeout and retry for large files
            local max_retries=3
            local retry=0
            local download_success=false

            while [ $retry -lt $max_retries ] && [ "$download_success" = "false" ]; do
                if [ $retry -gt 0 ]; then
                    log_warn "Retry $retry/$max_retries for $filename"
                fi

                # -C - enables resume, --max-time is total time limit (2 hours for large files)
                if curl -C - --connect-timeout 60 --max-time 7200 --retry 2 \
                    --progress-bar -o "$temp_file" "$download_url" 2>&1 | \
                    tr '\r' '\n' | grep -E "^[# ]" | tail -1; then

                    # Verify file size matches expected
                    if [ -f "$temp_file" ]; then
                        local actual_size=$(stat -c%s "$temp_file" 2>/dev/null || stat -f%z "$temp_file")
                        if [ "$actual_size" -ge "$file_size" ]; then
                            download_success=true
                            log_success "Download complete: $filename"
                        else
                            log_warn "Incomplete download: got $actual_size bytes, expected $file_size"
                        fi
                    fi
                fi

                retry=$((retry + 1))
            done

            if [ "$download_success" = "false" ] || [ ! -f "$temp_file" ]; then
                log_error "Download failed after $max_retries attempts: $filename"
                rm -f "$temp_file"
                continue
            fi

            local upload_file="$temp_file"

            # Compress if enabled
            if [ "$COMPRESS_TO_1080P" = "true" ]; then
                local compressed_file="$TEMP_DIR/compressed_$filename"
                if compress_video "$temp_file" "$compressed_file"; then
                    upload_file="$compressed_file"
                fi
            fi

            # Upload to S3
            if upload_to_s3 "$upload_file" "$s3_key"; then
                # Delete from GoPro if requested
                if [ "$DELETE_AFTER" = "true" ]; then
                    delete_from_gopro "$gopro_ip" "$dir" "$filename"
                fi
            fi

            # Cleanup temp files
            rm -f "$temp_file" "$TEMP_DIR/compressed_$filename"
        done
    done

    log_success "Finished processing: $camera_name"
}

# Main function
main() {
    echo ""
    echo "========================================"
    echo "  GoPro Video Upload Script"
    echo "========================================"
    echo ""

    log_info "Location: $LOCATION"
    log_info "Device: $DEVICE_NAME"
    log_info "S3 Bucket: $S3_BUCKET"
    log_info "Compress: $COMPRESS_TO_1080P"

    if [ "$DRY_RUN" = "true" ]; then
        log_warn "DRY-RUN MODE - No actual uploads will be performed"
    fi

    if [ "$DELETE_AFTER" = "true" ]; then
        log_warn "DELETE_AFTER enabled - Videos will be deleted from GoPro after upload"
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

    # Find GoPros
    log_info "Searching for connected GoPros..."
    IFS=' ' read -ra GOPRO_IPS <<< "$(find_gopro_ips)"

    if [ ${#GOPRO_IPS[@]} -eq 0 ]; then
        log_error "No GoPros found"
        exit 1
    fi

    log_success "Found ${#GOPRO_IPS[@]} GoPro(s)"
    echo ""

    # Process each GoPro
    for gopro_ip in "${GOPRO_IPS[@]}"; do
        process_gopro "$gopro_ip"
        echo ""
    done

    # Cleanup
    rm -rf "$TEMP_DIR"

    echo "========================================"
    log_success "Upload complete!"
    echo "========================================"
}

# Run main function
main "$@"
