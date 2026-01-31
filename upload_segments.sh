#!/bin/bash
# upload_segments.sh
# Uploads downloaded GoPro segments from ~/gopro_videos/segments/ to S3.
# Reads AWS credentials and config from .env (never hardcoded).

set -euo pipefail

# ======================== Load .env ========================

# Find .env relative to this script, or in home directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE=""

for candidate in "${SCRIPT_DIR}/.env" "${HOME}/.env"; do
    if [ -f "$candidate" ]; then
        ENV_FILE="$candidate"
        break
    fi
done

if [ -z "$ENV_FILE" ]; then
    echo "ERROR: .env file not found in ${SCRIPT_DIR}/ or ${HOME}/"
    echo "Create one based on .env.example with your AWS credentials."
    exit 1
fi

# Source .env (handle values with or without quotes)
set -a
while IFS='=' read -r key value; do
    # Skip comments and empty lines
    [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
    # Strip leading/trailing whitespace from key
    key=$(echo "$key" | xargs)
    # Strip quotes from value
    value=$(echo "$value" | sed 's/^["'\'']*//;s/["'\'']*$//')
    export "$key=$value"
done < "$ENV_FILE"
set +a

# ======================== Validate Config ========================

: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID in .env}"
: "${AWS_SECRET_ACCESS_KEY:?Set AWS_SECRET_ACCESS_KEY in .env}"

UPLOAD_BUCKET="${UPLOAD_BUCKET:-jetson-videos}"
UPLOAD_REGION="${UPLOAD_REGION:-us-east-1}"
UPLOAD_LOCATION="${UPLOAD_LOCATION:-default-location}"
UPLOAD_DEVICE_NAME="${UPLOAD_DEVICE_NAME:-jetson-nano-01}"
DELETE_AFTER_UPLOAD="${DELETE_AFTER_UPLOAD:-false}"

VIDEO_STORAGE_DIR="${HOME}/gopro_videos"
SEGMENTS_DIR="${VIDEO_STORAGE_DIR}/segments"
LOG_DIR="${VIDEO_STORAGE_DIR}/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/upload_$(date '+%Y%m%d_%H%M%S').log"

# ======================== Usage ========================

usage() {
    echo "Usage: $0 [session_name|all] [options]"
    echo ""
    echo "Uploads segment sessions from ~/gopro_videos/segments/ to S3."
    echo "Credentials are loaded from .env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)."
    echo ""
    echo "Commands:"
    echo "  all                Upload all sessions"
    echo "  <session_name>     Upload a specific session"
    echo "  list               List sessions and their upload status"
    echo "  status             Show S3 bucket contents for today"
    echo ""
    echo "Options:"
    echo "  --delete           Delete local files after successful upload"
    echo ""
    echo "Examples:"
    echo "  $0 list"
    echo "  $0 all"
    echo "  $0 enxd43260ef4715_NL_20260130_143025"
    echo "  $0 all --delete"
    echo ""
    echo "S3 path format: s3://${UPLOAD_BUCKET}/${UPLOAD_LOCATION}/{date}/${UPLOAD_DEVICE_NAME} - {camera}_ch{NN}.mp4"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

# Parse arguments
TARGET="$1"
shift
DELETE_LOCAL=false
for arg in "$@"; do
    case "$arg" in
        --delete) DELETE_LOCAL=true ;;
        *) echo "Unknown option: $arg"; usage ;;
    esac
done

# ======================== Helper Functions ========================

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

err() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*"
    echo "$msg" >&2
    echo "$msg" >> "$LOG_FILE"
}

# Progress bar for uploads
show_upload_progress() {
    local current="$1"
    local total="$2"
    local label="$3"
    local width=30

    if [ "$total" -le 0 ]; then return; fi

    local percent=$(( current * 100 / total ))
    local filled=$(( percent * width / 100 ))
    local empty=$(( width - filled ))

    local bar=""
    for ((i=0; i<filled; i++)); do bar+="█"; done
    for ((i=0; i<empty; i++)); do bar+="░"; done

    printf "\r  %s [%s] %3d%%" "$label" "$bar" "$percent"
}

# ======================== S3 Upload ========================

upload_file_to_s3() {
    local file_path="$1"
    local s3_key="$2"

    local file_size
    file_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null || echo 0)
    local file_hr
    file_hr=$(numfmt --to=iec-i --suffix=B "$file_size" 2>/dev/null || echo "${file_size} bytes")

    log "  Uploading: $(basename "$file_path") (${file_hr}) -> s3://${UPLOAD_BUCKET}/${s3_key}"

    # Use aws cli for upload with progress
    if aws s3 cp "$file_path" "s3://${UPLOAD_BUCKET}/${s3_key}" \
        --region "$UPLOAD_REGION" \
        --content-type "video/mp4" \
        --no-progress 2>> "$LOG_FILE"; then
        log "  Uploaded: s3://${UPLOAD_BUCKET}/${s3_key}"
        return 0
    else
        err "  Failed to upload $(basename "$file_path")"
        return 1
    fi
}

# ======================== Session Upload ========================

upload_session() {
    local session_dir="$1"
    local session_name
    session_name=$(basename "$session_dir")

    log "Processing session: ${session_name}"

    # Parse session name: {interface_id}_{angle}_{YYYYMMDD}_{HHMMSS}
    # interface_id can contain underscores, so extract date/time from the end
    local parts_count
    parts_count=$(echo "$session_name" | tr '_' '\n' | wc -l)

    if [ "$parts_count" -lt 3 ]; then
        err "Invalid session name format: ${session_name} (expected {id}_{YYYYMMDD}_{HHMMSS})"
        return 1
    fi

    # Extract date and time from the last two underscore-separated parts
    local date_str time_str interface_id
    time_str=$(echo "$session_name" | rev | cut -d'_' -f1 | rev)
    date_str=$(echo "$session_name" | rev | cut -d'_' -f2 | rev)
    interface_id=$(echo "$session_name" | rev | cut -d'_' -f3- | rev)

    # Format date as YYYY-MM-DD
    local upload_date
    upload_date="${date_str:0:4}-${date_str:4:2}-${date_str:6:2}"

    # Derive camera name from interface_id
    # Same logic as Python: camera_name_map.get(interface_id) or GoPro-{last4}
    local camera_name
    camera_name="GoPro-${interface_id: -4}"

    log "  Interface: ${interface_id}, Camera: ${camera_name}, Date: ${upload_date}"

    # Get sorted list of video files
    local video_files=()
    while IFS= read -r -d '' vf; do
        video_files+=("$vf")
    done < <(find "$session_dir" -maxdepth 1 -name '*.MP4' -o -name '*.mp4' | sort -z 2>/dev/null || find "$session_dir" -maxdepth 1 \( -name '*.MP4' -o -name '*.mp4' \) -print0 | sort -z)

    # Fallback if -print0 isn't working
    if [ ${#video_files[@]} -eq 0 ]; then
        while IFS= read -r vf; do
            video_files+=("$vf")
        done < <(find "$session_dir" -maxdepth 1 \( -name '*.MP4' -o -name '*.mp4' \) | sort)
    fi

    local total_files=${#video_files[@]}

    if [ "$total_files" -eq 0 ]; then
        log "  No video files in session"
        return 0
    fi

    log "  Found ${total_files} video file(s)"

    local uploaded=0
    local failed=0

    for idx in "${!video_files[@]}"; do
        local file_path="${video_files[$idx]}"
        local file_num=$((idx + 1))

        # Build camera name with chapter suffix (matches Python logic)
        local file_camera_name
        if [ "$total_files" -gt 1 ]; then
            file_camera_name=$(printf "%s_ch%02d" "$camera_name" "$file_num")
        else
            file_camera_name="$camera_name"
        fi

        # Build S3 key: {location}/{date}/{device_name} - {camera_name}.mp4
        local s3_key="${UPLOAD_LOCATION}/${upload_date}/${UPLOAD_DEVICE_NAME} - ${file_camera_name}.mp4"

        printf "  [%d/%d] " "$file_num" "$total_files"

        if upload_file_to_s3 "$file_path" "$s3_key"; then
            uploaded=$((uploaded + 1))
        else
            failed=$((failed + 1))
        fi
    done

    log "  Session complete: ${uploaded} uploaded, ${failed} failed"

    # Delete local session if requested and all uploads succeeded
    if [ "$DELETE_LOCAL" = true ] && [ "$failed" -eq 0 ]; then
        log "  Deleting local session: ${session_dir}"
        rm -rf "$session_dir"
    fi

    return "$failed"
}

# ======================== List Sessions ========================

list_sessions() {
    if [ ! -d "$SEGMENTS_DIR" ]; then
        echo "No segments directory found at ${SEGMENTS_DIR}"
        exit 0
    fi

    echo "Sessions in ${SEGMENTS_DIR}:"
    echo ""
    printf "%-55s %6s %10s\n" "SESSION" "FILES" "SIZE"
    printf "%s\n" "$(printf '%.0s-' {1..75})"

    local total_files=0
    local total_size=0

    for session_dir in "$SEGMENTS_DIR"/*/; do
        [ -d "$session_dir" ] || continue
        local session_name
        session_name=$(basename "$session_dir")

        local file_count=0
        local session_size=0

        for vf in "$session_dir"*.MP4 "$session_dir"*.mp4; do
            [ -f "$vf" ] || continue
            file_count=$((file_count + 1))
            local sz
            sz=$(stat -c%s "$vf" 2>/dev/null || echo 0)
            session_size=$((session_size + sz))
        done

        total_files=$((total_files + file_count))
        total_size=$((total_size + session_size))

        local size_hr
        size_hr=$(numfmt --to=iec-i --suffix=B "$session_size" 2>/dev/null || echo "${session_size}")

        printf "%-55s %6d %10s\n" "$session_name" "$file_count" "$size_hr"
    done

    echo ""
    local total_hr
    total_hr=$(numfmt --to=iec-i --suffix=B "$total_size" 2>/dev/null || echo "${total_size}")
    echo "Total: ${total_files} files, ${total_hr}"
}

# ======================== S3 Status ========================

show_s3_status() {
    local today
    today=$(date '+%Y-%m-%d')

    echo "S3 contents for ${UPLOAD_LOCATION}/${today}:"
    echo ""

    aws s3 ls "s3://${UPLOAD_BUCKET}/${UPLOAD_LOCATION}/${today}/" \
        --region "$UPLOAD_REGION" \
        --human-readable 2>/dev/null || echo "  (empty or access denied)"
}

# ======================== Main ========================

main() {
    log "=========================================="
    log "Segment Upload — target: ${TARGET}"
    log "  Bucket: s3://${UPLOAD_BUCKET}"
    log "  Location: ${UPLOAD_LOCATION}"
    log "  Device: ${UPLOAD_DEVICE_NAME}"
    log "  Region: ${UPLOAD_REGION}"
    log "  Delete after upload: ${DELETE_LOCAL}"
    log "  Log file: ${LOG_FILE}"
    log "=========================================="

    # Check for aws cli
    if ! command -v aws &> /dev/null; then
        err "AWS CLI not found. Install with: sudo apt install awscli  (or: pip install awscli)"
        exit 1
    fi

    # Verify credentials work
    log "Verifying AWS credentials..."
    if ! aws sts get-caller-identity --region "$UPLOAD_REGION" > /dev/null 2>&1; then
        # Try a lightweight S3 operation instead (sts might not be available)
        if ! aws s3 ls "s3://${UPLOAD_BUCKET}" --region "$UPLOAD_REGION" --max-items 1 > /dev/null 2>&1; then
            err "AWS credentials invalid or bucket not accessible. Check .env"
            exit 1
        fi
    fi
    log "AWS credentials OK"

    case "$TARGET" in
        list)
            list_sessions
            exit 0
            ;;
        status)
            show_s3_status
            exit 0
            ;;
        all)
            if [ ! -d "$SEGMENTS_DIR" ]; then
                err "No segments directory: ${SEGMENTS_DIR}"
                exit 1
            fi

            local session_count=0
            local success_count=0
            local fail_count=0

            for session_dir in "$SEGMENTS_DIR"/*/; do
                [ -d "$session_dir" ] || continue
                session_count=$((session_count + 1))

                log "------------------------------------------"
                if upload_session "$session_dir"; then
                    success_count=$((success_count + 1))
                else
                    fail_count=$((fail_count + 1))
                fi
            done

            log "=========================================="
            log "Upload complete: ${success_count}/${session_count} sessions succeeded, ${fail_count} failed"
            log "=========================================="
            ;;
        *)
            # Specific session name
            local session_dir="${SEGMENTS_DIR}/${TARGET}"
            if [ ! -d "$session_dir" ]; then
                err "Session not found: ${TARGET}"
                echo "Available sessions:"
                ls -1 "$SEGMENTS_DIR" 2>/dev/null || echo "  (none)"
                exit 1
            fi

            upload_session "$session_dir"

            log "=========================================="
            log "Upload complete"
            log "=========================================="
            ;;
    esac
}

main
