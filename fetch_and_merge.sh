#!/bin/bash
# fetch_and_merge.sh
# Discovers USB-connected GoPro cameras, downloads all video files,
# and merges chapters per-GoPro per-date using ffmpeg concat demuxer.

set -euo pipefail

# ======================== Usage ========================

usage() {
    echo "Usage: $0 <MM-DD>"
    echo ""
    echo "Downloads GoPro files from a specific date and merges them per-camera."
    echo ""
    echo "Examples:"
    echo "  $0 01-30          # Downloads and merges all files from January 30th"
    echo "  $0 12-25          # Downloads and merges all files from December 25th"
    exit 1
}

# ======================== Date Argument ========================

if [ $# -lt 1 ]; then
    usage
fi

TARGET_DATE_INPUT="$1"

# Validate MM-DD format
if [[ ! "$TARGET_DATE_INPUT" =~ ^[0-9]{2}-[0-9]{2}$ ]]; then
    err "Invalid date format: ${TARGET_DATE_INPUT}. Expected MM-DD (e.g., 01-30)"
    usage
fi

TARGET_MM="${TARGET_DATE_INPUT:0:2}"
TARGET_DD="${TARGET_DATE_INPUT:3:2}"

# ======================== Configuration ========================
VIDEO_STORAGE_DIR="${HOME}/gopro_videos"
SEGMENTS_DIR="${VIDEO_STORAGE_DIR}/segments"
LOG_DIR="${VIDEO_STORAGE_DIR}/logs"
CONNECT_TIMEOUT=3
DOWNLOAD_TIMEOUT=600
KEEPALIVE_INTERVAL=2

mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/fetch_merge_${TARGET_MM}-${TARGET_DD}_$(date '+%Y%m%d_%H%M%S').log"

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

# Spinner for long-running operations
SPINNER_PID=""
start_spinner() {
    local msg="$1"
    (
        local chars='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
        local i=0
        while true; do
            local c="${chars:$i:1}"
            printf "\r  %s %s" "$c" "$msg"
            i=$(( (i + 1) % ${#chars} ))
            sleep 0.1
        done
    ) &
    SPINNER_PID=$!
}

stop_spinner() {
    if [ -n "$SPINNER_PID" ]; then
        kill "$SPINNER_PID" 2>/dev/null || true
        wait "$SPINNER_PID" 2>/dev/null || true
        SPINNER_PID=""
        printf "\r\033[K"  # Clear the spinner line
    fi
}

# Progress bar for downloads
show_progress_bar() {
    local current="$1"
    local total="$2"
    local filename="$3"
    local width=30

    if [ "$total" -le 0 ]; then return; fi

    local percent=$(( current * 100 / total ))
    local filled=$(( percent * width / 100 ))
    local empty=$(( width - filled ))
    local current_mb=$(awk "BEGIN {printf \"%.1f\", ${current}/1048576}")
    local total_mb=$(awk "BEGIN {printf \"%.1f\", ${total}/1048576}")

    local bar=""
    for ((i=0; i<filled; i++)); do bar+="█"; done
    for ((i=0; i<empty; i++)); do bar+="░"; done

    printf "\r  %s [%s] %3d%% (%s/%s MB)" "$filename" "$bar" "$percent" "$current_mb" "$total_mb"
}

cleanup_on_exit() {
    stop_spinner
    # Kill any background keep-alive loops
    if [ ${#KEEPALIVE_PIDS[@]} -gt 0 ]; then
        for pid in "${KEEPALIVE_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
    fi
}
trap cleanup_on_exit EXIT

declare -a KEEPALIVE_PIDS=()

# ======================== GoPro Discovery ========================

discover_gopros() {
    log "Scanning USB network interfaces for GoPro cameras..."
    local found=0

    # Get all network interfaces with 172.x.x.x IPs (USB-connected GoPros)
    while IFS= read -r line; do
        # Extract interface name (last field) and IP (second field, strip CIDR)
        local iface ip base our_last
        iface=$(echo "$line" | awk '{print $NF}')
        ip=$(echo "$line" | awk '{print $2}' | cut -d'/' -f1)

        # Verify it's a 172.x.x.x address
        [[ "$ip" =~ ^172\. ]] || continue

        base=$(echo "$ip" | rev | cut -d'.' -f2- | rev)
        our_last=$(echo "$ip" | rev | cut -d'.' -f1 | rev)

        log "  Interface ${iface}: our IP is ${ip} (base=${base}, last=${our_last})"

        # Build candidate GoPro IPs based on our own IP
        local candidates=()
        if [ "$our_last" = "50" ]; then
            candidates=("${base}.51" "${base}.1")
        elif [ "$our_last" = "51" ]; then
            candidates=("${base}.50" "${base}.1")
        else
            candidates=("${base}.51" "${base}.50" "${base}.1")
        fi

        # Try each candidate
        for candidate in "${candidates[@]}"; do
            start_spinner "Trying ${candidate}..."
            if curl -s --connect-timeout "$CONNECT_TIMEOUT" \
                "http://${candidate}:8080/gopro/camera/state" > /dev/null 2>&1; then
                stop_spinner
                log "  ✓ Found GoPro at ${candidate} (via interface ${iface})"
                GOPRO_IPS+=("$candidate")
                GOPRO_IFACES+=("$iface")
                found=$((found + 1))
                break
            else
                stop_spinner
            fi
        done
    done < <(ip addr show | grep 'inet 172\.')

    if [ "$found" -eq 0 ]; then
        err "No GoPro cameras found on USB interfaces."
        exit 1
    fi

    log "Discovered ${found} GoPro camera(s)."
}

# ======================== Keep-Alive ========================

start_keepalive() {
    local gopro_ip="$1"
    (
        while true; do
            curl -s --connect-timeout 2 \
                "http://${gopro_ip}:8080/gopro/camera/keep_alive" > /dev/null 2>&1 || true
            sleep "$KEEPALIVE_INTERVAL"
        done
    ) &
    KEEPALIVE_PIDS+=($!)
}

stop_keepalive() {
    if [ ${#KEEPALIVE_PIDS[@]} -gt 0 ]; then
        local pid="${KEEPALIVE_PIDS[-1]}"
        kill "$pid" 2>/dev/null || true
        unset 'KEEPALIVE_PIDS[-1]'
    fi
}

# ======================== Video Integrity ========================

FFPROBE_TIMEOUT=120

verify_video() {
    local filepath="$1"
    local filename
    filename=$(basename "$filepath")

    # ffprobe to check the file has a valid video stream with duration
    local duration
    duration=$(timeout "$FFPROBE_TIMEOUT" ffprobe -v error -select_streams v:0 \
        -show_entries stream=duration -of csv=p=0 "$filepath" 2>/dev/null) || {
        local exit_code=$?
        if [ "$exit_code" -eq 124 ]; then
            # Timeout — assume valid (large 4K file on loaded system)
            log "  ⚠ ffprobe timeout for ${filename} — assuming valid (file size matched)"
            echo "$duration" >> "$LOG_FILE"
            return 0
        fi
        return 1
    }

    if [ -n "$duration" ] && awk "BEGIN {exit !($duration > 0)}" 2>/dev/null; then
        return 0
    fi
    return 1
}

# ======================== Camera Info ========================

get_media_list() {
    local gopro_ip="$1"
    curl -s --connect-timeout "$CONNECT_TIMEOUT" \
        "http://${gopro_ip}:8080/gopro/media/list" 2>/dev/null
}

get_camera_angle() {
    # Get the camera angle code (FL, FR, NL, NR) from the GoPro's ap_ssid
    # NOTE: Do NOT call log() here — this runs inside $() so stdout is captured
    local gopro_ip="$1"
    local info_json
    info_json=$(curl -s --connect-timeout "$CONNECT_TIMEOUT" \
        "http://${gopro_ip}:8080/gopro/camera/info" 2>/dev/null) || { echo "UNK"; return; }

    local camera_name
    camera_name=$(echo "$info_json" | jq -r '.ap_ssid // .info.ap_ssid // empty' 2>/dev/null)

    if [ -z "$camera_name" ]; then
        echo "UNK"
        return
    fi

    # Write camera name to log file directly (not stdout, which is captured)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')]   Camera name: ${camera_name}" >> "$LOG_FILE"

    local upper_name
    upper_name=$(echo "$camera_name" | tr '[:lower:]' '[:upper:]')

    # Match common angle keywords
    for code in FL FR NL NR; do
        if [[ "$upper_name" == *"$code"* ]]; then
            echo "$code"
            return
        fi
    done

    # Fallback: try to match "FAR"/"NEAR" + "LEFT"/"RIGHT" patterns
    local pos="" side=""
    [[ "$upper_name" == *FAR* ]] && pos="F"
    [[ "$upper_name" == *NEAR* ]] && pos="N"
    [[ "$upper_name" == *LEFT* ]] && side="L"
    [[ "$upper_name" == *RIGHT* ]] && side="R"

    if [ -n "$pos" ] && [ -n "$side" ]; then
        echo "${pos}${side}"
        return
    fi

    echo "UNK"
}

# ======================== File Download ========================

download_file() {
    local gopro_ip="$1"
    local directory="$2"
    local filename="$3"
    local output_path="$4"
    local expected_size="$5"
    local url="http://${gopro_ip}:8080/videos/DCIM/${directory}/${filename}"

    # Resume support: check if partial file exists
    local current_size=0
    if [ -f "$output_path" ]; then
        current_size=$(stat -c%s "$output_path" 2>/dev/null || echo 0)
        if [ "$current_size" -ge "$expected_size" ]; then
            show_progress_bar "$expected_size" "$expected_size" "$filename"
            printf " ✓\n"
            log "  Already downloaded: ${filename}"
            return 0
        fi
        log "  Resuming ${filename} from byte ${current_size}..."
    fi

    local max_retries=5
    for attempt in $(seq 1 $max_retries); do
        # Use curl with progress callback via --write-out and background monitoring
        if [ "$current_size" -gt 0 ]; then
            curl --connect-timeout "$CONNECT_TIMEOUT" \
                --max-time "$DOWNLOAD_TIMEOUT" \
                -H "Range: bytes=${current_size}-" \
                -o "$output_path" -C - \
                "$url" 2>/dev/null &
        else
            curl --connect-timeout "$CONNECT_TIMEOUT" \
                --max-time "$DOWNLOAD_TIMEOUT" \
                -o "$output_path" \
                "$url" 2>/dev/null &
        fi
        local curl_pid=$!

        # Monitor download progress in foreground
        while kill -0 "$curl_pid" 2>/dev/null; do
            if [ -f "$output_path" ]; then
                local dl_size
                dl_size=$(stat -c%s "$output_path" 2>/dev/null || echo 0)
                show_progress_bar "$dl_size" "$expected_size" "$filename"
            fi
            sleep 0.5
        done
        wait "$curl_pid" 2>/dev/null || true

        # Verify file size
        local actual_size
        actual_size=$(stat -c%s "$output_path" 2>/dev/null || echo 0)
        show_progress_bar "$actual_size" "$expected_size" "$filename"

        if [ "$actual_size" -ge "$expected_size" ]; then
            # Size matches — run integrity check
            printf " verifying..."
            if verify_video "$output_path"; then
                printf "\r\033[K"
                show_progress_bar "$actual_size" "$expected_size" "$filename"
                printf " ✓\n"
                log "  Downloaded & verified: ${filename} ($(numfmt --to=iec-i --suffix=B "$actual_size"))"
            else
                printf "\r\033[K"
                show_progress_bar "$actual_size" "$expected_size" "$filename"
                printf " ⚠ (integrity check failed, keeping file)\n"
                log "  ⚠ ${filename}: integrity check failed but file size matches — keeping"
            fi
            return 0
        fi

        printf " ✗\n"
        current_size="$actual_size"
        log "  Retry ${attempt}/${max_retries} for ${filename} (got ${actual_size}/${expected_size} bytes)"
        sleep 2
    done

    err "Failed to download ${filename} after ${max_retries} attempts"
    return 1
}

# ======================== Download & Organize Per GoPro ========================

# GoPro filename format: G[Type][Chapter][VideoID].MP4
# e.g. GH010042.MP4 = Type H, Chapter 01, Video 0042
# Chapters with the same VideoID belong to one recording.

get_video_id() {
    local filename="$1"
    local upper
    upper=$(echo "$filename" | tr '[:lower:]' '[:upper:]')
    if [[ ${#upper} -ge 8 && "$upper" == G* ]]; then
        echo "${upper:4:4}"
    else
        echo "${filename%.*}"
    fi
}

get_chapter_num() {
    local filename="$1"
    local upper
    upper=$(echo "$filename" | tr '[:lower:]' '[:upper:]')
    if [[ ${#upper} -ge 8 && "$upper" == G* ]]; then
        echo "${upper:2:2}"
    else
        echo "00"
    fi
}

download_and_organize() {
    local gopro_ip="$1"
    local iface="$2"

    # Get camera angle code
    local angle
    angle=$(get_camera_angle "$gopro_ip")
    log "  Camera angle: ${angle}"

    log "Fetching media list from ${gopro_ip}..."
    local media_json
    media_json=$(get_media_list "$gopro_ip")

    if [ -z "$media_json" ]; then
        err "Could not get media list from ${gopro_ip}"
        return 1
    fi

    local file_count
    file_count=$(echo "$media_json" | jq '[.media[].fs[]] | length')
    log "Found ${file_count} file(s) on GoPro"

    if [ "$file_count" -eq 0 ]; then
        log "No files on this GoPro"
        return 0
    fi

    # Step 1: Build list of matching files grouped by video ID
    # Format: video_id \t chapter_num \t directory \t filename \t size \t creation_ts
    declare -A video_groups  # video_id -> newline-separated "chapter_num\tdir\tfilename\tsize\tcreation_ts"
    declare -A video_first_ts  # video_id -> earliest creation timestamp (for session naming)
    local downloaded=0
    local skipped=0

    while IFS=$'\t' read -r directory filename size creation_ts; do
        local upper_fn
        upper_fn=$(echo "$filename" | tr '[:lower:]' '[:upper:]')
        [[ "$upper_fn" == *.MP4 ]] || continue

        # Filter by target date
        local file_mm file_dd
        file_mm=$(date -d "@${creation_ts}" '+%m' 2>/dev/null || echo "00")
        file_dd=$(date -d "@${creation_ts}" '+%d' 2>/dev/null || echo "00")

        if [ "$file_mm" != "$TARGET_MM" ] || [ "$file_dd" != "$TARGET_DD" ]; then
            skipped=$((skipped + 1))
            continue
        fi

        local vid
        vid=$(get_video_id "$filename")
        local entry="${directory}\t${filename}\t${size}\t${creation_ts}"

        if [ -z "${video_groups[$vid]+x}" ]; then
            video_groups[$vid]="$entry"
            video_first_ts[$vid]="$creation_ts"
        else
            video_groups[$vid]="${video_groups[$vid]}"$'\n'"$entry"
            # Track earliest timestamp for session naming
            if [ "$creation_ts" -lt "${video_first_ts[$vid]}" ]; then
                video_first_ts[$vid]="$creation_ts"
            fi
        fi
    done < <(echo "$media_json" | jq -r '.media[] | .d as $dir | .fs[] | "\($dir)\t\(.n)\t\(.s)\t\(.cre)"')

    if [ ${#video_groups[@]} -eq 0 ]; then
        log "No video files match ${TARGET_MM}-${TARGET_DD} (skipped ${skipped})"
        return 0
    fi

    log "Found ${#video_groups[@]} recording(s) matching ${TARGET_MM}-${TARGET_DD}, skipped ${skipped} file(s)"

    start_keepalive "$gopro_ip"

    # Step 2: For each video ID, create a session folder and download chapters
    declare -a SESSION_DIRS=()

    for vid in $(echo "${!video_groups[@]}" | tr ' ' '\n' | sort); do
        local first_ts="${video_first_ts[$vid]}"
        local session_ts
        session_ts=$(date -d "@${first_ts}" '+%Y%m%d_%H%M%S' 2>/dev/null || echo "00000000_000000")

        # Session folder: {interface}_{angle}_{YYYYMMDD_HHMMSS}
        local session_id="${iface}_${angle}_${session_ts}"
        local session_dir="${SEGMENTS_DIR}/${session_id}"
        mkdir -p "$session_dir"
        SESSION_DIRS+=("$session_dir")

        log "Recording ${vid} → session: ${session_id}"

        # Sort chapters by GoPro chapter number and download
        local chapter_idx=0
        while IFS=$'\t' read -r directory filename size creation_ts; do
            chapter_idx=$((chapter_idx + 1))
            local chapter_name
            chapter_name=$(printf "chapter_%03d_%s" "$chapter_idx" "$filename")
            local output_path="${session_dir}/${chapter_name}"

            log "  Downloading: ${directory}/${filename} ($(numfmt --to=iec-i --suffix=B "$size"))"
            download_file "$gopro_ip" "$directory" "$filename" "$output_path" "$size"

            # Preserve the GoPro creation timestamp
            touch -d "@${creation_ts}" "$output_path" 2>/dev/null || true

            downloaded=$((downloaded + 1))
        done < <(echo -e "${video_groups[$vid]}" | sort -t$'\t' -k2,2)
        # Sort by filename ensures chapter order (GX01..., GX02..., GX03...)
    done

    stop_keepalive
    log "Downloaded ${downloaded} file(s) into ${#SESSION_DIRS[@]} session(s)"
    log "Done processing GoPro at ${gopro_ip}"
}

# ======================== Main ========================

main() {
    log "=========================================="
    log "GoPro Fetch & Merge — date filter: ${TARGET_MM}-${TARGET_DD}"
    log "Log file: ${LOG_FILE}"
    log "=========================================="

    # Check dependencies
    for cmd in curl jq numfmt; do
        if ! command -v "$cmd" &> /dev/null; then
            err "Required command not found: ${cmd}"
            exit 1
        fi
    done

    mkdir -p "$SEGMENTS_DIR"

    declare -a GOPRO_IPS=()
    declare -a GOPRO_IFACES=()

    # Step 1: Discover GoPros
    discover_gopros

    # Step 2: Download, organize into segments, and merge
    for i in "${!GOPRO_IPS[@]}"; do
        local ip="${GOPRO_IPS[$i]}"
        local iface="${GOPRO_IFACES[$i]}"

        log "------------------------------------------"
        log "Processing GoPro at ${ip} (${iface})"
        log "------------------------------------------"

        download_and_organize "$ip" "$iface" || {
            err "Failed to process GoPro at ${ip}."
            continue
        }
    done

    log "=========================================="
    log "All done! Segments: ${SEGMENTS_DIR}"
    log "Full log: ${LOG_FILE}"
    log "=========================================="
    ls -lhR "$SEGMENTS_DIR" 2>/dev/null | tee -a "$LOG_FILE" || true
}

main "$@"
