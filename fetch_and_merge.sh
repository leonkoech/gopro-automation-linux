#!/bin/bash
# fetch_and_merge.sh
# Discovers USB-connected GoPro cameras, downloads all video files,
# and merges chapters per-GoPro per-date using ffmpeg concat demuxer.

set -euo pipefail

# ======================== Configuration ========================
OUTPUT_DIR="${HOME}/gopro_videos/merged"
TEMP_DIR="${HOME}/gopro_videos/tmp_downloads"
CHUNK_SIZE=1048576  # 1MB download chunks
CONNECT_TIMEOUT=3
DOWNLOAD_TIMEOUT=600
KEEPALIVE_INTERVAL=2

# ======================== Helper Functions ========================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

err() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

cleanup_on_exit() {
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
            log "  Trying ${candidate}..."
            if curl -s --connect-timeout "$CONNECT_TIMEOUT" \
                "http://${candidate}:8080/gopro/camera/state" > /dev/null 2>&1; then
                log "  Found GoPro at ${candidate} (via interface ${iface})"
                GOPRO_IPS+=("$candidate")
                GOPRO_IFACES+=("$iface")
                found=$((found + 1))
                break
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

# ======================== Media Listing ========================

get_media_list() {
    local gopro_ip="$1"
    curl -s --connect-timeout "$CONNECT_TIMEOUT" \
        "http://${gopro_ip}:8080/gopro/media/list" 2>/dev/null
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
            log "  Already downloaded: ${filename} (${expected_size} bytes)"
            return 0
        fi
        log "  Resuming ${filename} from byte ${current_size}..."
    fi

    local max_retries=5
    for attempt in $(seq 1 $max_retries); do
        if [ "$current_size" -gt 0 ]; then
            # Resume download
            curl -s --connect-timeout "$CONNECT_TIMEOUT" \
                --max-time "$DOWNLOAD_TIMEOUT" \
                -H "Range: bytes=${current_size}-" \
                -o "$output_path" -C - \
                "$url" 2>/dev/null
        else
            curl -s --connect-timeout "$CONNECT_TIMEOUT" \
                --max-time "$DOWNLOAD_TIMEOUT" \
                -o "$output_path" \
                "$url" 2>/dev/null
        fi

        # Verify file size
        local actual_size
        actual_size=$(stat -c%s "$output_path" 2>/dev/null || echo 0)
        if [ "$actual_size" -ge "$expected_size" ]; then
            return 0
        fi

        current_size="$actual_size"
        log "  Retry ${attempt}/${max_retries} for ${filename} (got ${actual_size}/${expected_size} bytes)"
        sleep 2
    done

    err "Failed to download ${filename} after ${max_retries} attempts"
    return 1
}

# ======================== Download All From One GoPro ========================

download_gopro_files() {
    local gopro_ip="$1"
    local gopro_label="$2"
    local gopro_dir="${TEMP_DIR}/${gopro_label}"
    mkdir -p "$gopro_dir"

    log "Fetching media list from ${gopro_ip}..."
    local media_json
    media_json=$(get_media_list "$gopro_ip")

    if [ -z "$media_json" ]; then
        err "Could not get media list from ${gopro_ip}"
        return 1
    fi

    # Parse media list with jq: extract directory, filename, size, creation time
    local file_count
    file_count=$(echo "$media_json" | jq '[.media[].fs[]] | length')
    log "Found ${file_count} file(s) on GoPro ${gopro_label}"

    if [ "$file_count" -eq 0 ]; then
        log "No files to download from ${gopro_label}"
        return 0
    fi

    start_keepalive "$gopro_ip"

    # Iterate over each directory and file
    echo "$media_json" | jq -r '.media[] | .d as $dir | .fs[] | "\($dir)\t\(.n)\t\(.s)\t\(.cre)"' | \
    while IFS=$'\t' read -r directory filename size creation_ts; do
        # Only download video files
        local upper_fn
        upper_fn=$(echo "$filename" | tr '[:lower:]' '[:upper:]')
        if [[ ! "$upper_fn" == *.MP4 ]]; then
            continue
        fi

        local output_path="${gopro_dir}/${directory}__${filename}"
        log "  Downloading: ${directory}/${filename} ($(numfmt --to=iec-i --suffix=B "$size"))"
        download_file "$gopro_ip" "$directory" "$filename" "$output_path" "$size"
    done

    stop_keepalive
    log "Download complete for GoPro ${gopro_label}"
}

# ======================== Chapter Grouping & Merging ========================

# GoPro filename format: G[Type][Chapter][VideoID].MP4
# e.g. GH010042.MP4 = Type H, Chapter 01, Video 0042
# Chapters with the same VideoID belong together.

get_video_id() {
    local filename="$1"
    # Strip directory prefix (100GOPRO__) if present
    local base
    base=$(basename "$filename")
    local upper
    upper=$(echo "$base" | tr '[:lower:]' '[:upper:]')

    if [[ ${#upper} -ge 8 && "$upper" == G* ]]; then
        echo "${upper:4:4}"  # Characters 4-7 = video ID
    else
        echo "$base"
    fi
}

get_chapter_num() {
    local filename="$1"
    local base
    base=$(basename "$filename")
    local upper
    upper=$(echo "$base" | tr '[:lower:]' '[:upper:]')

    if [[ ${#upper} -ge 8 && "$upper" == G* ]]; then
        echo "${upper:2:2}"  # Characters 2-3 = chapter number
    else
        echo "00"
    fi
}

get_file_date() {
    local filepath="$1"
    # Use file modification time to determine the date
    date -r "$filepath" '+%Y-%m-%d' 2>/dev/null || echo "unknown"
}

merge_gopro_files() {
    local gopro_label="$1"
    local gopro_dir="${TEMP_DIR}/${gopro_label}"

    if [ ! -d "$gopro_dir" ] || [ -z "$(ls -A "$gopro_dir" 2>/dev/null)" ]; then
        log "No files to merge for ${gopro_label}"
        return 0
    fi

    log "Grouping and merging files for ${gopro_label}..."

    # Step 1: Group files by (date, video_id)
    # Build an associative array: key="date|video_id" -> sorted list of chapter files
    declare -A groups

    for filepath in "${gopro_dir}"/*.MP4 "${gopro_dir}"/*.mp4; do
        [ -f "$filepath" ] || continue

        local filename
        filename=$(basename "$filepath")
        local video_id
        video_id=$(get_video_id "$filename")
        local file_date
        file_date=$(get_file_date "$filepath")
        local key="${file_date}|${video_id}"

        if [ -z "${groups[$key]+x}" ]; then
            groups[$key]="$filepath"
        else
            groups[$key]="${groups[$key]}"$'\n'"$filepath"
        fi
    done

    # Step 2: For each group, merge chapters into one file
    # Then collect per-date files for the final per-date merge
    declare -A date_files

    for key in "${!groups[@]}"; do
        local file_date="${key%%|*}"
        local video_id="${key##*|}"
        local file_list="${groups[$key]}"

        # Sort chapters by chapter number
        local sorted_files
        sorted_files=$(echo "$file_list" | while read -r f; do
            local chap
            chap=$(get_chapter_num "$(basename "$f")")
            echo "${chap} ${f}"
        done | sort -k1,1 | awk '{print $2}')

        local file_count
        file_count=$(echo "$sorted_files" | wc -l)

        local group_output="${gopro_dir}/merged_${file_date}_${video_id}.mp4"

        if [ "$file_count" -eq 1 ]; then
            # Single chapter, just copy
            cp "$(echo "$sorted_files" | head -1)" "$group_output"
            log "  Single chapter: video ${video_id} on ${file_date}"
        else
            # Multiple chapters, merge with ffmpeg concat demuxer
            local concat_file="${gopro_dir}/concat_${video_id}.txt"
            echo "$sorted_files" | while read -r f; do
                echo "file '$(realpath "$f")'"
            done > "$concat_file"

            log "  Merging ${file_count} chapters for video ${video_id} on ${file_date}..."
            ffmpeg -y -f concat -safe 0 -i "$concat_file" -c copy "$group_output" \
                -loglevel warning 2>&1 | while read -r line; do log "    ffmpeg: $line"; done

            rm -f "$concat_file"
        fi

        # Collect for per-date merge
        if [ -z "${date_files[$file_date]+x}" ]; then
            date_files[$file_date]="$group_output"
        else
            date_files[$file_date]="${date_files[$file_date]}"$'\n'"$group_output"
        fi
    done

    # Step 3: Merge all videos from the same date into one final file per date
    local date_output_dir="${OUTPUT_DIR}/${gopro_label}"
    mkdir -p "$date_output_dir"

    for file_date in "${!date_files[@]}"; do
        local day_files="${date_files[$file_date]}"
        local day_count
        day_count=$(echo "$day_files" | wc -l)
        local final_output="${date_output_dir}/${gopro_label}_${file_date}.mp4"

        if [ "$day_count" -eq 1 ]; then
            mv "$(echo "$day_files" | head -1)" "$final_output"
            log "  Final (single video): ${final_output}"
        else
            # Sort by filename (which includes video ID) for consistent ordering
            local sorted_day
            sorted_day=$(echo "$day_files" | sort)

            local concat_file="${gopro_dir}/concat_date_${file_date}.txt"
            echo "$sorted_day" | while read -r f; do
                echo "file '$(realpath "$f")'"
            done > "$concat_file"

            log "  Merging ${day_count} videos for ${file_date} into final file..."
            ffmpeg -y -f concat -safe 0 -i "$concat_file" -c copy "$final_output" \
                -loglevel warning 2>&1 | while read -r line; do log "    ffmpeg: $line"; done

            rm -f "$concat_file"
            log "  Final: ${final_output}"
        fi
    done

    log "Merge complete for ${gopro_label}. Output: ${date_output_dir}/"
}

# ======================== Main ========================

main() {
    log "=========================================="
    log "GoPro Fetch & Merge"
    log "=========================================="

    # Check dependencies
    for cmd in curl jq ffmpeg numfmt; do
        if ! command -v "$cmd" &> /dev/null; then
            err "Required command not found: ${cmd}"
            exit 1
        fi
    done

    mkdir -p "$OUTPUT_DIR" "$TEMP_DIR"

    declare -a GOPRO_IPS=()
    declare -a GOPRO_IFACES=()

    # Step 1: Discover GoPros
    discover_gopros

    # Step 2: Download files from each GoPro
    for i in "${!GOPRO_IPS[@]}"; do
        local ip="${GOPRO_IPS[$i]}"
        local iface="${GOPRO_IFACES[$i]}"
        local label="gopro_${iface}"

        log "------------------------------------------"
        log "Processing GoPro at ${ip} (${label})"
        log "------------------------------------------"

        download_gopro_files "$ip" "$label" || {
            err "Failed to download from ${ip}, skipping merge."
            continue
        }

        merge_gopro_files "$label"
    done

    # Step 3: Cleanup temp files
    log "Cleaning up temporary downloads..."
    rm -rf "$TEMP_DIR"

    log "=========================================="
    log "All done! Merged videos are in: ${OUTPUT_DIR}"
    log "=========================================="
    ls -lhR "$OUTPUT_DIR" 2>/dev/null || true
}

main "$@"
