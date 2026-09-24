#!/usr/bin/env bash
# Live HLS publisher — PROOF OF CONCEPT.
#
# Pulls one Zowietek camera over RTSP, transcodes to 720p H.264 on the Jetson's
# hardware blocks, and writes an HLS EVENT playlist with EXT-X-PROGRAM-DATE-TIME.
#
# STANDALONE. This does not import, modify or restart anything in agx_pipeline.
# It is a separate process against the same camera, safe to run while the
# ingestion service is up, and it writes only under $OUT_ROOT.
#
# Why this shape (measured on the box 2026-09-24, see PROOF_OF_CONCEPT.md):
#   - The cameras stream HEVC 3840x2160. Chrome/hls.js cannot play HEVC in HLS,
#     so a stream-copy is impossible — transcode to H.264 is FORCED.
#   - Stock ffmpeg 4.4.2 on this box has NO h264_nvenc, and h264_v4l2m2m fails
#     with "Could not find a valid device". The Jetson encoder is only reachable
#     through the NVIDIA GStreamer elements.
#   - So: GStreamer does the hardware work (nvv4l2decoder -> nvvidconv ->
#     nvv4l2h264enc), pipes MPEG-TS to ffmpeg, and ffmpeg does ONLY the HLS
#     muxing. ffmpeg is what writes PROGRAM-DATE-TIME, which the whole
#     timestamp-rebase design depends on.
#
# Usage:
#   ./live_hls.sh <camera_ip> <angle> [seg_seconds] [out_root]
#   ./live_hls.sh 10.1.10.142 FL 4 /home/dev/live_poc
set -euo pipefail

CAM_IP="${1:?camera ip required}"
ANGLE="${2:?angle required (FL|FR)}"
SEG="${3:-4}"
OUT_ROOT="${4:-/home/dev/live_poc}"

RTSP_PORT="${RTSP_PORT:-554}"
RTSP_PATH="${RTSP_PATH:-/main/av}"
WIDTH="${WIDTH:-1280}"
HEIGHT="${HEIGHT:-720}"
BITRATE="${BITRATE:-2500000}"
FPS="${FPS:-30}"

# How ffmpeg timestamps the incoming TS packets. This single choice decides
# whether PROGRAM-DATE-TIME is trustworthy, and the whole rebase rests on it.
#
#   wallclock (default) — stamp every packet with its ARRIVAL time, so media
#     time is wall-clock time by construction and PDT cannot drift away from it.
#   genpts — let ffmpeg synthesise timestamps from an assumed frame rate.
#     MEASURED BAD on this box: the stream ran ~13% fast and PDT drifted -11.8s
#     inside 100s, which over a 2h game is ~14 minutes of error. Kept only so
#     the comparison can be re-run; do not ship it.
case "${PDT_MODE:-wallclock}" in
  wallclock) FF_INPUT_FLAGS="-use_wallclock_as_timestamps 1" ;;
  genpts)    FF_INPUT_FLAGS="-fflags +genpts" ;;
  *)         echo "unknown PDT_MODE=${PDT_MODE}" >&2; exit 2 ;;
esac

OUT="$OUT_ROOT/$ANGLE"
mkdir -p "$OUT"
rm -f "$OUT"/*.ts "$OUT"/*.m3u8

URL="rtsp://${CAM_IP}:${RTSP_PORT}${RTSP_PATH}"
# Keyframe cadence MUST equal the segment length in frames, or the muxer cannot
# cut on an IDR and segment durations drift off target.
GOP=$(( SEG * FPS ))

echo "[live_hls] angle=$ANGLE src=$URL -> $OUT (${WIDTH}x${HEIGHT} @ $((BITRATE/1000))kbps, ${SEG}s segments, GOP=$GOP)"

# HW: RTSP -> depay -> NVDEC -> HW scale -> NVENC -> TS on stdout.
# `sync=false` keeps the pipeline pulling at source rate rather than clocking to
# the pipeline clock, which is what we want for a live passthrough.
gst-launch-1.0 -q \
  rtspsrc location="$URL" protocols=tcp latency=200 \
  ! rtph265depay ! h265parse ! nvv4l2decoder \
  ! nvvidconv ! "video/x-raw(memory:NVMM),width=${WIDTH},height=${HEIGHT}" \
  ! nvv4l2h264enc bitrate="$BITRATE" iframeinterval="$GOP" idrinterval="$GOP" insert-sps-pps=true \
  ! h264parse ! mpegtsmux ! fdsink fd=1 sync=false \
| ffmpeg -hide_banner -nostdin -loglevel warning \
  $FF_INPUT_FLAGS -f mpegts -i - \
  -map 0:v -c copy \
  -f hls -hls_time "$SEG" -hls_playlist_type event \
  -hls_flags append_list+program_date_time+independent_segments \
  -hls_segment_filename "$OUT/seg_%05d.ts" \
  "$OUT/live.m3u8"
