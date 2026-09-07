#!/bin/bash
# P3 feasibility, stage 4: is the P3 proxy as good as today's post-game proxy?
#
# Both paths decode 4K H.265 on NVDEC and encode 1080p H.264 on NVENC at the
# same bitrate. Exactly ONE thing differs, and it is the reason this test
# exists:
#
#   today (ingest.py:139 _transcode_hw)   nvv4l2decoder ! nvvideoconvert ! enc
#   P3    (scripts/p3_proxy_throughput)   nvv4l2decoder ! nvvidconv     ! enc
#
# nvvideoconvert is DeepStream's converter; nvvidconv is the L4T one. Both
# scale on the VIC and both are "hardware", but they are different elements and
# nothing guarantees they scale identically. If P3 is going to replace the
# post-game transcode, its output has to be at least as good.
#
# Method: run ONE real 4K master through both pipelines, then score each output
# against the same reference -- the master itself, downscaled to 1080p by
# ffmpeg's swscale (a third, independent scaler, so neither GStreamer element
# is being graded by its own ruler). SSIM and PSNR both, since they disagree in
# useful ways: PSNR punishes any pixel difference, SSIM tracks structure.
#
# File-based on purpose. No cameras, no game, no live streams -- run it any
# time, and it is exactly reproducible because both paths see identical input.
#
#   ./p3_proxy_quality.sh /path/to/master.mp4
#   ./p3_proxy_quality.sh /path/to/master.mp4 120     # only the first 120s
#
set -u

SRC=${1:?usage: p3_proxy_quality.sh <4K-master.mp4> [seconds]}
SECS=${2:-60}
BITRATE=${BITRATE:-8000000}   # matches ingest.py HW_BITRATE
FPS=${FPS:-30}
OUT=${OUTDIR:-$HOME/p3quality}

[ -f "$SRC" ] || { echo "no such file: $SRC"; exit 1; }
command -v gst-launch-1.0 >/dev/null || { echo "gst-launch-1.0 not found"; exit 1; }
command -v ffmpeg >/dev/null || { echo "ffmpeg not found"; exit 1; }
for el in nvv4l2decoder nvvidconv nvvideoconvert nvv4l2h264enc; do
  gst-inspect-1.0 "$el" >/dev/null 2>&1 \
    || echo "note: $el missing -- its half of the comparison will be skipped"
done

mkdir -p "$OUT"
echo "source:  $SRC"
echo "window:  first ${SECS}s"
echo "out:     $OUT"
echo

# A short cut first, so every stage sees the same frames and a long master does
# not make this a 40-minute test. Stream copy from a keyframe, no re-encode, so
# the cut itself cannot affect quality.
CUT="$OUT/cut.mp4"
if [ ! -s "$CUT" ]; then
  echo "cutting ${SECS}s of source..."
  ffmpeg -v error -y -i "$SRC" -t "$SECS" -c copy -an "$CUT" || exit 1
fi

# The reference: the same frames at 1080p, scaled by ffmpeg rather than by
# either element under test. Lanczos because it is a high-quality resampler and
# the reference should not be the weakest link.
REF="$OUT/reference_1080p.mp4"
if [ ! -s "$REF" ]; then
  echo "building 1080p reference (ffmpeg/swscale, lanczos)..."
  ffmpeg -v error -y -i "$CUT" -vf "scale=1920:1080:flags=lanczos" \
         -c:v libx264 -preset slow -crf 12 -an "$REF" || exit 1
fi

# The two candidates. Identical but for the converter element.
run_pipeline() {   # $1 = converter element, $2 = output file
  local conv="$1" out="$2"
  gst-inspect-1.0 "$conv" >/dev/null 2>&1 || return 1
  rm -f "$out"
  gst-launch-1.0 -e \
    filesrc "location=$CUT" ! qtdemux ! h265parse \
    ! nvv4l2decoder \
    ! "$conv" ! "video/x-raw(memory:NVMM),width=1920,height=1080" \
    ! nvv4l2h264enc bitrate=$BITRATE iframeinterval=$FPS idrinterval=$FPS \
      maxperf-enable=1 \
    ! h264parse ! "video/x-h264,stream-format=byte-stream,alignment=au" \
    ! filesink "location=$out" >"$out.log" 2>&1
}

echo "encoding via nvvideoconvert (today's ingest path)..."
run_pipeline nvvideoconvert "$OUT/today.h264" && A="$OUT/today.h264" || A=""
echo "encoding via nvvidconv (the P3 path)..."
run_pipeline nvvidconv "$OUT/p3.h264" && B="$OUT/p3.h264" || B=""
echo

# ---------------------------------------------------------------- settings
# Objective and cheap, and this is the check that has actually caught a real
# defect before: nvv4l2h264enc defaults idrinterval to 256 (~8.5s at 30fps),
# and players can only seek to IDR frames -- the cause of multi-second seek
# stalls in the annotation editor. Expect gaps of ~1s.
settings() {
  local f="$1" name="$2"
  [ -s "$f" ] || { echo "$name: NO OUTPUT (see $f.log)"; return; }
  local dims frames
  dims=$(ffprobe -v error -select_streams v:0 \
         -show_entries stream=width,height,avg_frame_rate -of csv=p=0 "$f")
  frames=$(ffprobe -v error -select_streams v:0 -count_frames \
           -show_entries stream=nb_read_frames -of csv=p=0 "$f" | tr -dc 0-9)
  local idr
  idr=$(ffprobe -v error -select_streams v:0 -show_entries frame=key_frame,pts_time \
        -of csv=p=0 "$f" 2>/dev/null \
        | awk -F, '$1==1{if(p!=""){d=$2-p; s+=d; n++} p=$2} END{if(n)printf "%.2fs mean over %d", s/n, n+1; else print "NONE FOUND"}')
  printf "%-10s %-22s frames=%-7s size=%-7s IDR every %s\n" \
    "$name" "$dims" "${frames:-?}" "$(du -h "$f" | cut -f1)" "$idr"
}

echo "── encoder settings ────────────────────────────────────────────────"
printf "%-10s %-22s %s\n" "path" "WxH,fps" "checks"
settings "$A" "today"
settings "$B" "p3"
echo

# ---------------------------------------------------------------- fidelity
# SSIM and PSNR against the independent reference. Both are computed by ffmpeg
# and need no special build (unlike VMAF, which needs libvmaf compiled in).
score() {
  local f="$1" name="$2"
  [ -s "$f" ] || return
  local ssim psnr
  ssim=$(ffmpeg -v error -r $FPS -i "$f" -i "$REF" -lavfi ssim -f null - 2>&1 \
         | grep -o 'All:[0-9.]*' | head -1 | cut -d: -f2)
  psnr=$(ffmpeg -v error -r $FPS -i "$f" -i "$REF" -lavfi psnr -f null - 2>&1 \
         | grep -o 'average:[0-9.]*' | head -1 | cut -d: -f2)
  printf "%-10s SSIM %-9s PSNR %s dB\n" "$name" "${ssim:-?}" "${psnr:-?}"
}

echo "── fidelity vs 1080p reference ─────────────────────────────────────"
score "$A" "today"
score "$B" "p3"

cat <<'NOTE'

Reading it:
  SSIM within ~0.005 and PSNR within ~0.5 dB
      -> the two scalers are equivalent. P3 costs nothing in quality and
         stage 4 passes.
  p3 materially lower
      -> nvvidconv is the weaker scaler here. Options: use nvvideoconvert in
         the P3 pipeline instead (it is available on the host), or raise the
         P3 bitrate to compensate.
  p3 materially HIGHER
      -> worth knowing too; today's ingest proxies could be improved for free.
  IDR spacing not ~1s on either path
      -> a defect regardless of the comparison. Annotation-editor seeking
         depends on it.

SSIM/PSNR measure fidelity to the reference, not whether a human finds the
clip acceptable. Look at one before signing this off -- especially at the
hoop, where the detail actually matters.
NOTE
