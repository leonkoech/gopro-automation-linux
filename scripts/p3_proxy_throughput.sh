#!/bin/bash
# P3 feasibility, stage 2: can this box sustain N continuous 4K->1080p proxies?
#
# P3 transcodes every tracking angle for the whole game instead of converting
# masters afterwards. That is N concurrent pipelines running at 1x realtime,
# forever, on ONE NVDEC and ONE NVENC (confirmed 2026-09-05: the AGX Orin has
# one of each, not the two an earlier draft assumed).
#
# The empirical case says it should fit: post-game the box sustains ~7x realtime
# in aggregate through the same two engines, and during games they sit ~96% idle.
# Four continuous streams need 4x realtime. This is the test of that.
#
# Pass = every pipeline produces ~fps*secs frames. A pipeline that cannot keep
# up with a LIVE source does not slow down -- it drops. So a short frame count,
# not a long runtime, is the failure signal.
#
# Writes only to scratch. Nothing in the production path is touched, so this is
# safe to run alongside anything except a recording (it adds a second RTSP pull
# per camera and real load on the video engines).
#
# NR (10.1.10.76) is offline with a lens issue, so only three cameras are
# reachable. To still load the box with four pipelines, the list is cycled --
# the 4th pulls a second session from the 1st camera. Legitimate here because
# stage 2 measures THIS BOX's engines, not the cameras, and stage 1 already
# showed a camera serves six sessions untroubled.
#
#   ./p3_proxy_throughput.sh            # 4 pipelines, 180s
#   ./p3_proxy_throughput.sh 2 300      # back off to 2 if 4 fails
#   CAMS="10.1.10.142 10.1.10.51" ./p3_proxy_throughput.sh 2
#
set -u

N=${1:-4}
SECS=${2:-180}
CAMS=${CAMS:-"10.1.10.142 10.1.10.51 10.1.10.92"}   # FL NL FR (NR offline)
FPS=${FPS:-30}
BITRATE=${BITRATE:-8000000}                          # matches ingest.py HW_BITRATE
PORT=${RTSP_PORT:-554}
RPATH=${RTSP_PATH:-/main/av}
OUT=${OUTDIR:-$HOME/p3test}

command -v gst-launch-1.0 >/dev/null || { echo "gst-launch-1.0 not found"; exit 1; }
for el in nvv4l2decoder nvvidconv nvv4l2h264enc; do
  gst-inspect-1.0 "$el" >/dev/null 2>&1 || { echo "missing GStreamer element: $el"; exit 1; }
done

if curl -sf --max-time 3 "${CAMREC_URL:-http://localhost:8000}/api/status" 2>/dev/null \
   | grep -q '"busy": *true'; then
  echo "!! camrec reports a camera BUSY -- a recording may be in progress."
  echo "!! Ctrl-C now unless you are sure. Continuing in 10s..."
  sleep 10
fi

rm -rf "$OUT"; mkdir -p "$OUT"
set -- $CAMS
ncams=$#
echo "$N pipeline(s), ${SECS}s, 4K H.265 -> NVDEC -> VIC 1080p -> NVENC H.264"
echo "cameras: $CAMS  (cycled if N > $ncams)"
echo "out:     $OUT"
echo

# idrinterval as well as iframeinterval: nvv4l2h264enc defaults idrinterval to
# 256 (~8.5s at 30fps) and players can only seek to IDR frames. Same reasoning
# as ingest.py:163. maxperf-enable=1 races the encode to idle.
#
# Raw H.264 elementary stream, NOT mp4. Three reasons: mp4mux rejects
# nvv4l2h264enc's output with "Buffer has no PTS" and dies after ~3s; a muxed
# file needs clean EOS to write its moov atom, so any rough shutdown produces an
# unreadable file and a frame count that lies; and this test only needs frames
# counted, not a playable proxy. A partial .h264 is always countable.
# The real P3 implementation WILL need a container, so the PTS problem has to be
# solved there -- likely mpegtsmux, or setting timestamps on the encoder output.
pids=()
for i in $(seq 1 "$N"); do
  eval "ip=\${$(( (i - 1) % ncams + 1 ))}"
  url="rtsp://$ip:$PORT$RPATH"
  # `r. ! application/x-rtp,media=video` -- these cameras carry AAC on the same
  # RTSP session, and without this rtspsrc can bind the audio pad to
  # rtph265depay. recording.py:151 does the same thing for the same reason.
  gst-launch-1.0 -e \
    rtspsrc "location=$url" protocols=tcp latency=200 name=r \
    r. ! "application/x-rtp,media=video" \
    ! rtph265depay ! h265parse \
    ! nvv4l2decoder \
    ! nvvidconv ! "video/x-raw(memory:NVMM),width=1920,height=1080" \
    ! nvv4l2h264enc bitrate=$BITRATE iframeinterval=$FPS idrinterval=$FPS \
      maxperf-enable=1 \
    ! h264parse ! "video/x-h264,stream-format=byte-stream,alignment=au" \
    ! filesink "location=$OUT/p$i.h264" \
    >"$OUT/p$i.log" 2>&1 &
  pids+=($!)
  echo "started pipeline $i -> $ip (pid $!)"
  # Small stagger. Production never opens N RTSP sessions at the same instant --
  # camrec starts one container per camera, sequentially -- and six simultaneous
  # SETUPs have been seen to fail with "Failed to connect (Generic error)" on
  # cameras that serve the same six happily when they arrive spread out.
  [ "$i" -lt "$N" ] && sleep "${START_STAGGER:-2}"
done

echo
echo "running ${SECS}s; watch engines with:  sudo tegrastats  /  jtop"
sleep 5
# Fail fast: if a pipeline died in the first seconds it is a build/link problem,
# not a throughput one, and there is no point waiting out the full run.
dead=()
for i in $(seq 1 "$N"); do
  kill -0 "${pids[$((i-1))]}" 2>/dev/null || dead+=("$i")
done
if [ ${#dead[@]} -gt 0 ]; then
  echo "!! pipeline(s) ${dead[*]} exited within 5s -- a pipeline error, not saturation."
  for i in "${dead[@]}"; do
    echo "--- p$i.log ---"; tail -4 "$OUT/p$i.log"
  done
  echo "!! surviving pipelines are still running; stopping them."
  kill -INT "${pids[@]}" 2>/dev/null; wait 2>/dev/null
  exit 1
fi
sleep $(( SECS - 5 ))
# SIGINT to the gst-launch PIDs directly, not pkill -f: gst-launch -e turns INT
# into EOS so mp4mux finalises the moov atom, and a pattern match would also hit
# any wrapper process, leaving the real one to be killed some other way -- an
# unfinalised file that ffprobe cannot read.
kill -INT "${pids[@]}" 2>/dev/null
wait
echo

want=$(( FPS * SECS ))
printf "%-5s %-9s %-9s %-7s %s\n" pipe frames want size note
for i in $(seq 1 "$N"); do
  f="$OUT/p$i.h264"
  if [ ! -s "$f" ]; then
    printf "%-5s %-9s %-9s %-7s %s\n" "$i" NONE "$want" - "NO OUTPUT: see p$i.log"
    continue
  fi
  got=$(ffprobe -v error -select_streams v:0 -count_frames \
        -show_entries stream=nb_read_frames -of csv=p=0 "$f" 2>/dev/null | tr -dc 0-9)
  sz=$(du -h "$f" | cut -f1)
  pctv=$(awk -v g="${got:-0}" -v w="$want" 'BEGIN{printf "%.0f", (w?100*g/w:0)}')
  note="ok (${pctv}% of realtime)"
  [ "$pctv" -lt 95 ] 2>/dev/null && note="BEHIND -- only ${pctv}% of realtime"
  warn=$(grep -ciE 'WARN|ERROR|dropped|underflow' "$OUT/p$i.log" 2>/dev/null); warn=${warn:-0}
  [ "$warn" -gt 0 ] && note="$note; $warn warn/err line(s)"
  printf "%-5s %-9s %-9s %-7s %s\n" "$i" "${got:-?}" "$want" "$sz" "$note"
done

cat <<'NOTE'

Reading it:
  every pipeline >=95% of realtime, no warnings
      -> the box sustains this many continuous proxies. P3's throughput
         question is answered for N angles.
  one or more BEHIND
      -> saturated. Re-run with fewer to find the ceiling. Note the limit could
         be NVDEC, NVENC, VIC or DRAM bandwidth, and Jetson reports utilisation
         for none of them (Appendix E) -- so this finds the number, not the
         reason.
  NO OUTPUT
      -> pipeline failed to build or connect; the .log says which element.

This is stage 2 of 4 and deliberately runs with nothing else happening. It does
NOT show whether capture survives this load during a game -- that is stage 3,
which runs the same pipelines alongside a real recording and judges on capture
health, not on these frame counts.
NOTE
