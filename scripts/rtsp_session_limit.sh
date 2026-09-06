#!/bin/bash
# P3 feasibility, stage 1: how many concurrent RTSP sessions will a camera serve?
#
# P3 (continuous 1080p proxy on all four angles) puts THREE sessions on every
# camera -- the 4K master recorder, the audio-only pull, and the proxy. FL and
# FR already carry three every game night; NL and NR carry two. So the question
# is not "can a camera do three" but the two things that would actually bite:
#
#   1. Where is the ceiling? RTSP signals a session limit as 453 (Not Enough
#      Bandwidth) or 503, and some encoders just refuse the TCP connect.
#   2. Does attaching a NEW session disturb the ones already connected? That is
#      the dangerous one: a session opened for a proxy must not degrade the
#      session recording the master.
#
# Sessions are therefore started STAGGERED, not all at once, and each one's
# progress is sampled every second via ffmpeg -progress. A healthy session
# advances ~fps frames per second throughout; one that stalls or slows the
# moment a later session attaches is the failure we are looking for.
#
# TCP because that is what production uses (recording.py _single_cam_gst sets
# protocols=tcp; the audio pull and highlight buffer both pass
# -rtsp_transport tcp).
#
# RUN IT WHEN NOTHING IS RECORDING. It opens real sessions on a real camera.
#
#   ./rtsp_session_limit.sh 10.1.10.142        # FL, 6 sessions, 180s each
#   ./rtsp_session_limit.sh 10.1.10.51 8 240   # NL -- the flaky one, worth its own run
#
set -u

IP=${1:-10.1.10.142}
N=${2:-6}
SECS=${3:-180}
STAGGER=${4:-20}
PORT=${RTSP_PORT:-554}
PATH_=${RTSP_PATH:-/main/av}
URL="rtsp://$IP:$PORT$PATH_"
OUT=${OUTDIR:-$HOME/rtsptest/$IP}

command -v ffmpeg >/dev/null || { echo "ffmpeg not found"; exit 1; }

# Cheap guard against running this mid-game. Not authoritative -- camrec may be
# on a different port, or recording may be driven by the built-in controller --
# so it warns rather than refuses.
if curl -sf --max-time 3 "${CAMREC_URL:-http://localhost:8000}/api/status" 2>/dev/null \
   | grep -q '"busy": *true'; then
  echo "!! camrec reports a camera BUSY -- a recording may be in progress."
  echo "!! Ctrl-C now unless you are sure. Continuing in 10s..."
  sleep 10
fi

mkdir -p "$OUT" && rm -f "$OUT"/s*.prog "$OUT"/s*.err
echo "camera   $URL"
echo "sessions $N, ${SECS}s each, ${STAGGER}s apart -> $OUT"
echo

t0=$(date +%s)
for i in $(seq 1 "$N"); do
  # -c copy: NEVER decode. Production stream-copies (recording.py, the audio
  # pull, the highlight buffer are all -c copy / stream copy), so decoding here
  # would test this box's 4K HEVC software decode instead of the camera --
  # which is exactly what an earlier version of this script did, showing every
  # session at ~11fps and blaming the camera for the test's own load.
  ffmpeg -hide_banner -nostdin -rtsp_transport tcp -i "$URL" \
         -t "$SECS" -an -c copy -f null - \
         -progress "$OUT/s$i.prog" >/dev/null 2>"$OUT/s$i.err" &
  echo "$(date +%T)  +$(( $(date +%s) - t0 ))s  started session $i (pid $!)"
  [ "$i" -lt "$N" ] && sleep "$STAGGER"
done
wait
echo

# A session that never produced a progress block never connected. One whose
# frame count stops advancing stalled -- and WHEN it stalled, relative to the
# stagger, says which new session disturbed it.
printf "%-4s %-8s %-9s %-8s %s\n" ses frames dur secs/gap note
for i in $(seq 1 "$N"); do
  p="$OUT/s$i.prog"; e="$OUT/s$i.err"
  if [ ! -s "$p" ]; then
    why=$(grep -m1 -iE '453|503|refused|denied|unauthorized|timed? ?out|Invalid data' "$e" \
          2>/dev/null | cut -c1-60)
    printf "%-4s %-8s %-9s %-8s %s\n" "$i" NONE - - "NO CONNECT: ${why:-see s$i.err}"
    continue
  fi
  frames=$(grep -a '^frame=' "$p" | tail -1 | cut -d= -f2)
  dur=$(grep -a '^out_time_ms=' "$p" | tail -1 | cut -d= -f2)
  dur=$(( ${dur:-0} / 1000000 ))
  # Largest jump in out_time between consecutive progress blocks: >3s means the
  # stream stopped advancing for that long.
  gap=$(grep -a '^out_time_ms=' "$p" | cut -d= -f2 \
        | awk 'NR>1{d=($1-p)/1000000; if(d>m)m=d} {p=$1} END{printf "%.1f", m+0}')
  errs=$(grep -ciE 'error|dropped|corrupt|missed' "$e" 2>/dev/null); errs=${errs:-0}
  note="ok"
  [ "${gap%.*}" -ge 3 ] 2>/dev/null && note="STALLED ${gap}s"
  [ "$errs" -gt 0 ] && note="$note; $errs err line(s)"
  printf "%-4s %-8s %-9s %-8s %s\n" "$i" "${frames:-?}" "${dur}s" "$gap" "$note"
done

cat <<'NOTE'

Expect ~fps*secs frames per session (30fps -> ~5400 in 180s). Every session
well under that, including ones that ran alone, means the BOX is the limit,
not the camera -- check that -c copy is really in the ffmpeg line above.

Reading it:
  all sessions similar frames, gap < 3s, no errors
      -> the camera serves N concurrently; P3's three per camera is a non-issue
  session k shows NO CONNECT
      -> the ceiling is k-1; the reason is in its .err file
  an EARLIER session stalls or errors around the time a later one starts
      -> attaching a session degrades existing ones. This is the finding that
         would block P3, because the degraded one could be a master recording.
NOTE
