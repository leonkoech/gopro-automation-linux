#!/bin/bash
# Bounded 2-minute live stream test: publish, upload, stop cleanly, finalize.
ROOT=/home/dev/live_test
SESS=test-$(date -u +%Y%m%dT%H%M%SZ)
echo "$SESS" > /home/dev/live_test_session
rm -rf "$ROOT"; mkdir -p "$ROOT"
rm -f /home/dev/live_test_DONE

set -a; source /home/dev/app/.env.agx 2>/dev/null || source /home/dev/.env.agx 2>/dev/null; set +a

PDT_MODE=wallclock setsid /home/dev/live_poc_bin/live_hls.sh 10.1.10.142 FL 4 "$ROOT" > "$ROOT/FL.log" 2>&1 </dev/null &
PDT_MODE=wallclock setsid /home/dev/live_poc_bin/live_hls.sh 10.1.10.92  FR 4 "$ROOT" > "$ROOT/FR.log" 2>&1 </dev/null &
sleep 3
setsid timeout 170 python3 /home/dev/live_poc_bin/hls_uploader.py "$ROOT/FL" "live-poc/$SESS/FL" > "$ROOT/upFL.log" 2>&1 </dev/null &
setsid timeout 170 python3 /home/dev/live_poc_bin/hls_uploader.py "$ROOT/FR" "live-poc/$SESS/FR" > "$ROOT/upFR.log" 2>&1 </dev/null &

# 120s of footage, then stop the way _do_stop would: SIGINT ffmpeg so it
# finalizes the playlist with EXT-X-ENDLIST, then tear down gst.
sleep 120
pkill -INT -f "ffmpeg -hide_banner -nostdin -loglevel warning" 2>/dev/null
sleep 5
pkill -9 -f "gst-launch-1.0 -q rtspsrc" 2>/dev/null
pkill -9 -f "live_hls.sh" 2>/dev/null
sleep 2

# final pass so the last segments + the finalized playlist reach S3
python3 /home/dev/live_poc_bin/hls_uploader.py "$ROOT/FL" "live-poc/$SESS/FL" --once >> "$ROOT/upFL.log" 2>&1
python3 /home/dev/live_poc_bin/hls_uploader.py "$ROOT/FR" "live-poc/$SESS/FR" --once >> "$ROOT/upFR.log" 2>&1
pkill -f hls_uploader.py 2>/dev/null
echo "$SESS" > /home/dev/live_test_DONE
