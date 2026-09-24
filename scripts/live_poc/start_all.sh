#!/bin/bash
# Launch the PoC publishers for FL and FR, detached.
pkill -f live_hls.sh 2>/dev/null
pkill -f "gst-launch.*rtsp" 2>/dev/null
sleep 1
rm -rf /home/dev/live_poc
mkdir -p /home/dev/live_poc
setsid /home/dev/live_poc_bin/live_hls.sh 10.1.10.142 FL 4 /home/dev/live_poc > /home/dev/live_poc/FL.log 2>&1 < /dev/null &
setsid /home/dev/live_poc_bin/live_hls.sh 10.1.10.92  FR 4 /home/dev/live_poc > /home/dev/live_poc/FR.log 2>&1 < /dev/null &
sleep 1
echo started
