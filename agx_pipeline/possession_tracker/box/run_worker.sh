#!/bin/bash
# Start the possession-tracker queue worker if it is not already running (idempotent, so cron
# can call it every few minutes as a keep-alive and at boot). Logs to /home/dev/possession/worker.log.
#   crontab:  @reboot     bash /home/dev/possession/run_worker.sh
#             */5 * * * * bash /home/dev/possession/run_worker.sh
# Stop:  touch /home/dev/possession/WORKER_OFF   (the keep-alive then leaves it stopped)
cd /home/dev/possession || exit 1
[ -f WORKER_OFF ] && exit 0
if pgrep -f "python3 /home/dev/possession/queue_worker.py" > /dev/null; then exit 0; fi
set -a; . /home/dev/gopro-automation-linux/.env.agx; set +a
. /home/dev/sam3env.sh
setsid nohup python3 /home/dev/possession/queue_worker.py >> /home/dev/possession/worker.log 2>&1 < /dev/null &
