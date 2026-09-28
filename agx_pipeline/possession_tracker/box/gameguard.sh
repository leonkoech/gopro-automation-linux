# kills SAM3 perception the moment the box starts recording a game; workers wait until it stops
while pgrep -f "bash worker.sh" > /dev/null; do
  if curl -s localhost:5000/health | grep -q "\"recording\":true"; then
    for p in $(pgrep -f "python3 perceive.py"); do kill $p; echo "$(date +%H:%M) game started: killed perceive $p"; done
  fi
  sleep 15
done
