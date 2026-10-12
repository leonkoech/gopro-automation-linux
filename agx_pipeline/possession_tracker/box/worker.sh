# worker.sh <worker-index> <n-workers>: perceive every n-th clip of full_queue.txt, never during a game
. /home/dev/sam3env.sh; cd /home/dev/possession
i=0
while read -r out f; do
  i=$((i+1)); [ $(( (i-1) % $2 )) -eq $(( $1 )) ] || continue
  n=$(basename $f .mp4); [ -f $out/cache/$n.npz ] && continue
  while curl -s localhost:5000/health | grep -q "\"recording\":true"; do sleep 60; done
  timeout 1200 python3 perceive.py $f $out 5.0 < /dev/null 2>&1 | grep -E "frames|Error" | sed "s/^/[w$1] /"
done < full_queue.txt
echo "WORKER $1 DONE"
