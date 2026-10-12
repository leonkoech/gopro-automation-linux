# fast perception (YOLO-seg + ByteTrack) on every clip of both games; waits during games
. /home/dev/sam3env.sh; cd /home/dev/possession
for pair in "out_fast cb9e1294" "out7_fast 7cef734e_sync"; do
  set -- $pair
  for f in /home/dev/confirm_test/clips/$2/*.mp4; do
    n=$(basename $f .mp4); [ -f $1/cache/$n.npz ] && continue
    while curl -s localhost:5000/health | grep -q "\"recording\":true"; do sleep 60; done
    timeout 300 python3 perceive_fast.py $f $1 5.0 < /dev/null 2>&1 | grep -E "total|Error"
  done
done
for f in /home/dev/possession/lclips/*.mp4; do n=$(basename $f .mp4); [ -f out_fast/cache/$n.npz ] || timeout 300 python3 perceive_fast.py $f out_fast 8.0 < /dev/null 2>&1 | grep -E "total|Error"; done
echo FAST_ALL_DONE
