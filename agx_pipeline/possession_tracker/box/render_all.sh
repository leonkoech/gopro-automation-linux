# re-score, then render + shrink every complete batch of 10 not yet rendered, both games
cd /home/dev/possession
bash score_now.sh v7 > score_v7.log 2>&1
. /home/dev/sam3env.sh
for g in cb9e1294 7cef734e; do
  o=out; [ $g = 7cef734e ] && o=out7
  n=$(ls $o/cache | grep -vc "^L_")
  for b in $(seq 1 $((n / 10))); do
    d=$o/review_v7/batch_$(printf %02d $b)
    [ -f $d/small/INDEX.md ] && continue
    python3 render_batch.py $g $b > /dev/null 2>&1 && bash small_batch.sh $d > /dev/null 2>&1
    echo "$g batch $b ready"
  done
done
echo RENDER_ALL_DONE
