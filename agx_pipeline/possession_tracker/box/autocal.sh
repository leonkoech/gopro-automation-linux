# per-game court-line fit for one validation game: calib_check (golden report + empty court), calib_fit
g=$1; V=/home/dev/validate/$g; O=/home/dev/validate/$g/calib; mkdir -p $O
cd /home/dev/confirm_test
for cam in FL FR; do
  python3 calib_check.py $V/$cam.mp4 $cam $O/${cam}_golden /home/dev/shot_typing > $O/${cam}_golden.log 2>&1
  EMPTY=$(ls $O/${cam}_golden*empty*.jpg 2>/dev/null | head -1)
  python3 calib_fit.py "$EMPTY" $cam $O/calib_arcs_$cam.json /home/dev/shot_typing > $O/${cam}_fit.log 2>&1
  python3 calib_check.py $V/$cam.mp4 $cam $O/${cam}_fit $O "$EMPTY" > $O/${cam}_fitcheck.log 2>&1
  echo "== $cam"; tail -4 $O/${cam}_golden.log; tail -2 $O/${cam}_fit.log; tail -4 $O/${cam}_fitcheck.log
done
echo AUTOCAL_DONE
