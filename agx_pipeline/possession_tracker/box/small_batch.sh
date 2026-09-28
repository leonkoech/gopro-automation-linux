# re-encode a review batch to small H.264 files for a slow link: small_batch.sh <batch_dir>
cd "$1" && mkdir -p small && for f in *.mp4; do
  [ -f small/$f ] || nice -n 19 ffmpeg -nostdin -v error -y -i "$f" -c:v libx264 -preset veryfast -crf 30 -pix_fmt yuv420p small/$f; done
cp INDEX.md small/ 2>/dev/null; du -sh small
