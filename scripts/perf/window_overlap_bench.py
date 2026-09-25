#!/usr/bin/env python3
"""
Does the scan window double the detector's work? Measure before changing it.

live.py's _process_window prepends the PREVIOUS segment to the current one
(SHOT_LIVE_WINDOW, default true) so a shot straddling the 4s boundary is whole
in one window. It does that by stream-copy concatenating the two files and
scanning the result -- so every segment is decoded AND inferred twice: once as
"current", again as "prev" in the next window.

The proposed fix caches each segment's track and builds the window by joining
TRACKS instead of files, which would halve decode and inference together. This
measures the ceiling of that win before anyone writes it. No production code is
touched; it cuts its own segments from a master and scans them.

Three timings over the same footage:

  concat        the ffmpeg stream copy + temp file the current path pays per window
  scan 1 seg    what a cached implementation would scan per window
  scan 2 segs   what the current path actually scans per window

  current per window = concat + scan2        cached per window = scan1

Uses the live loop's own settings, not detect.py's: stride from SHOT_LIVE_STRIDE
(4) and imgsz from SHOT_DET_IMGSZ defaulting to 640 the way live.py:200 does,
NOT detect.py:30's 1280. Set SHOT_DET_WEIGHT to a .engine to measure the
TensorRT path instead.

    python3 scripts/perf/window_overlap_bench.py --video <SL master>.mp4
    python3 scripts/perf/window_overlap_bench.py --video ... --at 900 --repeats 5
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(cmd):
    subprocess.run(cmd, check=True, stdin=subprocess.DEVNULL,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def cut(src: str, start: float, secs: float, out: str) -> None:
    """Stream-copy one segment. -ss before -i seeks to a keyframe, so the cut is
    approximate -- frame counts are reported so the comparison stays honest."""
    _run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-ss", f"{start:.3f}",
          "-i", src, "-t", f"{secs:.3f}", "-c", "copy", "-map", "0:v", out])


def nframes(path: str) -> int:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-count_frames",
         "-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", path],
        capture_output=True, text=True).stdout
    return int("".join(c for c in out if c.isdigit()) or 0)


def concat(a: str, b: str, out: str) -> float:
    """Replicates live.py's _concat. Returns seconds."""
    lst = out + ".txt"
    with open(lst, "w") as f:
        f.write("file '%s'\nfile '%s'\n" % (a, b))
    t0 = time.perf_counter()
    _run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-f", "concat",
          "-safe", "0", "-i", lst, "-c", "copy", "-map", "0:v", out])
    secs = time.perf_counter() - t0
    os.remove(lst)
    return secs


def main() -> int:
    ap = argparse.ArgumentParser(description="Measure the scan-window overlap cost")
    ap.add_argument("--video", required=True, help="an SL/SR master")
    ap.add_argument("--at", type=float, default=600.0, help="seconds into the file")
    ap.add_argument("--seg", type=float, default=4.0, help="= SHOT_SEGMENT_SEC")
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()

    if not os.path.isfile(a.video):
        raise SystemExit(f"no such video: {a.video}")

    stride = int(os.getenv("SHOT_LIVE_STRIDE", os.getenv("SHOT_AUTO_STRIDE", "4")))
    imgsz = int(os.getenv("SHOT_DET_IMGSZ", "640"))       # live.py:200, not detect.py:30

    from agx_pipeline.shot_detect import node
    from agx_pipeline.shot_detect.backtest import scan
    from agx_pipeline.shot_detect.detect import ShotDetector

    weight = os.getenv("SHOT_DET_WEIGHT", node._DEFAULT_WEIGHT)
    print(f"weight  {os.path.basename(weight)}")
    print(f"stride  {stride}   imgsz {imgsz}   segment {a.seg}s   repeats {a.repeats}\n")
    det = ShotDetector(weight)

    tmp = tempfile.mkdtemp(prefix="winbench_")
    A, B, AB = f"{tmp}/a.mp4", f"{tmp}/b.mp4", f"{tmp}/ab.mp4"
    try:
        cut(a.video, a.at, a.seg, A)
        cut(a.video, a.at + a.seg, a.seg, B)
        na, nb = nframes(A), nframes(B)
        if not na or not nb:
            raise SystemExit("cut produced no frames — try a different --at")
        t_cat = min(concat(A, B, AB) for _ in range(a.repeats))
        nab = nframes(AB)
        print(f"segments: A={na} frames, B={nb} frames, concat={nab} frames\n")

        def timed(path):
            scan.scan_ball_and_hoops(det.model, path, det.device,
                                     stride=stride, imgsz=imgsz)   # warm
            best = None
            for _ in range(a.repeats):
                t0 = time.perf_counter()
                tr, _h = scan.scan_ball_and_hoops(det.model, path, det.device,
                                                  stride=stride, imgsz=imgsz)
                s = time.perf_counter() - t0
                best = s if best is None else min(best, s)
            return best, len(tr)

        t1, n1 = timed(B)
        t2, n2 = timed(AB)
    finally:
        for p in (A, B, AB):
            try:
                os.remove(p)
            except OSError:
                pass
        try:
            os.rmdir(tmp)
        except OSError:
            pass
    det.empty_cache()

    cur = t_cat + t2
    print(f"  {'concat (stream copy)':<26}{t_cat:7.2f}s")
    print(f"  {'scan 1 segment':<26}{t1:7.2f}s   {n1:4d} ball detections")
    print(f"  {'scan 2 segments (window)':<26}{t2:7.2f}s   {n2:4d} ball detections")
    print(f"\n  current per window   concat + scan2 = {cur:.2f}s")
    print(f"  cached  per window   scan1          = {t1:.2f}s")
    saving = 1.0 - (t1 / cur) if cur else 0.0
    print(f"  saving               {saving*100:.0f}%   ({cur/t1:.2f}x less work)"
          if t1 else "")

    print(f"""
  SL and SR each close a {a.seg:.0f}s segment every {a.seg:.0f}s, so the loop must finish
  TWO windows per {a.seg:.0f}s of play. It currently needs {2*cur:.1f}s -> {2*cur/a.seg:.2f}x realtime
  ({'KEEPING UP' if 2*cur <= a.seg else 'BEHIND — the queue grows until MAX_AGE_S discards it'}).
  With the cache that becomes {2*t1:.1f}s -> {2*t1/a.seg:.2f}x realtime.

  scan2 should land near 2x scan1. If it does, the overlap really is doubling
  the work and a track cache recovers it. If it is much less than 2x, per-scan
  fixed costs dominate and the win is smaller than it looks.""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
