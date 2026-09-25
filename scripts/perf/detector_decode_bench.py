#!/usr/bin/env python3
"""
Detector throughput levers that stack with TensorRT. Two test plans in one tool.

The live loop is backlogged because it is slow: it polls closed ~4s segments
and cannot drain them as fast as they arrive (live.py's own comment says "~2x
too slow"), so a queue builds and MAX_AGE_S=480 discards the tail. That is why
the measured p90 sits within seconds of 480. Anything that raises segments/sec
helps; nothing else does. TensorRT is the biggest such lever and is already
planned -- these are the ones that add to it rather than substitute for it.

TEST 1 -- grayscale decode
    detect.py decodes `-pix_fmt bgr24` (lines 195 and 258) for a model the code
    itself calls "the YOLO v3 grayscale ball detector", fed by a BFS-PGE-04S2M,
    a MONO camera. So ffmpeg runs a colour conversion to triple the byte count,
    pushes 3x the data through the pipe, and Python reads 3x the bytes, to
    deliver luma replicated across three identical channels.
    Measures: wall time and bytes for bgr24 vs gray+numpy-expand, same frames.
    Expect: gray wins on pipe bytes by 3x; the open question is whether the
    numpy expand gives it back. That is what this settles.

TEST 2 -- inference size
    SHOT_DET_IMGSZ has TWO different defaults: detect.py:30 says 1280, and
    live.py:200 says 640. Same variable, two code paths -- the live loop runs
    640 while ShotDetector._ball_track runs 1280. Inference cost scales with
    pixels, so if the two paths disagree, one of them is doing 4x the work.
    Measures: inference fps at several imgsz over identical frames.
    Expect: a clear cost curve. Whether the smaller size still DETECTS is a
    backtest question, not a throughput one -- this only prices the options.

Neither test touches production. Both need one SL/SR clip.

    python3 scripts/perf/detector_decode_bench.py --video ~/backtest/SL.mp4
    python3 scripts/perf/detector_decode_bench.py --video ... --weights .../v3.pt
    python3 scripts/perf/detector_decode_bench.py --video ... --imgsz 640 960 1280
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def decode(video: str, pix_fmt: str, size, max_frames: int, expand: bool):
    """Decode max_frames at pix_fmt; return (frames, seconds, bytes_off_pipe).

    Mirrors detect.py's reader: NVDEC where available, -vsync 0 passthrough
    (the FLIR files' timebase is bogus and ffmpeg's default sync silently
    corrupts frames), raw video straight off a pipe.
    """
    W, H = size
    depth = 3 if pix_fmt == "bgr24" else 1
    stride = W * H * depth
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error"]
    if os.environ.get("SHOT_DECODE", "").strip().lower() == "nv":
        cmd += ["-c:v", "h264_nvv4l2dec"]
    cmd += ["-i", video, "-vsync", "0", "-frames:v", str(max_frames),
            "-f", "rawvideo", "-pix_fmt", pix_fmt, "-"]

    frames, nbytes = [], 0
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, bufsize=stride)
    try:
        while len(frames) < max_frames:
            buf = p.stdout.read(stride)
            if not buf or len(buf) < stride:
                break
            nbytes += len(buf)
            fr = np.frombuffer(buf, np.uint8).reshape(H, W, depth)
            if expand and depth == 1:
                # gray -> 3 identical channels, in RAM. This is the cost that
                # might give back what the narrower pipe saved.
                fr = np.repeat(fr, 3, axis=2)
            frames.append(fr)
    finally:
        p.stdout.close()
        p.wait()
    return frames, time.perf_counter() - t0, nbytes


def main() -> int:
    ap = argparse.ArgumentParser(description="Decode-path and imgsz levers")
    ap.add_argument("--video", required=True, help="an SL/SR clip")
    ap.add_argument("--frames", type=int, default=480, help="4s at 120fps")
    ap.add_argument("--width", type=int, default=720)
    ap.add_argument("--height", type=int, default=540)
    ap.add_argument("--weights", help="run TEST 2 as well (a .pt or .engine)")
    ap.add_argument("--imgsz", type=int, nargs="+", default=[640, 960, 1280])
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()

    if not os.path.isfile(a.video):
        raise SystemExit(f"no such video: {a.video}")
    size = (a.width, a.height)

    # ---------------------------------------------------------------- TEST 1
    print(f"TEST 1 -- decode path, {a.frames} frames, best of {a.repeats}\n")
    results = {}
    for label, pix, expand in (("bgr24 (today)", "bgr24", False),
                               ("gray", "gray", False),
                               ("gray + expand to 3ch", "gray", True)):
        best, frames, nbytes = None, [], 0
        for _ in range(a.repeats):
            frames, secs, nbytes = decode(a.video, pix, size, a.frames, expand)
            best = secs if best is None else min(best, secs)
        if not frames:
            print(f"  {label:<24} decoded nothing — wrong size, or not h264?")
            continue
        results[label] = (best, nbytes, frames)
        print(f"  {label:<24} {best:6.2f}s   {nbytes/1e6:7.1f} MB off pipe   "
              f"{len(frames)/best:6.1f} fps   shape {frames[0].shape}")

    base = results.get("bgr24 (today)")
    if base:
        print()
        for label, (secs, nbytes, _) in results.items():
            if label.startswith("bgr24"):
                continue
            print(f"  {label:<24} {base[0]/secs:.2f}x faster, "
                  f"{base[1]/nbytes:.2f}x less pipe traffic")
        print("""
  The model wants luma; the camera produces luma. If 'gray + expand' still wins,
  bgr24 is pure waste and detect.py:195/258 should change. If the expand gives
  it all back, the real fix is a model that takes 1 channel -- or DeepStream,
  which never copies the frame to CPU at all.""")

    # ---------------------------------------------------------------- TEST 2
    if not a.weights:
        print("\n(TEST 2 skipped — pass --weights to price inference size)")
        return 0
    if not os.path.isfile(a.weights):
        raise SystemExit(f"no such weights: {a.weights}")

    frames = (results.get("gray + expand to 3ch") or base or (None, None, None))[2]
    if not frames:
        raise SystemExit("no decoded frames to infer over")

    from agx_pipeline.shot_detect import detect as D
    print(f"\nTEST 2 -- inference size, {len(frames)} frames, batch {D.DET_BATCH}")
    print(f"  live.py:200 default = 640   detect.py:30 default = {D.DET_IMGSZ}\n")
    det = D.ShotDetector(a.weights)
    prev = None
    for imgsz in a.imgsz:
        det.model.predict(frames[:D.DET_BATCH], imgsz=imgsz, conf=D.DET_CONF,
                          verbose=False, device=det.device)          # warm-up
        best = None
        for _ in range(a.repeats):
            t0 = time.perf_counter()
            for s in range(0, len(frames), D.DET_BATCH):
                det.model.predict(frames[s:s + D.DET_BATCH], imgsz=imgsz,
                                  conf=D.DET_CONF, verbose=False, device=det.device)
            secs = time.perf_counter() - t0
            best = secs if best is None else min(best, secs)
        fps = len(frames) / best
        rel = f"   {prev/fps:.2f}x slower than the previous size" if prev else ""
        print(f"  imgsz {imgsz:<6} {best:6.2f}s   {fps:6.1f} fps{rel}")
        prev = fps
    det.empty_cache()

    print("""
  This prices the options; it does not choose. Whether a smaller imgsz still
  finds the ball is a backtest question for the CV team. What it does settle is
  how much the two conflicting defaults cost -- and that they should agree.

  Both tests stack with TensorRT: they change how many pixels arrive and how
  many are inferred, not how fast the kernels run.""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
