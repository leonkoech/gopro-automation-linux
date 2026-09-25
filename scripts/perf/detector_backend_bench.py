#!/usr/bin/env python3
"""
PyTorch vs TensorRT for the shot detector: is the engine actually faster?

The full backtest (agx_pipeline/shot_detect/backtest/) already answers whether
a TensorRT engine DETECTS THE SAME THINGS -- it replays a game against frozen
human ground truth and scores precision/recall and make/miss. What it does not
measure is speed: there is no timing instrumentation in it, and a full run is
hours per model.

That is the wrong order to spend time in. This script answers the cheap
question first -- is the engine faster at all -- in minutes, on real footage,
using the same ShotDetector the live loop uses. Only if the answer is yes is
the multi-hour accuracy backtest worth running.

    python3 scripts/perf/detector_backend_bench.py \
        --video /home/dev/backtest/fdcd9bd4/SL.mp4 \
        --weights .../v3_best.pt .../v3_best.engine

Three things to get right, or the number is meaningless:

  DECODE MUST NOT BE THE BOTTLENECK. Frames are decoded once, up front, into
  RAM, and every backend then infers over the SAME list. Nothing is decoded
  inside the timed section. (detect.py's own reader takes SHOT_DECODE=nv to put
  decode on NVDEC for exactly this reason; here we sidestep it entirely.)

  WARM-UP IS NOT OPTIONAL. The first inference pays CUDA context creation,
  kernel autotune and, for TensorRT, engine deserialization -- seconds that
  have nothing to do with steady-state throughput. Warm-up batches are run and
  discarded before timing starts.

  BUILD THE ENGINE ON THIS BOX. TensorRT engines are specific to the GPU
  architecture and the TensorRT version; one built elsewhere will not load, and
  it needs rebuilding after a JetPack upgrade. Export with:
      yolo export model=v3_best.pt format=engine half=True imgsz=1280 device=0

Batch size, image size and confidence come from the same env vars the live
detector reads (SHOT_DET_BATCH / _IMGSZ / _CONF), so this measures the
configuration that actually runs, not a synthetic one.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_frames(video: str, n: int, size, stride: int):
    """Decode once, into RAM, before any timing. Uses the detector's own reader
    so the frames are exactly what it would see in production."""
    from agx_pipeline.shot_detect.detect import iter_frames
    frames = []
    for _idx, fr in iter_frames(video, size=size, select_stride=stride,
                                max_frames=n):
        frames.append(fr)
        if len(frames) >= n:
            break
    return frames


def bench(weight: str, frames, warmup_batches: int, repeats: int):
    """-> dict of timings, or {'error': ...}. Returns per-repeat wall times so
    the caller can see spread, not just a mean that hides a bad run."""
    from agx_pipeline.shot_detect import detect as D
    t_load = time.perf_counter()
    try:
        det = D.ShotDetector(weight)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}
    load_s = time.perf_counter() - t_load

    batch = D.DET_BATCH
    warm = frames[: batch * max(1, warmup_batches)]
    try:
        det._ball_track(warm)          # discarded: CUDA init, autotune, deserialize
    except Exception as e:  # noqa: BLE001
        return {"error": f"warm-up failed: {type(e).__name__}: {e}"}

    runs = []
    balls = 0
    for _ in range(repeats):
        t0 = time.perf_counter()
        track = det._ball_track(frames)
        runs.append(time.perf_counter() - t0)
        balls = len(track)
    det.empty_cache()
    n = len(frames)
    best = min(runs)
    return {
        "load_s": load_s,
        "runs": runs,
        "best_s": best,
        "fps": n / best,
        "ms_per_frame": 1000.0 * best / n,
        "detections": balls,
        "device": det.device,
        "batch": batch,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark shot-detector backends on identical frames")
    ap.add_argument("--video", required=True, help="an SL/SR clip (real footage)")
    ap.add_argument("--weights", nargs="+", required=True,
                    help="two or more .pt / .engine paths to compare")
    ap.add_argument("--frames", type=int, default=480,
                    help="frames to infer over (default 480 = 4s at 120fps)")
    ap.add_argument("--stride", type=int, default=1, help="decode every Nth frame")
    ap.add_argument("--repeats", type=int, default=3,
                    help="timed passes per backend; the best is reported")
    ap.add_argument("--warmup-batches", type=int, default=2)
    ap.add_argument("--width", type=int, default=720)
    ap.add_argument("--height", type=int, default=540)
    a = ap.parse_args()

    if not os.path.isfile(a.video):
        raise SystemExit(f"no such video: {a.video}")

    print(f"decoding {a.frames} frames from {os.path.basename(a.video)} "
          f"(once, outside the timed section)...")
    frames = load_frames(a.video, a.frames, (a.width, a.height), a.stride)
    if len(frames) < a.frames:
        print(f"note: got {len(frames)} frames, not {a.frames} — short clip?")
    if not frames:
        raise SystemExit("decoded no frames; is SHOT_DECODE/ffmpeg working?")

    from agx_pipeline.shot_detect import detect as D
    print(f"imgsz={D.DET_IMGSZ}  batch={D.DET_BATCH}  conf={D.DET_CONF}  "
          f"frames={len(frames)}  repeats={a.repeats}")
    print()

    results = {}
    for w in a.weights:
        name = os.path.basename(w)
        if not os.path.isfile(w):
            print(f"{name:<34} SKIP — no such file")
            continue
        print(f"benchmarking {name} ...", flush=True)
        results[w] = bench(w, frames, a.warmup_batches, a.repeats)

    print()
    print(f"{'backend':<34}{'fps':>9}{'ms/frame':>11}{'load':>8}{'dets':>7}  spread")
    print("-" * 84)
    for w, r in results.items():
        name = os.path.basename(w)
        if "error" in r:
            print(f"{name:<34} FAILED — {r['error'][:40]}")
            continue
        spread = f"{min(r['runs']):.2f}-{max(r['runs']):.2f}s"
        print(f"{name:<34}{r['fps']:>9.1f}{r['ms_per_frame']:>11.2f}"
              f"{r['load_s']:>7.1f}s{r['detections']:>7}  {spread}")

    ok = [(w, r) for w, r in results.items() if "error" not in r]
    if len(ok) >= 2:
        base_w, base = ok[0]
        print()
        for w, r in ok[1:]:
            sp = r["fps"] / base["fps"]
            print(f"{os.path.basename(w)} is {sp:.2f}x "
                  f"{'faster' if sp >= 1 else 'SLOWER'} than "
                  f"{os.path.basename(base_w)}")
            if r["detections"] != base["detections"]:
                print(f"  !! detection COUNT differs "
                      f"({base['detections']} vs {r['detections']}) — expected "
                      f"with FP16, and exactly why the accuracy backtest is "
                      f"still required. This is not a verdict, only a flag.")
            else:
                print("  detection counts match on this clip (necessary, not "
                      "sufficient — run the backtest for the real answer)")

    print("""
This measures INFERENCE THROUGHPUT only, on pre-decoded frames.

  Faster here  -> run agx_pipeline.shot_detect.backtest.run with each weight
                  and compare setup2_automated. Speed means nothing if the
                  verdicts move.
  Not faster   -> stop. The engine is not worth the accuracy risk, and no
                  multi-hour backtest is needed to know that.

The live detector's real-world shortfall also includes decode and the Python
loop around inference; a 2x model speedup is not a 2x end-to-end speedup.""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
