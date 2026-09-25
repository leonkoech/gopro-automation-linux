#!/usr/bin/env python3
"""
TensorRT for the ball detector: environment check, export, parity, benchmark.

TRT has been "in the plan" for weeks without an engine existing. This is the
one command that produces one and says whether it is worth adopting. It does
nothing to production -- it writes an .engine beside the weights and reports.

Why no code change is needed afterwards: node.py reads SHOT_DET_WEIGHT and
hands it to ShotDetector -> YOLO(path), and ultralytics loads .engine through
the same constructor as .pt. The backtest's --weight does no extension check
either. So adopting TRT is one environment variable, for the live loop, the
confirm pass and the backtest at once.

Four steps, each gating the next:

  1 ENVIRONMENT  Is tensorrt importable *on the host*? The detector runs on the
    host; DeepStream and its TensorRT live in the container. If the host has no
    tensorrt module this is blocked on environment, not effort, and that is
    worth knowing in 5 seconds rather than after an hour of export.

  2 EXPORT       yolo export format=engine. Slow on Jetson (10-40 min) and
    hardware-specific: an engine is valid only for this GPU and this TensorRT
    version, and must be rebuilt after a JetPack upgrade.
    imgsz and batch are BAKED IN. live.py runs imgsz=640 (live.py:200) while
    detect.py's DET_IMGSZ default is 1280 -- an engine built at the wrong one
    is useless for the path you care about, so this defaults to the live value
    and says so.

  3 PARITY       FP16 changes numerics, so detections can move. Runs both
    backends over identical frames and compares detection counts and box
    centres. This is a smoke test, NOT the accuracy answer -- that is
    agx_pipeline.shot_detect.backtest against frozen ground truth.

  4 BENCHMARK    Inference fps for each, on pre-decoded frames, warm.

    python3 scripts/perf/trt_kickstart.py --check          # step 1 only, instant
    python3 scripts/perf/trt_kickstart.py --video SL.mp4   # all four
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_WEIGHT = ROOT / "agx_pipeline/shot_detect/weights/ball_yolo26s_gray_hifps_v3_best.pt"


def step1_environment() -> bool:
    """-> True if an export can even be attempted here."""
    print("STEP 1 — environment\n")
    ok = True

    try:
        import torch
        cuda = torch.cuda.is_available()
        print(f"  torch          {torch.__version__}   cuda={cuda}")
        if not cuda:
            print("                 !! no CUDA — export and inference both need it")
            ok = False
    except Exception as e:  # noqa: BLE001
        print(f"  torch          MISSING — {e}")
        return False

    try:
        import ultralytics
        print(f"  ultralytics    {ultralytics.__version__}")
    except Exception as e:  # noqa: BLE001
        print(f"  ultralytics    MISSING — {e}")
        ok = False

    try:
        import tensorrt
        print(f"  tensorrt       {tensorrt.__version__}  <- importable on the HOST")
    except Exception as e:  # noqa: BLE001
        print(f"  tensorrt       NOT IMPORTABLE — {e}")
        print("""
  This is the blocker, and it is an environment problem rather than a
  modelling one. The detector runs on the host; TensorRT ships with JetPack,
  and this box reports "Jetpack NOT DETECTED". Options, cheapest first:

    - python3 -c "import tensorrt" inside the DeepStream container. If it
      works there, TRT exists on the box but not on the host's interpreter.
    - Check for the system package: dpkg -l | grep -i tensorrt
    - If it is present but not importable, it is usually a PYTHONPATH or venv
      issue: /usr/lib/python3/dist-packages holds the JetPack bindings and a
      venv created without --system-site-packages cannot see them.

  Exporting on another machine will NOT work: engines are specific to the GPU
  architecture and TensorRT version.""")
        ok = False

    print(f"\n  {'ready to export' if ok else 'cannot export here — fix the above first'}\n")
    return ok


def step2_export(weight: Path, imgsz: int, batch: int, half: bool) -> Path | None:
    """Export .engine beside the .pt. Returns its path, or None."""
    out = weight.with_suffix(".engine")
    print(f"STEP 2 — export  imgsz={imgsz} batch={batch} half={half}")
    if out.exists():
        print(f"  {out.name} already exists — reusing it")
        print("  (delete it to force a rebuild; engines do not update themselves)\n")
        return out
    print("  this takes 10-40 minutes on Jetson and pins one CPU core.\n")
    t0 = time.perf_counter()
    try:
        from ultralytics import YOLO
        # dynamic=True is REQUIRED, not a tuning choice. A fixed-shape engine
        # accepts exactly `batch` images and nothing else, and this codebase
        # infers on varying batch sizes: detect.py's _infer_batch ends a window
        # on a short chunk whenever the frame count is not a multiple of
        # DET_BATCH, and backtest/scan.py's estimate_rim predicts on ONE frame.
        # A static engine fails both with
        #   "input size (1,3,640,640) not equal to max model size (16,3,640,640)".
        # `batch` then sets the MAXIMUM of the optimisation profile.
        YOLO(str(weight)).export(format="engine", imgsz=imgsz, batch=batch,
                                 dynamic=True, half=half, device=0)
    except Exception as e:  # noqa: BLE001
        print(f"  export FAILED — {type(e).__name__}: {e}\n")
        return None
    print(f"  done in {(time.perf_counter()-t0)/60:.1f} min -> {out.name}\n")
    return out if out.exists() else None


def _frames(video: str, n: int, size):
    from agx_pipeline.shot_detect.detect import iter_frames
    got = []
    for _i, fr in iter_frames(video, size=size, select_stride=1, max_frames=n):
        got.append(fr)
        if len(got) >= n:
            break
    return got


def _detections(weight: str, frames, imgsz: int):
    """-> (list of per-frame best-ball centres or None, seconds for a warm pass)."""
    from agx_pipeline.shot_detect import detect as D
    det = D.ShotDetector(weight)
    det.model.predict(frames[:D.DET_BATCH], imgsz=imgsz, conf=D.DET_CONF,
                      verbose=False, device=det.device)              # warm
    centres, t0 = [], time.perf_counter()
    for s in range(0, len(frames), D.DET_BATCH):
        for r in det.model.predict(frames[s:s + D.DET_BATCH], imgsz=imgsz,
                                   conf=D.DET_CONF, verbose=False,
                                   device=det.device):
            b = D._best_ball(r.boxes)
            centres.append(None if b is None else (round(b[0], 1), round(b[1], 1)))
    secs = time.perf_counter() - t0
    det.empty_cache()
    return centres, secs


def main() -> int:
    ap = argparse.ArgumentParser(description="Kickstart the TensorRT evaluation")
    ap.add_argument("--check", action="store_true", help="step 1 only")
    ap.add_argument("--video", help="an SL/SR clip, for steps 3 and 4")
    ap.add_argument("--weight", default=str(DEFAULT_WEIGHT))
    ap.add_argument("--imgsz", type=int, default=640,
                    help="BAKED INTO the engine. 640 = what live.py runs")
    ap.add_argument("--batch", type=int, default=16, help="= SHOT_DET_BATCH")
    ap.add_argument("--frames", type=int, default=480)
    ap.add_argument("--width", type=int, default=720)
    ap.add_argument("--height", type=int, default=540)
    ap.add_argument("--fp32", action="store_true", help="export without FP16")
    a = ap.parse_args()

    if not step1_environment() or a.check:
        return 0 if a.check else 1

    weight = Path(a.weight)
    if not weight.is_file():
        raise SystemExit(f"no such weights: {weight}")

    engine = step2_export(weight, a.imgsz, a.batch, not a.fp32)
    if engine is None:
        return 1
    if not a.video:
        print("(steps 3 and 4 skipped — pass --video to compare and benchmark)")
        return 0
    if not os.path.isfile(a.video):
        raise SystemExit(f"no such video: {a.video}")

    print(f"STEP 3/4 — parity and speed over {a.frames} identical frames\n")
    frames = _frames(a.video, a.frames, (a.width, a.height))
    if not frames:
        raise SystemExit("decoded no frames — check --width/--height")

    pt_c, pt_s = _detections(str(weight), frames, a.imgsz)
    tr_c, tr_s = _detections(str(engine), frames, a.imgsz)

    both = sum(1 for p, t in zip(pt_c, tr_c) if p and t)
    neither = sum(1 for p, t in zip(pt_c, tr_c) if not p and not t)
    only_pt = sum(1 for p, t in zip(pt_c, tr_c) if p and not t)
    only_tr = sum(1 for p, t in zip(pt_c, tr_c) if t and not p)
    drift = [max(abs(p[0]-t[0]), abs(p[1]-t[1]))
             for p, t in zip(pt_c, tr_c) if p and t]

    print(f"  {'.pt  (torch)':<16}{len(frames)/pt_s:7.1f} fps   "
          f"{sum(1 for c in pt_c if c):4d} frames with a ball")
    print(f"  {'.engine (trt)':<16}{len(frames)/tr_s:7.1f} fps   "
          f"{sum(1 for c in tr_c if c):4d} frames with a ball")
    print(f"\n  speedup        {pt_s/tr_s:.2f}x")
    print(f"  agree          {both} both found, {neither} both empty")
    print(f"  disagree       {only_pt} only .pt, {only_tr} only .engine")
    if drift:
        drift.sort()
        print(f"  box drift      median {drift[len(drift)//2]:.1f}px, "
              f"max {drift[-1]:.1f}px  (where both found a ball)")

    sp = pt_s / tr_s
    print(f"""
  Read it as a go/no-go on SPEED only:
    >1.5x and few disagreements -> worth the accuracy backtest. Run
       python3 -m agx_pipeline.shot_detect.backtest.run --weight {engine.name} ...
       and compare setup2_automated against the same run with the .pt.
    ~1.0x -> TRT is not buying anything here; stop, and spend the time on the
       decode path instead (the live loop's 240 fps frame-supply requirement).
    Many disagreements at any speed -> FP16 moved the detections. Re-export
       with --fp32 and compare again before blaming TensorRT.

  Whatever this says, it is NOT the accuracy answer. Only the backtest against
  frozen ground truth decides whether the verdicts still hold.

  Adoption, if it passes: SHOT_DET_WEIGHT={engine}
  (no code change — node.py already routes it through ShotDetector).
  Current: {'FASTER' if sp > 1.2 else 'NOT MEANINGFULLY FASTER'} at imgsz={a.imgsz}.""")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
