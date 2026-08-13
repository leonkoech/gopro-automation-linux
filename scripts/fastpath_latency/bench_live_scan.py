"""Fast-path latency fix — idle-box benchmark (run on the AGX, NO other GPU jobs).

Measures the live-loop cost per segment with CPU ffmpeg decode vs the Orin's
NVDEC engine (SHOT_DECODE=nv, detect.iter_frames), and checks verdict PARITY on
a window that contains a real rim event. Carves segments (-c copy: same codec
as the live splitmuxsink output; keyframes every 0.25s so copy-cuts are safe)
from a real SL master.

Usage:  python3 bench_live_scan.py <SL_master> <event_start_s> [quiet_start_s]
        event_start_s: ~5s before a known rim event (parity window)
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, "/home/dev/gopro-automation-linux")
from agx_pipeline.shot_detect.backtest import scan  # noqa: E402
from agx_pipeline.shot_detect.node import _VALIDATOR  # noqa: E402

SRC = sys.argv[1]
EVENT = float(sys.argv[2])
QUIET = float(sys.argv[3]) if len(sys.argv) > 3 else 900.0
OUT = "/tmp/bench_segs"
os.makedirs(OUT, exist_ok=True)

CARVES = {"par8": (EVENT, 8), "ev4": (EVENT + 2, 4), "ev2": (EVENT + 3, 2),
          "q4": (QUIET, 4)}
for name, (ss, dur) in CARVES.items():
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{ss:.1f}",
                    "-i", SRC, "-t", str(dur), "-c", "copy", "-map", "0:v",
                    f"{OUT}/{name}.mp4"], check=True)


def decode_time(path, nv):
    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error"]
    if nv:
        cmd += ["-c:v", "h264_nvv4l2dec"]
    cmd += ["-i", path, "-vf", "select=not(mod(n\\,4))", "-vsync", "0",
            "-f", "rawvideo", "-pix_fmt", "bgr24", "-"]
    t0 = time.time()
    subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
    return time.time() - t0


for name in ("q4", "par8"):
    for nv in (False, True):
        try:
            dt = decode_time(f"{OUT}/{name}.mp4", nv)
            print(f"DECODE {name} {'nv ' if nv else 'cpu'}: {dt:.2f}s", flush=True)
        except subprocess.CalledProcessError as e:
            print(f"DECODE {name} {'nv ' if nv else 'cpu'}: FAILED ({e})", flush=True)

det, rims = _VALIDATOR.get()
rim = (scan.estimate_rim(det.model, f"{OUT}/par8.mp4", det.device, fps=119.9)
       or (rims or {}).get("SL"))
print(f"rim: {rim}", flush=True)


def run_scan(name, mode):
    os.environ["SHOT_DECODE"] = mode if mode == "nv" else ""
    t0 = time.time()
    shots = scan.scan_shots(det.model, f"{OUT}/{name}.mp4", det.device, 119.9,
                            rim, stride=4, imgsz=640, progress_every=0)
    dt = time.time() - t0
    sig = ";".join(f"{s['t_shot']:.2f}:{s['verdict']}" for s in shots)
    print(f"SCAN {name} {mode}: {dt:.2f}s shots=[{sig}]", flush=True)
    return sig


par = {}
for mode in ("cpu", "nv"):
    par[mode] = run_scan("par8", mode)
print(f"PARITY: {'OK' if par['cpu'] == par['nv'] else 'MISMATCH'}", flush=True)
for name in ("ev4", "ev2", "q4"):
    run_scan(name, "nv")
print("BENCH_DONE", flush=True)
