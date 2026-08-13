"""Offline validation of live.py (green-button latency fix) on REAL carved
segments — no service, no cameras, no Firestore writes.

Stubs cfg + fb, pre-places seg_%05d_SL.mp4 files carved (-c copy, same codec as
the live splitmuxsink output) from a real SL master, then runs LiveShotScorer's
worker loop until the backlog drains. Verifies: shots found with sane verdicts,
the new latency_s/scan_s/backlog fields present, and reports drain throughput
(processed-seconds per wall-second — must be >= ~1.0x per angle to keep up live).

Run on the AGX from the repo (imports the repo's live.py — deploy the candidate
files first, validate, THEN restart the service).

Usage:  python3 fastpath_harness.py <SL_master> <start_s> <seg_sec> <window 0|1> [nv 0|1]
"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

SRC = sys.argv[1]
START = float(sys.argv[2])
SEG = int(sys.argv[3])
WIN = sys.argv[4] == "1"
NV = len(sys.argv) > 5 and sys.argv[5] == "1"
N_SEG = max(2, int(24 / SEG) + 1)         # ~24s of footage + 1 open segment
STAGE = "/tmp/fastpath_stage"
LABEL = "fp_test"

os.environ["SHOT_LIVE_ENABLED"] = "true"
os.environ["SHOT_LIVE_POLL_SEC"] = "1"
os.environ["SHOT_SEGMENT_SEC"] = str(SEG)
os.environ["SHOT_LIVE_WINDOW"] = "true" if WIN else "false"
os.environ["SHOT_SEGMENT_ENABLED"] = "true"
os.environ["SHOT_DECODE"] = "nv" if NV else ""

sys.path.insert(0, "/home/dev/gopro-automation-linux")
from agx_pipeline.shot_detect.live import LiveShotScorer  # noqa: E402
from agx_pipeline.shot_recording import shot_seg_dir  # noqa: E402


class Cfg:
    output_dir = f"{STAGE}/out"


class _Doc:
    def __init__(self, sink):
        self._sink = sink

    def set(self, data, merge=False):
        self._sink.append(data)


class _Coll:
    def __init__(self, sink):
        self._sink = sink

    def document(self, _id):
        return _Doc(self._sink)


class _DB:
    def __init__(self, sink):
        self._sink = sink

    def collection(self, _name):
        return _Coll(self._sink)


class FB:
    def __init__(self):
        self.writes = []
        self.db = _DB(self.writes)


shutil.rmtree(Cfg.output_dir, ignore_errors=True)
label_dir = os.path.join(Cfg.output_dir, LABEL)
seg_dir = shot_seg_dir(Cfg.output_dir, LABEL)
os.makedirs(label_dir, exist_ok=True)
os.makedirs(seg_dir, exist_ok=True)

spawned = (datetime.now(timezone.utc) - timedelta(seconds=N_SEG * SEG)).isoformat()
with open(os.path.join(label_dir, f"{LABEL}_shot_timing.json"), "w") as f:
    json.dump({"fps_lock": 119.9,
               "cameras": [{"angle": "SL", "spawned_at": spawned}]}, f)

for i in range(N_SEG):
    dst = os.path.join(seg_dir, f"seg_{i:05d}_SL.mp4")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                    "-ss", f"{START + i * SEG:.1f}", "-i", SRC, "-t", str(SEG),
                    "-c", "copy", "-map", "0:v", dst], check=True)
n_closed = N_SEG - 1                      # highest index stays "open"

fb = FB()
scorer = LiveShotScorer(Cfg(), fb)
t0 = time.time()
scorer.start(LABEL, "fp-test-game")
while time.time() - t0 < 600:
    time.sleep(2)
    done = [w for w in fb.writes
            if w.get("shot_live", {}).get("n_segments") == n_closed]
    if done:
        break
scorer.stop()
wall = time.time() - t0

last = fb.writes[-1]["shot_live"] if fb.writes else {}
shots = last.get("shots", [])
print(f"\n===== HARNESS seg={SEG}s window={WIN} nv={NV} =====")
print(f"segments processed: {last.get('n_segments')}/{n_closed} "
      f"in {wall:.1f}s wall (includes one-time detector load)")
print(f"throughput: {n_closed * SEG / wall:.2f}x real time (1 angle; need >~1.0)")
print(f"shots: {last.get('n_shots')} (make={last.get('n_make')} "
      f"miss={last.get('n_miss')}) max_backlog={last.get('max_backlog')}")
for s in shots:
    print(f"  {s['cam']} seg={s['seg']} t={s['t_shot']} {s['verdict']} "
          f"scan_s={s.get('scan_s')} latency_s={s.get('latency_s')}")
missing = [k for k in ("latency_s", "scan_s") if shots and k not in shots[0]]
print(f"instrumentation fields: {'MISSING ' + str(missing) if missing else 'OK'}")
print("HARNESS_DONE")
