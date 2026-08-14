"""Batch driver: fetch each GT game's SL/SR/FL/FR from S3 and run gt_eval.

The role-aware ingest archives the native shot-cam recordings (720x540 ~120fps
h264) beside the registered FL/FR in court-a/<date>/<prefix>/ — so GT games
whose local footage was cleaned are still fully evaluable. Downloads are
deleted after each game (clips + report kept). Defers during the evening game
window and never runs while a recording is active.

Usage: python3 gt_eval_batch.py  (game list embedded)
"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime

import boto3
import requests

BUCKET = "uball-videos-production"
DL = "/home/dev/gt_eval_dl"
GAMES = [
    ("2e574fd2-ce0c-4873-bc57-bf66bd41c078", "2026-08-06", "Uptempo v Colada"),
    ("254631c9-8e12-436e-a283-fc878ec78f5f", "2026-08-05", "ViceCity v ET"),
    ("4e618940-85f3-4b94-8c38-2728ac42b560", "2026-08-04", "Akatsuki v ADIM"),
    ("166554d6-4477-4c6f-8f64-d4d527261b96", "2026-08-04", "Hustle v LosRonas"),
    ("736bd664-b0df-49fc-b8a6-77ad09507e95", "2026-08-05", "Iconic v KTB"),
]
s3 = boto3.client("s3")


def log(m):
    print(f"[batch {datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def wait_safe():
    """Defer during the evening game window; never start while recording."""
    while True:
        h = datetime.now().hour
        if 18 <= h or h < 3:
            log("game window — sleeping 10 min")
            time.sleep(600)
            continue
        try:
            r = requests.get("http://localhost:5000/health", timeout=5).json()
            if r.get("recording"):
                log("recording active — sleeping 5 min")
                time.sleep(300)
                continue
        except Exception:  # noqa: BLE001
            pass
        return


for gid, date, name in GAMES:
    label = f"s3_{gid.split('-')[0]}"
    rep = f"/home/dev/gt_eval/{label}_report.json"
    if os.path.exists(rep):
        log(f"{name}: report exists — skipping")
        continue
    wait_safe()
    p4 = "-".join(gid.split("-")[:4])
    d = f"{DL}/{label}"
    os.makedirs(d, exist_ok=True)
    ok = True
    for cam in ("SL", "SR", "FL", "FR"):
        key = f"court-a/{date}/{p4}/{date}_{p4}_{cam}.mp4"
        dst = f"{d}/{label}_{cam}.mp4"
        if os.path.exists(dst) and os.path.getsize(dst) > 1e9:
            continue
        log(f"{name}: downloading {cam}")
        try:
            s3.download_file(BUCKET, key, dst)
        except Exception as e:  # noqa: BLE001
            log(f"{name}: {cam} download FAILED ({e})")
            ok = False
            break
    if not ok:
        continue
    wait_safe()
    log(f"{name}: running gt_eval")
    cp = subprocess.run(
        [sys.executable, "/home/dev/gt_eval.py", "--label", label,
         "--uball-game-id", gid, "--rec-dir", DL],
        cwd="/home/dev")
    log(f"{name}: gt_eval rc={cp.returncode}")
    shutil.rmtree(d, ignore_errors=True)   # keep clips+report, drop footage

log("BATCH_DONE")
summary = {}
for gid, date, name in GAMES:
    rep = f"/home/dev/gt_eval/s3_{gid.split('-')[0]}_report.json"
    if os.path.exists(rep):
        r = json.load(open(rep))
        summary[name] = {"gt": r["gt_shots"], "cv": r["cv_shots"],
                         "matched": r["matched"],
                         "makemiss": r["makemiss_accuracy"],
                         "model_missed": len(r["model_missed"]),
                         "gt_missed": len(r["gt_missed_in_game"])}
print("BATCH_SUMMARY " + json.dumps(summary), flush=True)
