#!/usr/bin/env python3
"""Show the logs of recent ingestion-runs -- what 'completed_with_errors' means.

  python3 runlog.py                 # last 10 runs, warn+error lines only
  python3 runlog.py --all           # every log line, not just problems
  python3 runlog.py --run 2f1afb04  # one run, full detail incl. per-angle status
"""
import argparse, os, sys
os.environ.setdefault("FIREBASE_CREDENTIALS_PATH",
    "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json")
import firebase_admin
from firebase_admin import credentials, firestore

ap = argparse.ArgumentParser()
ap.add_argument("--runs", type=int, default=10)
ap.add_argument("--run", help="one run id (prefix ok)")
ap.add_argument("--all", action="store_true", help="info lines too")
a = ap.parse_args()

p = os.environ["FIREBASE_CREDENTIALS_PATH"]
if not os.path.exists(p):
    raise SystemExit(f"credentials not found: {p}")
try: firebase_admin.get_app()
except ValueError: firebase_admin.initialize_app(credentials.Certificate(p))
db = firestore.client()

q = db.collection("ingestion-runs").order_by(
        "started_at", direction=firestore.Query.DESCENDING).limit(200)
docs = [(d.id, d.to_dict() or {}) for d in q.stream()]
if a.run:
    docs = [(i, r) for i, r in docs if i.startswith(a.run)]
else:
    docs = docs[:a.runs]

for rid, r in docs:
    st = r.get("status")
    if not a.all and not a.run and st == "completed":
        continue                      # only show runs that had trouble
    print(f"\n{'='*78}\n{rid[:8]}  {r.get('date')}  {st}  "
          f"angles={','.join(r.get('angles') or [])}")
    if r.get("error"):
        print(f"  run error: {r['error']}")
    for stage, s in (r.get("stages") or {}).items():
        if s.get("status") == "failed" or s.get("error"):
            print(f"  stage {stage}: {s.get('status')}  {s.get('error') or ''}")
    for ang, per in (r.get("angle_status") or {}).items():
        bad = {k: v for k, v in per.items()
               if isinstance(v, dict) and (v.get("status") == "failed" or v.get("error"))}
        if bad:
            print(f"  angle {ang}: {bad}")
    for L in r.get("logs") or []:
        lvl = (L.get("level") or "").lower()
        if a.all or lvl in ("warn", "warning", "error"):
            print(f"  [{lvl:<5}] {str(L.get('ts'))[:19]}  {L.get('msg')}")
