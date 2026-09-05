#!/usr/bin/env python3
"""
Per-stage timings for AGX ingestion runs, from the `ingestion-runs` docs.

The run doc records stage *status* but not stage *duration* -- `stages.transcode`
is only {status, done, total, error}. The timing is in the `logs` array, which
IngestionRun timestamps at 1s resolution:

    04:39:46  transcode: started
    05:06:40  transcode: done (3/3)          -> transcode took 26m54s
    05:06:40  upload: started
    05:08:39  NL uploaded -> s3://...        -> per-angle upload waterfall
    05:10:33  FL uploaded -> s3://...
    05:12:34  FR uploaded -> s3://...
    05:12:34  upload: done (3/3)
    05:12:39  both angles registered - game ready; notifying annotators

This parses those back into numbers, so a change like a wider transcode pool or
overlapping upload with transcode can be measured against previous nights
instead of estimated. `annotator_ready_s` (run start -> "game ready") is the
one that matters to the people waiting on the footage; the SL/SR shot-footage
upload happens after that line and is deliberately reported separately.

**Compare throughput, not duration.** Stage durations move with things that
have nothing to do with a code change: game length (observed 21.7m vs 49.6m
transcodes that ran at an identical 0.38 GB/min) and angle count (a camera
offline, or angles added later). Both spreads are larger than the effect an
A/B is usually looking for. Every run's proxy output size is on the doc, so
the GB/min and MB/s columns divide those confounders out - use them to compare
across a change, and read the durations for what a night actually costs.

Run it where Firebase creds live -- normally the AGX itself:

    python3 scripts/ingestion_run_timings.py --limit 20
    python3 scripts/ingestion_run_timings.py --since 2026-08-25 --angles
    python3 scripts/ingestion_run_timings.py --limit 50 --csv timings.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import firebase_admin
from firebase_admin import credentials, firestore

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # dotenv is optional on the box
    pass

COLLECTION = "ingestion-runs"
STAGES = ("transcode", "upload", "register")

FIREBASE_CREDENTIALS_PATH = os.getenv(
    "FIREBASE_CREDENTIALS_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)),
                 "uball-gopro-fleet-firebase-adminsdk.json"),
)

# "<ANGLE> uploaded -> s3://bucket/key (1234 MB)"  -- the 1080p proxy.
# Anchored so the 4K-mode line ("FL 4K master uploaded -> ...") and the FLIR
# line ("shot SL (...): uploaded as-is -> ...") don't match it. The trailing
# size is optional: ingest omits it when the file size was unreadable.
RE_ANGLE_UPLOAD = re.compile(r"^([A-Z]{2}) uploaded -> s3://\S+(?: \((\d+) MB\))?")
RE_4K_UPLOAD = re.compile(r"^([A-Z]{2}) 4K master uploaded -> s3://")
RE_SHOT_UPLOAD = re.compile(r"^shot ([A-Z]{2}) .*uploaded as-is -> s3://")


def init_firebase() -> firestore.Client:
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise SystemExit(
            f"Firebase credentials not found: {FIREBASE_CREDENTIALS_PATH}\n"
            "Set FIREBASE_CREDENTIALS_PATH, or run this on the AGX where the "
            "service's credentials already live.")
    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(
            credentials.Certificate(FIREBASE_CREDENTIALS_PATH))
    return firestore.client()


def parse_ts(value) -> Optional[datetime]:
    """IngestionRun writes '%Y-%m-%dT%H:%M:%SZ'; ingest.py's own _now() adds
    microseconds. Accept either, and a real Firestore timestamp too."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def secs(a: Optional[datetime], b: Optional[datetime]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round((b - a).total_seconds(), 1)


def summarize(doc: dict) -> dict:
    """One run -> a flat row of durations."""
    logs = doc.get("logs") or []
    events: List[tuple] = []
    for entry in logs:
        ts = parse_ts(entry.get("ts"))
        msg = (entry.get("msg") or "").strip()
        if ts and msg:
            events.append((ts, msg))

    started = parse_ts(doc.get("started_at"))
    completed = parse_ts(doc.get("completed_at"))
    row: Dict[str, object] = {
        "pipeline_id": doc.get("pipeline_id"),
        "date": doc.get("date"),
        "jetson_id": doc.get("jetson_id"),
        "video_name": doc.get("video_name"),
        "status": doc.get("status"),
        "n_angles": len(doc.get("angles") or []),
        "angles": ",".join(doc.get("angles") or []),
        "started_at": doc.get("started_at"),
    }

    # Stage durations: "<stage>: started" .. "<stage>: done|failed (n/m)"
    for stage in STAGES:
        t0 = t1 = None
        for ts, msg in events:
            if t0 is None and msg == f"{stage}: started":
                t0 = ts
            elif t0 is not None and msg.startswith(f"{stage}: ") \
                    and not msg.endswith("started"):
                t1 = ts
                break
        row[f"{stage}_s"] = secs(t0, t1)
    row["transcode_start"] = next(
        (ts.strftime("%H:%M:%S") for ts, m in events if m == "transcode: started"), None)

    # Per-angle 1080p upload completions, as offsets from the upload stage
    # start. Serial uploads show up here as an evenly-spaced staircase --
    # which is the whole argument for overlapping them with the transcode.
    up_start = next((ts for ts, m in events if m == "upload: started"), None)
    angle_uploads: Dict[str, float] = {}
    log_bytes: Dict[str, int] = {}
    for ts, msg in events:
        m = RE_ANGLE_UPLOAD.match(msg)
        if not m:
            continue
        if up_start:
            angle_uploads[m.group(1)] = secs(up_start, ts)
        if m.group(2):
            log_bytes[m.group(1)] = int(m.group(2)) * 1_000_000
    row["angle_upload_offsets_s"] = angle_uploads
    gaps = sorted(angle_uploads.values())
    row["upload_gap_max_s"] = (
        round(max(b - a for a, b in zip([0.0] + gaps, gaps)), 1) if gaps else None)

    # Bytes of 1080p proxy produced. Duration alone cannot be compared across
    # runs -- a 50-minute transcode of a long game and a 25-minute transcode of
    # a short one may be the same speed -- and the angle count changes too (a
    # camera offline, or angles added later). Normalising by bytes removes both
    # confounders, so an A/B needs a handful of runs instead of many.
    #
    # Prefer doc["uploads"], which carries real byte counts, over re-parsing the
    # log's rounded MB. Skip "<ANGLE>_4K" keys: those are the raw masters copied
    # up in 4K mode, not transcoder output.
    proxy_bytes = 0
    for angle, entry in (doc.get("uploads") or {}).items():
        if angle.endswith("_4K") or not isinstance(entry, dict):
            continue
        size = entry.get("size")
        if isinstance(size, (int, float)) and size > 0:
            proxy_bytes += int(size)
    if not proxy_bytes:
        proxy_bytes = sum(log_bytes.values())
    row["proxy_bytes"] = proxy_bytes or None
    gb = (proxy_bytes / 1e9) if proxy_bytes else None
    row["proxy_gb"] = round(gb, 2) if gb else None
    # Transcode throughput: output GB per wall minute of stage 1. Upload
    # throughput: MB/s over the upload stage -- how close to the uplink it runs,
    # and therefore how much of it overlapping can hide.
    row["transcode_gb_per_min"] = (
        round(gb / (row["transcode_s"] / 60), 3)
        if gb and row.get("transcode_s") else None)
    row["upload_mb_per_s"] = (
        round(proxy_bytes / 1e6 / row["upload_s"], 1)
        if proxy_bytes and row.get("upload_s") else None)

    # The moment the people waiting actually get the game.
    ready = next((ts for ts, m in events if "game ready" in m), None)
    row["annotator_ready_s"] = secs(started, ready)
    row["total_s"] = secs(started, completed)

    # Post-ready work, off the annotator critical path.
    shot_ts = [ts for ts, m in events if RE_SHOT_UPLOAD.match(m)]
    row["shot_upload_s"] = secs(ready, max(shot_ts)) if (ready and shot_ts) else None
    row["n_4k_uploads"] = sum(1 for _, m in events if RE_4K_UPLOAD.match(m))
    return row


def fetch(db, limit: int) -> List[dict]:
    # order_by on one field needs no composite index; every other filter is
    # applied client-side below for the same reason.
    q = (db.collection(COLLECTION)
           .order_by("started_at", direction=firestore.Query.DESCENDING)
           .limit(limit))
    return [d.to_dict() or {} for d in q.stream()]


def fmt(v, width: int = 8) -> str:
    """Duration -> '21.7m' / '54s' / '-'."""
    if v is None:
        return "-".rjust(width)
    if isinstance(v, float):
        return (f"{v / 60:.1f}m" if v >= 120 else f"{v:.0f}s").rjust(width)
    return str(v).rjust(width)


def num(v, width: int = 7, places: int = 1) -> str:
    """Plain number -> '8.2' / '-'."""
    return ("-" if v is None else f"{v:.{places}f}").rjust(width)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Per-stage timings for AGX ingestion runs")
    ap.add_argument("--limit", type=int, default=20,
                    help="most recent N runs to fetch (default 20)")
    ap.add_argument("--since", help="keep runs with date >= YYYY-MM-DD")
    ap.add_argument("--jetson", help="keep runs from this jetson_id")
    ap.add_argument("--status", help="keep runs with this status "
                                     "(completed, completed_with_errors, failed)")
    ap.add_argument("--angles", action="store_true",
                    help="also print the per-angle upload waterfall")
    ap.add_argument("--csv", help="write rows to this CSV path")
    ap.add_argument("--json", dest="as_json", action="store_true",
                    help="dump rows as JSON instead of a table")
    args = ap.parse_args()

    rows = [summarize(d) for d in fetch(init_firebase(), args.limit)]
    if args.since:
        rows = [r for r in rows if (r["date"] or "") >= args.since]
    if args.jetson:
        rows = [r for r in rows if r["jetson_id"] == args.jetson]
    if args.status:
        rows = [r for r in rows if r["status"] == args.status]

    if not rows:
        print("no matching ingestion runs")
        return 0

    if args.as_json:
        print(json.dumps(rows, indent=2, default=str))
    else:
        print(f"{'date':<11}{'run':<10}{'ang':>4}{'GB':>6}{'transcode':>10}"
              f"{'GB/min':>8}{'upload':>9}{'MB/s':>7}{'reg':>6}{'ready':>9}"
              f"{'total':>9}  status")
        print("-" * 110)
        for r in rows:
            print(f"{str(r['date'] or '-'):<11}{str(r['pipeline_id'] or '-'):<10}"
                  f"{r['n_angles']:>4}{num(r['proxy_gb'], 6)}"
                  f"{fmt(r['transcode_s'], 10)}{num(r['transcode_gb_per_min'], 8, 2)}"
                  f"{fmt(r['upload_s'], 9)}{num(r['upload_mb_per_s'], 7)}"
                  f"{fmt(r['register_s'], 6)}"
                  f"{fmt(r['annotator_ready_s'], 9)}{fmt(r['total_s'], 9)}"
                  f"  {r['status']}")
            if args.angles and r["angle_upload_offsets_s"]:
                waterfall = "  ".join(
                    f"{a}+{int(t)}s" for a, t in
                    sorted(r["angle_upload_offsets_s"].items(), key=lambda kv: kv[1]))
                print(f"{'':<11}  uploads from upload-start: {waterfall}"
                      f"   (largest gap {fmt(r['upload_gap_max_s'], 0).strip()})")

        done = [r for r in rows if r["transcode_s"] is not None]
        if done:
            def med(key):
                vals = sorted(r[key] for r in done if r[key] is not None)
                return vals[len(vals) // 2] if vals else None
            print("-" * 110)
            print(f"n={len(done)}  median transcode {fmt(med('transcode_s'), 0).strip()}"
                  f"  upload {fmt(med('upload_s'), 0).strip()}"
                  f"  annotator-ready {fmt(med('annotator_ready_s'), 0).strip()}")
            tp, up = med("transcode_gb_per_min"), med("upload_mb_per_s")
            print(f"{'':<6}median throughput "
                  f"transcode {num(tp, 0, 2).strip()} GB/min"
                  f"  upload {num(up, 0).strip()} MB/s"
                  f"   <- compare THESE across a change; duration also moves "
                  f"with game length and angle count")

    if args.csv:
        cols = [c for c in rows[0] if c != "angle_upload_offsets_s"]
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {len(rows)} rows -> {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
