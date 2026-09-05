#!/usr/bin/env python3
"""
Per-transcode wall time from a `docker events` capture.

Every _transcode_hw call is a `docker run --rm ... gst-launch-1.0`, so one
container's start->die span IS the whole Stage-4 cost: container creation,
CUDA init, and the encode. That is the number the warm transcode worker
shrinks and the sidecar proxy removes, available without deploying the
Grafana annotations.

Capture with:
    docker events --filter type=container --filter event=start \\
        --filter event=die --format '{{json .}}' > events.jsonl

Then:
    python3 transcode_container_times.py events.jsonl
    python3 transcode_container_times.py events.jsonl --image latest --each
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone


def load(path):
    """-> {container_id: {"start": ns, "die": ns, "image": str, "name": str}}"""
    by_id = {}
    bad = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue                      # nohup banner, stray output
            try:
                e = json.loads(line)
            except ValueError:
                bad += 1
                continue
            action = e.get("Action") or e.get("status")
            if action not in ("start", "die"):
                continue
            actor = e.get("Actor") or {}
            cid = actor.get("ID") or e.get("id")
            attrs = actor.get("Attributes") or {}
            ns = e.get("timeNano") or (e.get("time", 0) * 1_000_000_000)
            if not cid or not ns:
                continue
            rec = by_id.setdefault(cid, {"image": attrs.get("image") or e.get("from"),
                                         "name": attrs.get("name")})
            rec[action] = ns
    if bad:
        print(f"note: skipped {bad} unparseable lines", file=sys.stderr)
    return by_id


def pct(v, p):
    if not v:
        return None
    s = sorted(v)
    return s[min(len(s) - 1, int(round((p / 100) * (len(s) - 1))))]


def fmt(s):
    if s is None:
        return "-"
    return f"{s:.1f}s" if s < 120 else f"{s / 60:.1f}m"


def main() -> int:
    ap = argparse.ArgumentParser(description="Stage-4 wall time from docker events")
    ap.add_argument("events", help="the .jsonl capture")
    ap.add_argument("--image", help="only containers whose image contains this "
                                    "(e.g. 'latest' for transcodes)")
    ap.add_argument("--split", type=float, default=60.0, metavar="SEC",
                    help="runs longer than this are treated as ingest, shorter "
                         "as clips (default 60)")
    ap.add_argument("--each", action="store_true", help="list every container")
    args = ap.parse_args()

    runs = []
    unpaired = 0
    for cid, r in load(args.events).items():
        if "start" not in r or "die" not in r:
            unpaired += 1               # still running, or started before capture
            continue
        if args.image and args.image not in (r.get("image") or ""):
            continue
        runs.append({
            "id": cid[:12],
            "image": r.get("image"),
            "at": datetime.fromtimestamp(r["start"] / 1e9, tz=timezone.utc),
            "secs": (r["die"] - r["start"]) / 1e9,
        })
    if not runs:
        print("no completed container runs matched. Was anything transcoded, and "
              "does --image match? (transcodes use the ':latest' tag; camrec-v3 "
              "is ':webapp')")
        return 0
    runs.sort(key=lambda r: r["at"])

    clips = [r for r in runs if r["secs"] <= args.split]
    ingest = [r for r in runs if r["secs"] > args.split]
    if unpaired:
        print(f"note: {unpaired} container(s) had only one event (still running, "
              f"or started before the capture) - excluded\n")

    for label, group in (("clip transcodes", clips), ("ingest transcodes", ingest)):
        if not group:
            continue
        v = [r["secs"] for r in group]
        span = (group[-1]["at"] - group[0]["at"]).total_seconds() / 3600 or None
        print(f"{label}: n={len(v)}  "
              f"p50 {fmt(pct(v,50))}  p90 {fmt(pct(v,90))}  max {fmt(max(v))}  "
              f"mean {fmt(statistics.mean(v))}")
        print(f"  {group[0]['at']:%H:%M} -> {group[-1]['at']:%H:%M} UTC"
              + (f", one every {span*3600/len(v):.0f}s" if span and len(v) > 1 else "")
              + f", {fmt(sum(v))} of engine+startup time total")
        print()

    if clips:
        v = sorted(r["secs"] for r in clips)
        print("This span INCLUDES the per-clip docker+CUDA cold start, because the")
        print("container is created and torn down per transcode. The warm worker")
        print("removes that portion; the sidecar proxy removes the whole span.")
        print(f"Fastest clip transcode seen: {fmt(v[0])} - a floor for what the")
        print("encode alone costs, so roughly (p50 - floor) is the startup tax.")

    if args.each:
        print()
        print(f"{'start (UTC)':<21}{'secs':>9}  id            image")
        for r in runs:
            print(f"{r['at']:%Y-%m-%d %H:%M:%S}  {r['secs']:>8.1f}  {r['id']}  "
                  f"{r['image']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
