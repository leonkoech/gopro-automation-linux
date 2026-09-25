#!/usr/bin/env python3
"""
Measured time-to-clip, per stage, from the Grafana highlight annotations.

`agx_pipeline/grafana_annotate.py` posts a marker at every stage boundary of a
highlight clip. Paired up by log_id they give the real stage breakdown for
every clip of a night, replacing the estimates the latency analysis has been
carrying:

    detected -> queued -> cut start -> segments ready -> concat done
      -> transcode start -> transcode done -> trim start -> trim done
      -> upload start -> upload done -> clip ready

The two that matter most:

  segment_wait   cut start -> segments ready. Set by HIGHLIGHT_SEGMENT_SEC;
                 the thing HIGHLIGHT_SEGMENT_SEC=2 is meant to halve.
  transcode      transcode start -> transcode done. Includes the per-clip
                 `docker run` + CUDA cold start, because the annotation is
                 emitted before _transcode_1080p is called. This is the number
                 that decides whether the warm worker is enough on its own or
                 whether the transcode stage has to be removed entirely.

Annotations are cheap and plentiful (a busy night is ~180 clips x 13 markers),
so one night is a real distribution, not a sample of one.

Works against annotations from before or after the tag cleanup: the log_id is
read out of the annotation TEXT, which both versions carry, rather than out of
the tags, which only the older ones did.

Needs the same Grafana settings the AGX service posts with:

    export GRAFANA_URL=http://monitoring-box:3000
    export GRAFANA_TOKEN=<service-account token with Annotations:Read>

    python3 scripts/perf/clip_stage_timings.py --since 1
    python3 scripts/perf/clip_stage_timings.py --from '2026-09-03 19:00' --to '2026-09-04 00:00'
    python3 scripts/perf/clip_stage_timings.py --since 3 --csv clips.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

GRAFANA_URL = os.getenv("GRAFANA_URL", "").rstrip("/")
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN")

# Stage tag -> the ordered position it occupies in a clip's life.
STAGE_TAGS = ("detected", "queued", "cut_start", "segments_ready", "concat_done",
              "transcode_start", "transcode_done", "trim_start", "trim_done",
              "upload_start", "upload_done", "ready", "error")

# (label, from_tag, to_tag) - the intervals worth naming. Anything a clip is
# missing simply comes out None rather than dropping the whole clip: a run that
# failed mid-way still tells you how long it got through.
INTERVALS = [
    ("queue",        "detected",        "queued"),
    ("thread start", "queued",          "cut_start"),
    ("segment wait", "cut_start",       "segments_ready"),
    ("concat",       "segments_ready",  "concat_done"),
    ("transcode",    "transcode_start", "transcode_done"),
    ("trim",         "trim_start",      "trim_done"),
    ("upload",       "upload_start",    "upload_done"),
    ("mark ready",   "upload_done",     "ready"),
    ("TOTAL",        "detected",        "ready"),
]


def parse_when(s: str) -> datetime:
    """'2026-09-03 19:00' / '2026-09-03T19:00:00Z' / epoch seconds -> aware dt."""
    s = s.strip()
    if s.isdigit():
        return datetime.fromtimestamp(int(s), tz=timezone.utc)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise SystemExit(f"could not parse time: {s!r}")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fetch_annotations(t_from: datetime, t_to: datetime,
                      limit: int = 5000) -> List[dict]:
    if not (GRAFANA_URL and GRAFANA_TOKEN):
        raise SystemExit(
            "GRAFANA_URL and GRAFANA_TOKEN must be set (the same values the AGX "
            "service posts annotations with; the token needs Annotations:Read).")
    q = urllib.parse.urlencode({
        "tags": "highlight",          # every clip marker carries this
        "from": int(t_from.timestamp() * 1000),
        "to": int(t_to.timestamp() * 1000),
        "limit": limit,
    })
    req = urllib.request.Request(
        f"{GRAFANA_URL}/api/annotations?{q}",
        headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Grafana returned {e.code}: {e.read()[:300].decode(errors='replace')}")
    except urllib.error.URLError as e:
        raise SystemExit(f"could not reach {GRAFANA_URL}: {e.reason}")
    if len(data) >= limit:
        print(f"warning: hit the {limit}-annotation limit; narrow the window "
              f"or raise --limit, results are truncated", file=sys.stderr)
    return data


def log_id_of(text: str) -> Optional[str]:
    """Annotation text is '<what>: <log_id>', except the failure marker which is
    '<what>: <log_id> (<error>)'. The log_id is never in the tags on newer
    builds, so the text is the only reliable source."""
    if ": " not in text:
        return None
    tail = text.split(": ", 1)[1].strip()
    return tail.split(" (", 1)[0].strip() or None


def stage_of(tags: List[str]) -> Optional[str]:
    for t in tags or []:
        if t in STAGE_TAGS:
            return t
    return None


def group_clips(annotations: List[dict]) -> Dict[str, Dict[str, float]]:
    """-> {log_id: {stage: epoch_seconds}}. Keeps the EARLIEST timestamp per
    stage: a clip re-cut by _retry_failed_highlights reuses its log_id, and the
    first attempt is the one whose latency the viewer actually experienced."""
    clips: Dict[str, Dict[str, float]] = {}
    for a in annotations:
        stage = stage_of(a.get("tags"))
        lid = log_id_of(a.get("text") or "")
        ts = a.get("time")
        if not (stage and lid and isinstance(ts, (int, float))):
            continue
        c = clips.setdefault(lid, {})
        sec = ts / 1000.0
        if stage not in c or sec < c[stage]:
            c[stage] = sec
    return clips


def pct(vals: List[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    i = min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))
    return s[i]


def fmt(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"{v:.1f}s" if v < 120 else f"{v / 60:.1f}m"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Per-stage time-to-clip from Grafana highlight annotations")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--since", type=float, metavar="DAYS",
                   help="window = the last N days (default 1)")
    ap.add_argument("--from", dest="t_from", help="window start (local/ISO/epoch)")
    ap.add_argument("--to", dest="t_to", help="window end (default: now)")
    ap.add_argument("--limit", type=int, default=5000,
                    help="max annotations to fetch (default 5000)")
    ap.add_argument("--clips", action="store_true",
                    help="also print a line per clip, slowest first")
    ap.add_argument("--csv", help="write per-clip rows to this CSV path")
    args = ap.parse_args()

    t_to = parse_when(args.t_to) if args.t_to else datetime.now(timezone.utc)
    if args.t_from:
        t_from = parse_when(args.t_from)
    else:
        t_from = t_to - timedelta(days=args.since if args.since else 1)

    anns = fetch_annotations(t_from, t_to, args.limit)
    clips = group_clips(anns)
    if not clips:
        print(f"no highlight annotations between {t_from:%Y-%m-%d %H:%M} and "
              f"{t_to:%Y-%m-%d %H:%M} UTC.\n"
              f"(fetched {len(anns)} annotations tagged 'highlight'. If that is 0, "
              f"the instrumented build may not be deployed, or GRAFANA_URL/TOKEN "
              f"point at a different Grafana.)")
        return 0

    rows = []
    for lid, st in clips.items():
        row: Dict[str, object] = {"log_id": lid}
        first = min(st.values())
        row["at"] = datetime.fromtimestamp(first, tz=timezone.utc).strftime("%H:%M:%S")
        row["failed"] = "error" in st
        for label, a, b in INTERVALS:
            row[label] = round(st[b] - st[a], 2) if (a in st and b in st) else None
        rows.append(row)
    rows.sort(key=lambda r: r["at"])

    n_fail = sum(1 for r in rows if r["failed"])
    print(f"{len(rows)} clips between {t_from:%Y-%m-%d %H:%M} and "
          f"{t_to:%Y-%m-%d %H:%M} UTC"
          + (f"  ({n_fail} failed)" if n_fail else ""))
    print()
    print(f"{'stage':<15}{'n':>5}{'p50':>9}{'p90':>9}{'max':>9}{'share of p50 total':>21}")
    print("-" * 68)
    # Resolve the total first: it is the last row printed, so computing it
    # inside the loop would leave every share blank.
    total_p50 = pct([r["TOTAL"] for r in rows if r["TOTAL"] is not None], 50)
    for label, _a, _b in INTERVALS:
        vals = [r[label] for r in rows if r[label] is not None]
        p50 = pct(vals, 50)
        if label == "TOTAL":
            print("-" * 68)
        share = ""
        if p50 and total_p50 and label != "TOTAL":
            share = f"{100.0 * p50 / total_p50:.0f}%"
        print(f"{label:<15}{len(vals):>5}{fmt(p50):>9}{fmt(pct(vals, 90)):>9}"
              f"{fmt(pct(vals, 100)):>9}{share:>21}")

    tc = [r["transcode"] for r in rows if r["transcode"] is not None]
    if tc:
        print()
        print(f"transcode stage: p50 {fmt(pct(tc,50))}, p90 {fmt(pct(tc,90))}, "
              f"mean {fmt(statistics.mean(tc))}, n={len(tc)}")
        print(f"  cumulative across the window: {fmt(sum(tc))} of encoder+cold-start "
              f"time on the clip critical path")
        print("  this span INCLUDES the per-clip docker+CUDA cold start, so it is "
              "the\n  number the warm worker shrinks and the sidecar proxy removes.")

    if args.clips:
        print()
        cols = [lbl for lbl, _, _ in INTERVALS]
        print(f"{'at':<10}{'log_id':<22}" + "".join(f"{c[:9]:>10}" for c in cols))
        print("-" * (32 + 10 * len(cols)))
        for r in sorted(rows, key=lambda r: (r["TOTAL"] is None, -(r["TOTAL"] or 0))):
            print(f"{r['at']:<10}{str(r['log_id'])[:21]:<22}"
                  + "".join(f"{fmt(r[c]):>10}" for c in cols)
                  + ("  FAILED" if r["failed"] else ""))

    if args.csv:
        cols = ["log_id", "at", "failed"] + [lbl for lbl, _, _ in INTERVALS]
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {len(rows)} clips -> {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
