#!/usr/bin/env python3
"""
Achieved time-to-clip, from what the game docs already recorded.

Every highlight's progress is written to `basketball-games/{game}.highlights.
{log_id}`, and every write stamps `updatedAt`. So the moment each clip became
ready is on record for every clip ever produced -- no instrumentation to
deploy, and it works retroactively.

The start of the clock depends on which path triggered the clip:

  CV clips        the log_id encodes it: `cv_<epoch>_<side>` (same parse
                  service._retry_failed_highlights uses). epoch is the shot
                  moment, so ready - epoch is the COMPLETE CV time-to-clip,
                  detector lag included. This is the ~8-minute figure, measured.

  scorekeeper     the game doc's `logs` array has an entry whose `id` matches
                  the log_id and whose `timestamp` is when the play happened.
                  ready - timestamp therefore includes however long the
                  scorekeeper took to notice and tap: an UPPER BOUND on what
                  the pipeline cost, not the pipeline itself.

Why not better than an upper bound: highlight._mark writes the whole sub-map
rather than merging into it --

    .update({f"highlights.{log_id}": {**patch, "updatedAt": _utcnow()}})

-- so the "ready" write replaces the earlier one and destroys `requestedAt`,
which is the moment the service actually accepted the request. Adding
requestedAt to the ready patch would make the scorekeeper number exact from
then on; this script reports it whenever it happens to still be present (a
clip caught mid-flight).

Caveat worth remembering when reading the output: a clip that failed and was
re-cut by _retry_failed_highlights reuses its log_id, so its `updatedAt` is
the retry's completion, not the original attempt. Those show up as large
outliers -- --max-min drops them.

Run where Firebase creds live, normally the AGX:

    python3 scripts/clip_latency_firestore.py --games 10
    python3 scripts/clip_latency_firestore.py --game <firebase_game_id> --each
    python3 scripts/clip_latency_firestore.py --games 20 --csv clips.csv
"""

from __future__ import annotations

import argparse
import csv
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
except ImportError:
    pass

GAMES = "basketball-games"
FIREBASE_CREDENTIALS_PATH = os.getenv(
    "FIREBASE_CREDENTIALS_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)),
                 "uball-gopro-fleet-firebase-adminsdk.json"))

# cv_<epoch>_<side> -- the same shape _retry_failed_highlights parses.
RE_CV_ID = re.compile(r"^cv_(\d{9,})_([A-Za-z]+)$")


def init_firebase():
    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        raise SystemExit(
            f"Firebase credentials not found: {FIREBASE_CREDENTIALS_PATH}\n"
            "Set FIREBASE_CREDENTIALS_PATH, or run on the AGX.")
    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(
            credentials.Certificate(FIREBASE_CREDENTIALS_PATH))
    return firestore.client()


def parse_ts(v) -> Optional[datetime]:
    """Accept an ISO string (with or without microseconds), a Firestore
    timestamp, or epoch seconds/millis -- the logs array is written by the
    scorekeeper app and is not guaranteed to match the AGX's own format."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        s = float(v)
        if s > 1e11:            # milliseconds
            s /= 1000.0
        return datetime.fromtimestamp(s, tz=timezone.utc)
    if not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def clip_rows(game: dict) -> List[dict]:
    highlights = game.get("highlights") or {}
    if not isinstance(highlights, dict):
        return []
    logs_by_id = {l.get("id"): l for l in (game.get("logs") or [])
                  if isinstance(l, dict) and l.get("id")}
    left = (game.get("leftTeam") or {}).get("name")
    right = (game.get("rightTeam") or {}).get("name")
    rows = []
    for log_id, h in highlights.items():
        if not isinstance(h, dict):
            continue
        ready = parse_ts(h.get("updatedAt"))
        m = RE_CV_ID.match(log_id or "")
        if m:
            path, t0 = "cv", parse_ts(int(m.group(1)))
        else:
            path = "scorekeeper"
            log = logs_by_id.get(log_id) or {}
            t0 = parse_ts(log.get("timestamp"))
        rows.append({
            "game": game.get("_id"),
            "matchup": f"{left} vs {right}" if left and right else None,
            "log_id": log_id,
            "path": path,
            "status": h.get("status"),
            "angle": h.get("angle"),
            # only present on a clip caught mid-flight; the ready write wipes it
            "requested_at": h.get("requestedAt"),
            "trigger_at": t0.isoformat() if t0 else None,
            "ready_at": ready.isoformat() if ready else None,
            "latency_s": (round((ready - t0).total_seconds(), 1)
                          if (ready and t0) else None),
        })
    return rows


def fetch_games(db, limit: int, game_id: Optional[str]) -> List[dict]:
    if game_id:
        snap = db.collection(GAMES).document(game_id).get()
        if not snap.exists:
            raise SystemExit(f"no such game: {game_id}")
        return [{**(snap.to_dict() or {}), "_id": snap.id}]
    # order_by on one field needs no composite index; a doc missing createdAt
    # would be skipped by the ordered query, so fall back to an unordered read.
    try:
        q = (db.collection(GAMES)
               .order_by("createdAt", direction=firestore.Query.DESCENDING)
               .limit(limit))
        docs = list(q.stream())
    except Exception as e:  # noqa: BLE001
        print(f"ordered query failed ({e}); falling back to unordered",
              file=sys.stderr)
        docs = list(db.collection(GAMES).limit(limit).stream())
    return [{**(d.to_dict() or {}), "_id": d.id} for d in docs]


def pct(v: List[float], p: float) -> Optional[float]:
    if not v:
        return None
    s = sorted(v)
    return s[min(len(s) - 1, int(round((p / 100) * (len(s) - 1))))]


def fmt(s: Optional[float]) -> str:
    if s is None:
        return "-"
    if s < 120:
        return f"{s:.0f}s"
    return f"{s / 60:.1f}m"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Achieved time-to-clip from the basketball-games docs")
    ap.add_argument("--games", type=int, default=10,
                    help="most recent N games (default 10)")
    ap.add_argument("--game", help="one firebase_game_id instead")
    ap.add_argument("--max-min", type=float, default=None, metavar="MIN",
                    help="drop clips slower than this many minutes -- use it to "
                         "exclude re-cut clips, whose updatedAt is the retry")
    ap.add_argument("--each", action="store_true", help="list every clip")
    ap.add_argument("--csv", help="write per-clip rows here")
    args = ap.parse_args()

    rows: List[dict] = []
    for g in fetch_games(init_firebase(), args.games, args.game):
        rows += clip_rows(g)
    if not rows:
        print("no highlights found on those games")
        return 0

    status = {}
    for r in rows:
        status[r["status"]] = status.get(r["status"], 0) + 1
    print(f"{len(rows)} clips across "
          f"{len({r['game'] for r in rows})} game(s)   "
          + "  ".join(f"{k}={v}" for k, v in sorted(status.items(), key=str)))

    dropped = 0
    timed = [r for r in rows if r["latency_s"] is not None]
    if args.max_min is not None:
        keep = [r for r in timed if r["latency_s"] <= args.max_min * 60]
        dropped = len(timed) - len(keep)
        timed = keep
    print()
    print(f"{'path':<14}{'n':>5}{'p50':>9}{'p90':>9}{'max':>9}   measures")
    print("-" * 78)
    for path, note in (("scorekeeper", "play -> ready (includes the tap delay)"),
                       ("cv", "shot -> ready (includes detector lag)")):
        v = [r["latency_s"] for r in timed if r["path"] == path]
        print(f"{path:<14}{len(v):>5}{fmt(pct(v,50)):>9}{fmt(pct(v,90)):>9}"
              f"{fmt(pct(v,100)):>9}   {note}")

    untimed = [r for r in rows if r["latency_s"] is None]
    if untimed or dropped:
        print()
        if dropped:
            print(f"{dropped} clip(s) over {args.max_min:g} min excluded "
                  f"(likely re-cuts; their updatedAt is the retry, not the "
                  f"first attempt)")
        if untimed:
            no_trigger = sum(1 for r in untimed if not r["trigger_at"])
            no_ready = sum(1 for r in untimed if not r["ready_at"])
            print(f"{len(untimed)} clip(s) not timed: {no_trigger} with no "
                  f"trigger time (scorekeeper clip with no matching logs[] "
                  f"entry), {no_ready} never reached a ready write")
    inflight = [r for r in rows if r["requested_at"]]
    if inflight:
        print(f"{len(inflight)} clip(s) still carry requestedAt (caught "
              f"mid-flight) -- for those, pipeline-only time is measurable")

    if args.each:
        print()
        print(f"{'log_id':<26}{'path':<13}{'status':<11}{'latency':>9}  trigger")
        for r in sorted(rows, key=lambda r: (r["latency_s"] is None,
                                             -(r["latency_s"] or 0))):
            print(f"{str(r['log_id'])[:25]:<26}{r['path']:<13}"
                  f"{str(r['status']):<11}{fmt(r['latency_s']):>9}  "
                  f"{r['trigger_at'] or '-'}")

    if args.csv:
        cols = list(rows[0])
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {len(rows)} clips -> {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
