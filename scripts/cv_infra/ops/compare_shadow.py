#!/usr/bin/env python3
"""Side-by-side shadow-vs-operator comparison for the V1 CV pipeline.

[UBA-228](https://linear.app/uball/issue/UBA-228) Option A — CSV export. Reads a Firebase basketball-games doc
and emits a per-game shot-count breakdown so the client can decide
whether to cut over from `cv_logs_staging` (shadow) to `logs`
(production) per UBA-229.

The match window is intentionally loose (default 5s): operators enter
scores from the bench, sometimes a few seconds after the actual play,
so an exact-timestamp match would under-count agreements. Tune via
``--window``.

Usage:

    # Single game, CSV to stdout (one summary row + per-shot rows):
    python scripts/cv_infra/ops/compare_shadow.py --game-id <firebase-game-id>

    # Multiple games:
    python scripts/cv_infra/ops/compare_shadow.py \\
        --game-id g1 --game-id g2 --game-id g3 \\
        --output shadow-report-week1.csv

    # Offline mode — feed it a JSON file with the shape Firebase returns:
    python scripts/cv_infra/ops/compare_shadow.py --from-json /tmp/game.json

    # Terminal table instead of CSV:
    python scripts/cv_infra/ops/compare_shadow.py --game-id <id> --format table

Output columns (CSV):
    type, game_id, timestamp_seconds, team, made_or_missed,
    operator_present, cv_present, agreement, cv_confidence

Where ``type`` is one of:
    summary   — one row per game with aggregate counts (first in the CSV)
    matched   — operator + CV agree within ±window
    cv_only   — CV detected, no nearby operator score
    op_only   — operator scored, no nearby CV detection

Firebase auth: uses `FIREBASE_CREDENTIALS_PATH` (a JSON file path) or
``GOOGLE_APPLICATION_CREDENTIALS``. Falls back to the
``firebase_admin.initialize_app()`` ADC chain — i.e. the same env
contract the rest of `gopro-automation-linux` expects.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------- dataclasses
@dataclass(frozen=True)
class Shot:
    timestamp_seconds: float
    team: str               # "left" | "right"
    outcome: str            # "made" | "missed"
    source: str             # "operator" | "cv"
    confidence: Optional[float] = None    # only set for CV
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GameReport:
    game_id: str
    operator_shots: List[Shot]
    cv_shots: List[Shot]
    matched: List[Tuple[Shot, Shot]]
    op_only: List[Shot]
    cv_only: List[Shot]


# ---------------------------------------------------------------- extraction
# Per UBA-217, CV writes the same actionTypes as the operator
# (`score_added` for made shots; `shot_missed` for missed shots);
# the difference is `payload.source == "cv"`. Earlier emitter revisions
# wrote `actionType: "cv_shot"` with `payload.outcome` — kept as a
# back-compat path below so a doc with both shapes can still be
# compared cleanly.
_MADE_ACTIONS = {"score_added"}
_MISSED_ACTIONS = {"shot_missed"}
_SHOT_ACTIONS = _MADE_ACTIONS | _MISSED_ACTIONS

# Back-compat aliases (early external callers imported these).
_OPERATOR_MADE_ACTIONS = _MADE_ACTIONS
_OPERATOR_MISSED_ACTIONS = _MISSED_ACTIONS
_CV_ACTIONS = {"cv_shot"}  # legacy actionType — still accepted on read


def _action_to_outcome(action: Optional[str]) -> Optional[str]:
    if action in _MADE_ACTIONS:
        return "made"
    if action in _MISSED_ACTIONS:
        return "missed"
    return None


def _seconds_since_game_start(log: Dict[str, Any],
                              game_start_iso: Optional[str]) -> Optional[float]:
    """Compute log.timestamp - game.createdAt in seconds.

    Returns None if either side is unparseable — caller skips the entry.
    """
    from datetime import datetime
    log_ts = log.get("timestamp")
    if not log_ts or not game_start_iso:
        return None
    try:
        log_dt = datetime.fromisoformat(str(log_ts).replace("Z", "+00:00"))
        start_dt = datetime.fromisoformat(str(game_start_iso).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return (log_dt - start_dt).total_seconds()


def extract_operator_shots(game: Dict[str, Any]) -> List[Shot]:
    """Yield operator-entered shots from `basketball-games.logs[]`.

    Skips entries with `payload.source == "cv"` so a post-cutover game
    (where CV shots ALSO land in `logs[]`) doesn't double-count CV
    detections as operator entries.
    """
    out: List[Shot] = []
    game_start = game.get("createdAt")
    for log in game.get("logs", []) or []:
        outcome = _action_to_outcome(log.get("actionType"))
        if outcome is None:
            continue
        payload = log.get("payload") or {}
        if payload.get("source") == "cv":
            continue
        ts = _seconds_since_game_start(log, game_start)
        if ts is None:
            continue
        team = log.get("team")
        if team not in ("left", "right"):
            continue
        out.append(Shot(timestamp_seconds=ts, team=team, outcome=outcome,
                        source="operator", raw=log))
    return sorted(out, key=lambda s: s.timestamp_seconds)


def extract_cv_shots(game: Dict[str, Any], *,
                     source_array: str = "cv_logs_staging") -> List[Shot]:
    """Yield CV-emitted shots from `game[source_array]`.

    Two emission shapes are accepted:
      * UBA-217 production shape — `actionType: "score_added"` or
        `"shot_missed"` with `payload.source == "cv"`. Outcome comes
        from the actionType.
      * Legacy back-compat — `actionType: "cv_shot"` with
        `payload.outcome ∈ {"made", "missed"}`. Older shadow runs
        emitted this shape; still readable so an in-progress shadow
        window remains comparable after a deploy.
    """
    out: List[Shot] = []
    game_start = game.get("createdAt")
    for log in game.get(source_array, []) or []:
        payload = log.get("payload") or {}
        action = log.get("actionType")
        outcome = _action_to_outcome(action)
        if outcome is not None and payload.get("source") == "cv":
            # Production path.
            pass
        elif action == "cv_shot":
            # Legacy back-compat.
            outcome = payload.get("outcome")
            if outcome not in ("made", "missed"):
                continue
        else:
            continue
        ts = _seconds_since_game_start(log, game_start)
        if ts is None:
            continue
        team = log.get("team")
        if team not in ("left", "right"):
            continue
        confidence = payload.get("confidence")
        try:
            confidence = float(confidence) if confidence is not None else None
        except (TypeError, ValueError):
            confidence = None
        out.append(Shot(timestamp_seconds=ts, team=team, outcome=outcome,
                        source="cv", confidence=confidence, raw=log))
    return sorted(out, key=lambda s: s.timestamp_seconds)


# ---------------------------------------------------------------- matching
def match_shots(
    operator_shots: List[Shot],
    cv_shots: List[Shot],
    *,
    window: float,
) -> Tuple[List[Tuple[Shot, Shot]], List[Shot], List[Shot]]:
    """Greedy temporal match within ``window`` seconds + same team.

    Picks each operator shot the nearest unclaimed CV shot inside the
    window with matching team. Outcome agreement is RECORDED (in the
    matched tuple's confidence field via raw) but doesn't gate the
    match — a "scored 2 but CV said missed" is a useful disagreement
    to surface, not a non-match.
    """
    cv_used: set = set()
    matched: List[Tuple[Shot, Shot]] = []
    op_only: List[Shot] = []

    for op in operator_shots:
        best_idx: Optional[int] = None
        best_dt = window + 1
        for i, cv in enumerate(cv_shots):
            if i in cv_used:
                continue
            if cv.team != op.team:
                continue
            dt = abs(cv.timestamp_seconds - op.timestamp_seconds)
            if dt < best_dt and dt <= window:
                best_dt = dt
                best_idx = i
        if best_idx is None:
            op_only.append(op)
        else:
            cv_used.add(best_idx)
            matched.append((op, cv_shots[best_idx]))

    cv_only = [c for i, c in enumerate(cv_shots) if i not in cv_used]
    return matched, op_only, cv_only


def build_report(game_id: str, game: Dict[str, Any], *,
                 window: float,
                 cv_source: str = "cv_logs_staging") -> GameReport:
    operator = extract_operator_shots(game)
    cv = extract_cv_shots(game, source_array=cv_source)
    matched, op_only, cv_only = match_shots(operator, cv, window=window)
    return GameReport(
        game_id=game_id,
        operator_shots=operator,
        cv_shots=cv,
        matched=matched,
        op_only=op_only,
        cv_only=cv_only,
    )


# ---------------------------------------------------------------- output
def _agreement(op: Shot, cv: Shot) -> str:
    return "agree" if op.outcome == cv.outcome else "disagree"


def csv_rows(report: GameReport) -> Iterable[List[Any]]:
    # Summary row first.
    matched_agree = sum(1 for op, cv in report.matched if op.outcome == cv.outcome)
    cv_made   = sum(1 for c in report.cv_shots if c.outcome == "made")
    cv_missed = sum(1 for c in report.cv_shots if c.outcome == "missed")
    op_made   = sum(1 for o in report.operator_shots if o.outcome == "made")
    op_missed = sum(1 for o in report.operator_shots if o.outcome == "missed")

    yield [
        "summary", report.game_id, "", "",
        f"op={op_made}m/{op_missed}x  cv={cv_made}m/{cv_missed}x  "
        f"matched={len(report.matched)} (agree={matched_agree})  "
        f"op_only={len(report.op_only)}  cv_only={len(report.cv_only)}",
        "", "", "", "",
    ]
    for op, cv in report.matched:
        yield [
            "matched", report.game_id,
            f"{op.timestamp_seconds:.2f}",
            op.team,
            f"op={op.outcome}, cv={cv.outcome}",
            "yes", "yes",
            _agreement(op, cv),
            f"{cv.confidence:.3f}" if cv.confidence is not None else "",
        ]
    for op in report.op_only:
        yield [
            "op_only", report.game_id,
            f"{op.timestamp_seconds:.2f}",
            op.team, op.outcome,
            "yes", "no", "—", "",
        ]
    for cv in report.cv_only:
        yield [
            "cv_only", report.game_id,
            f"{cv.timestamp_seconds:.2f}",
            cv.team, cv.outcome,
            "no", "yes", "—",
            f"{cv.confidence:.3f}" if cv.confidence is not None else "",
        ]


_CSV_HEADER = [
    "type", "game_id", "timestamp_seconds", "team", "made_or_missed",
    "operator_present", "cv_present", "agreement", "cv_confidence",
]


def write_csv(reports: List[GameReport], target) -> None:
    writer = csv.writer(target)
    writer.writerow(_CSV_HEADER)
    for r in reports:
        for row in csv_rows(r):
            writer.writerow(row)


def write_table(reports: List[GameReport], target) -> None:
    for r in reports:
        rows = list(csv_rows(r))
        target.write(f"\n=== game {r.game_id} ===\n")
        widths = [
            max(len(_CSV_HEADER[i]), *(len(str(row[i])) for row in rows))
            for i in range(len(_CSV_HEADER))
        ]
        line = "  ".join(_CSV_HEADER[i].ljust(widths[i]) for i in range(len(_CSV_HEADER)))
        target.write(line + "\n")
        target.write("-" * len(line) + "\n")
        for row in rows:
            target.write("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(row))) + "\n")


# ---------------------------------------------------------------- Firebase fetch
def _fetch_game_firebase(game_id: str) -> Dict[str, Any]:
    """Fetch one basketball-games doc via firebase_admin.

    Uses the same env contract as `firebase_service.py` —
    FIREBASE_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS.
    """
    import firebase_admin  # type: ignore[import-not-found]
    from firebase_admin import credentials, firestore  # type: ignore[import-not-found]

    if not firebase_admin._apps:
        cred_path = (os.environ.get("FIREBASE_CREDENTIALS_PATH")
                     or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        if not cred_path:
            raise RuntimeError(
                "Firebase auth missing — set FIREBASE_CREDENTIALS_PATH "
                "or GOOGLE_APPLICATION_CREDENTIALS to the admin JSON path."
            )
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc = db.collection("basketball-games").document(game_id).get()
    if not doc.exists:
        raise SystemExit(f"basketball-games/{game_id} not found")
    data = doc.to_dict() or {}
    data.setdefault("id", game_id)
    return data


# ---------------------------------------------------------------- CLI
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Side-by-side shadow-vs-operator shot comparison for V1 CV.")
    p.add_argument("--game-id", action="append", default=[],
                   help="Firebase basketball-games doc ID (repeat for multiple games)")
    p.add_argument("--from-json", type=Path,
                   help="Read a Firebase doc JSON from a file instead of "
                        "calling Firebase. Useful for tests + offline use.")
    p.add_argument("--cv-source", default="cv_logs_staging",
                   choices=("cv_logs_staging", "logs"),
                   help="Which array on the Firebase doc holds CV shots "
                        "(default: cv_logs_staging — set to logs after "
                        "the Phase 7 cutover).")
    p.add_argument("--window", type=float, default=5.0,
                   help="Match window in seconds (default 5.0).")
    p.add_argument("--output", type=Path,
                   help="Write CSV to a file instead of stdout.")
    p.add_argument("--format", default="csv", choices=("csv", "table"),
                   help="Output format (default csv).")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    if not args.game_id and not args.from_json:
        print("error: provide --game-id (repeatable) or --from-json", file=sys.stderr)
        return 2

    reports: List[GameReport] = []

    if args.from_json:
        with args.from_json.open() as f:
            game = json.load(f)
        gid = game.get("id") or args.from_json.stem
        reports.append(build_report(gid, game, window=args.window,
                                    cv_source=args.cv_source))

    for gid in args.game_id:
        game = _fetch_game_firebase(gid)
        reports.append(build_report(gid, game, window=args.window,
                                    cv_source=args.cv_source))

    target = args.output.open("w", newline="") if args.output else sys.stdout
    try:
        if args.format == "csv":
            write_csv(reports, target)
        else:
            write_table(reports, target)
    finally:
        if args.output:
            target.close()

    if args.output:
        print(f"wrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
