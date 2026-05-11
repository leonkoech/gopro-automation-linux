#!/usr/bin/env python3
"""Phase 5.2 / [UBA-221](https://linear.app/uball/issue/UBA-221) — CV-vs-human-plays accuracy report.

Reads CV shadow shots from Firebase (`basketball-games.{id}.cv_logs_staging[]`)
and human-entered plays from Supabase (`plays` rows for the game where
`source != 'cv'`), greedy-matches them in a ±1.0s window, and emits a
per-game markdown + CSV report with precision / recall / classification
accuracy.

Usage (online — fetches from Firebase + Supabase):

    python scripts/cv_infra/ops/accuracy_report.py \\
        --game-id <firebase-game-id> \\
        --supabase-game-id <supabase-uuid> \\
        --out-dir results/cv_accuracy/

Usage (offline — for tests + post-mortems):

    python scripts/cv_infra/ops/accuracy_report.py \\
        --cv-from-json   /tmp/firebase-game.json \\
        --plays-from-json /tmp/supabase-plays.json \\
        --out-dir /tmp/

The offline mode reads:
  * `--cv-from-json`     — Firebase doc (same shape `compare_shadow.py` accepts)
  * `--plays-from-json`  — JSON array of {id, timestamp_seconds, team,
                          classification, source, note?, ...} dicts —
                          shape matches what `plays_sync.py` writes via
                          `UballClient.create_play`.

Metrics:
  * Precision = TP / (TP + FP)
  * Recall    = TP / (TP + FN)   (also called "coverage" in the V1 spec)
  * Made/missed accuracy on matched = correct_classification / TP

Env (online mode only):
  * FIREBASE_CREDENTIALS_PATH | GOOGLE_APPLICATION_CREDENTIALS
  * SUPABASE_URL, SUPABASE_KEY  — annotation-tool Supabase
                                  (mhbrsftxvxxtfgbajrlc per project memory)

Exit codes:
  0  report written
  2  pre-flight failed (missing args / env)
  3  data fetch failed
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


# Re-use the offline-friendly extractor + matcher from compare_shadow.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_shadow import (  # noqa: E402
    Shot,
    extract_cv_shots,
    match_shots,
    _seconds_since_game_start,
)


# Plays classifications produced by plays_sync.py — see plays_sync.SHOT_LABELS.
# We treat any FG_MAKE / *_MAKE / *_MISS / FG_MISS as a "shot" for the
# accuracy calc.
_MADE_CLASSIFICATIONS = {"FG_MAKE", "2PT_MAKE", "3PT_MAKE", "FT_MAKE"}
_MISSED_CLASSIFICATIONS = {"FG_MISS", "2PT_MISS", "3PT_MISS", "FT_MISS"}
_SHOT_CLASSIFICATIONS = _MADE_CLASSIFICATIONS | _MISSED_CLASSIFICATIONS


@dataclass
class AccuracyMetrics:
    tp: int
    fp: int
    fn: int
    matched_made_correct: int      # matched + both said "made"
    matched_missed_correct: int    # matched + both said "missed"
    matched_disagreements: int     # matched + outcomes differ

    @property
    def precision(self) -> float:
        d = self.tp + self.fp
        return self.tp / d if d else 0.0

    @property
    def recall(self) -> float:
        d = self.tp + self.fn
        return self.tp / d if d else 0.0

    @property
    def classification_accuracy(self) -> float:
        # Of the matched shots, what fraction agree on made/missed?
        if self.tp == 0:
            return 0.0
        correct = self.matched_made_correct + self.matched_missed_correct
        return correct / self.tp


@dataclass
class AccuracyReport:
    game_id: str
    cv_shots: List[Shot]
    human_shots: List[Shot]
    matched: List[Tuple[Shot, Shot]]   # (human, cv)
    cv_only: List[Shot]                # false positives (CV claims shot, no human play)
    human_only: List[Shot]             # false negatives (human play, no CV claim)
    metrics: AccuracyMetrics
    game_meta: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------- extraction
def extract_human_shots_from_plays(plays: List[Dict[str, Any]],
                                   *, game_start_iso: Optional[str] = None) -> List[Shot]:
    """Convert Supabase `plays` rows into Shot objects.

    Expected fields per play:
        timestamp_seconds  (float — offset from game start)
        team_side          ("left"|"right") OR team (= "left"|"right")
        classification     (one of plays_sync.SHOT_LABELS keys)
        source             (string; "cv" rows are excluded — those are
                            the CV-emitted plays we're comparing against)

    Falls back to ISO timestamps if `timestamp_seconds` is missing.
    """
    out: List[Shot] = []
    for play in plays or []:
        if play.get("source") == "cv":
            continue  # don't compare CV plays to themselves
        classification = play.get("classification") or play.get("class")
        if classification not in _SHOT_CLASSIFICATIONS:
            continue
        if classification in _MADE_CLASSIFICATIONS:
            outcome = "made"
        else:
            outcome = "missed"
        team = play.get("team_side") or play.get("team")
        if team not in ("left", "right"):
            continue
        ts = play.get("timestamp_seconds")
        if ts is None:
            iso = play.get("timestamp") or play.get("created_at")
            ts = _seconds_since_game_start({"timestamp": iso}, game_start_iso)
        try:
            ts = float(ts) if ts is not None else None
        except (TypeError, ValueError):
            ts = None
        if ts is None:
            continue
        out.append(Shot(timestamp_seconds=ts, team=team, outcome=outcome,
                        source="human", raw=play))
    return sorted(out, key=lambda s: s.timestamp_seconds)


# ---------------------------------------------------------------- comparison
def compute_metrics(matched: List[Tuple[Shot, Shot]],
                    cv_only: List[Shot],
                    human_only: List[Shot]) -> AccuracyMetrics:
    tp = len(matched)
    fp = len(cv_only)
    fn = len(human_only)
    made_correct = 0
    missed_correct = 0
    disagreements = 0
    for human, cv in matched:
        if human.outcome == cv.outcome == "made":
            made_correct += 1
        elif human.outcome == cv.outcome == "missed":
            missed_correct += 1
        else:
            disagreements += 1
    return AccuracyMetrics(
        tp=tp, fp=fp, fn=fn,
        matched_made_correct=made_correct,
        matched_missed_correct=missed_correct,
        matched_disagreements=disagreements,
    )


def build_report(
    *,
    game_id: str,
    cv_shots: List[Shot],
    human_shots: List[Shot],
    window: float = 1.0,
    game_meta: Optional[Dict[str, Any]] = None,
) -> AccuracyReport:
    """Compare CV detections against human plays.

    The match call returns (matched, op_only, cv_only) where the first
    arg is "operator" / "human" — we re-name on the way out to make the
    accuracy semantics explicit.
    """
    matched, human_only, cv_only = match_shots(human_shots, cv_shots, window=window)
    metrics = compute_metrics(matched, cv_only, human_only)
    return AccuracyReport(
        game_id=game_id,
        cv_shots=cv_shots,
        human_shots=human_shots,
        matched=matched,
        cv_only=cv_only,
        human_only=human_only,
        metrics=metrics,
        game_meta=game_meta or {},
    )


# ---------------------------------------------------------------- formatters
def to_markdown(report: AccuracyReport) -> str:
    m = report.metrics
    left_team = report.game_meta.get("leftTeam", {}).get("name", "Lefts")
    right_team = report.game_meta.get("rightTeam", {}).get("name", "Rights")

    rows: List[str] = []
    rows.append(f"# CV accuracy report — game `{report.game_id}`")
    rows.append("")
    if report.game_meta:
        rows.append(f"**Teams**: {left_team} (left) vs {right_team} (right)  ")
        if (created := report.game_meta.get("createdAt")):
            rows.append(f"**Game start**: {created}  ")
    rows.append("")
    rows.append("## Summary")
    rows.append("")
    rows.append("| Metric | Value |")
    rows.append("| --- | --- |")
    rows.append(f"| Human plays  | {len(report.human_shots)} |")
    rows.append(f"| CV detections | {len(report.cv_shots)} |")
    rows.append(f"| True positives (matched)  | {m.tp} |")
    rows.append(f"| False positives (CV-only) | {m.fp} |")
    rows.append(f"| False negatives (human-only) | {m.fn} |")
    rows.append(f"| **Precision** | **{m.precision:.1%}** |")
    rows.append(f"| **Recall**    | **{m.recall:.1%}** |")
    rows.append(f"| Matched made/missed agreement | {m.classification_accuracy:.1%} ({m.matched_disagreements} disagreements) |")
    rows.append("")

    if report.matched:
        rows.append("## Matched shots (TP)")
        rows.append("")
        rows.append("| time (s) | team | human | CV | agreement | cv_conf |")
        rows.append("| ---: | --- | --- | --- | --- | ---: |")
        for human, cv in sorted(report.matched, key=lambda p: p[0].timestamp_seconds):
            agree = "✓" if human.outcome == cv.outcome else "✗"
            conf = f"{cv.confidence:.3f}" if cv.confidence is not None else "—"
            rows.append(
                f"| {human.timestamp_seconds:.2f} | {human.team} | "
                f"{human.outcome} | {cv.outcome} | {agree} | {conf} |"
            )
        rows.append("")

    if report.cv_only:
        rows.append("## False positives (CV detected, no human play)")
        rows.append("")
        rows.append("| time (s) | team | CV outcome | cv_conf |")
        rows.append("| ---: | --- | --- | ---: |")
        for cv in sorted(report.cv_only, key=lambda s: s.timestamp_seconds):
            conf = f"{cv.confidence:.3f}" if cv.confidence is not None else "—"
            rows.append(f"| {cv.timestamp_seconds:.2f} | {cv.team} | {cv.outcome} | {conf} |")
        rows.append("")

    if report.human_only:
        rows.append("## False negatives (human play, no CV detection)")
        rows.append("")
        rows.append("| time (s) | team | human outcome |")
        rows.append("| ---: | --- | --- |")
        for human in sorted(report.human_only, key=lambda s: s.timestamp_seconds):
            rows.append(f"| {human.timestamp_seconds:.2f} | {human.team} | {human.outcome} |")
        rows.append("")

    rows.append("## Reproduce")
    rows.append("")
    rows.append("```")
    rows.append(f"python3 scripts/cv_infra/ops/accuracy_report.py --game-id {report.game_id}")
    rows.append("```")
    return "\n".join(rows) + "\n"


_CSV_HEADER = [
    "type", "game_id", "timestamp_seconds", "team",
    "human_outcome", "cv_outcome", "cv_confidence", "agreement",
]


def to_csv_rows(report: AccuracyReport) -> Iterable[List[Any]]:
    m = report.metrics
    yield [
        "summary", report.game_id, "", "",
        f"human={len(report.human_shots)} cv={len(report.cv_shots)} "
        f"tp={m.tp} fp={m.fp} fn={m.fn} "
        f"precision={m.precision:.3f} recall={m.recall:.3f} "
        f"class_acc={m.classification_accuracy:.3f}",
        "", "", "",
    ]
    for human, cv in report.matched:
        yield [
            "matched", report.game_id, f"{human.timestamp_seconds:.2f}", human.team,
            human.outcome, cv.outcome,
            f"{cv.confidence:.3f}" if cv.confidence is not None else "",
            "agree" if human.outcome == cv.outcome else "disagree",
        ]
    for cv in report.cv_only:
        yield [
            "fp_cv_only", report.game_id, f"{cv.timestamp_seconds:.2f}", cv.team,
            "", cv.outcome,
            f"{cv.confidence:.3f}" if cv.confidence is not None else "",
            "—",
        ]
    for human in report.human_only:
        yield [
            "fn_human_only", report.game_id, f"{human.timestamp_seconds:.2f}", human.team,
            human.outcome, "",
            "",
            "—",
        ]


def write_report(report: AccuracyReport, out_dir: Path) -> Tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{report.game_id}.md"
    csv_path = out_dir / f"{report.game_id}.csv"
    md_path.write_text(to_markdown(report))
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(_CSV_HEADER)
        for row in to_csv_rows(report):
            w.writerow(row)
    return md_path, csv_path


# ---------------------------------------------------------------- online fetch
def _fetch_firebase_game(game_id: str) -> Dict[str, Any]:
    import firebase_admin  # type: ignore[import-not-found]
    from firebase_admin import credentials, firestore  # type: ignore[import-not-found]

    if not firebase_admin._apps:
        cred_path = (os.environ.get("FIREBASE_CREDENTIALS_PATH")
                     or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        if not cred_path:
            raise SystemExit(
                "Firebase auth missing — set FIREBASE_CREDENTIALS_PATH or "
                "GOOGLE_APPLICATION_CREDENTIALS to the admin JSON path."
            )
        firebase_admin.initialize_app(credentials.Certificate(cred_path))
    db = firestore.client()
    doc = db.collection("basketball-games").document(game_id).get()
    if not doc.exists:
        raise SystemExit(f"basketball-games/{game_id} not found")
    data = doc.to_dict() or {}
    data.setdefault("id", game_id)
    return data


def _fetch_supabase_plays(supabase_game_id: str) -> List[Dict[str, Any]]:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit(
            "Supabase auth missing — set SUPABASE_URL + SUPABASE_KEY env vars "
            "for the annotation-tool project (mhbrsftxvxxtfgbajrlc)."
        )
    try:
        from supabase import create_client  # type: ignore[import-not-found]
    except ImportError as e:
        raise SystemExit(
            f"`supabase` package not installed: {e}. "
            "Run `pip install supabase` to use online mode."
        ) from e
    client = create_client(url, key)
    resp = (client.table("plays")
            .select("*")
            .eq("game_id", supabase_game_id)
            .execute())
    return resp.data or []


# ---------------------------------------------------------------- CLI
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--game-id", help="Firebase basketball-games doc ID (online mode)")
    p.add_argument("--supabase-game-id", help="Supabase games.id UUID (online mode)")
    p.add_argument("--cv-source", default="cv_logs_staging",
                   choices=("cv_logs_staging", "logs"),
                   help="Which array on the Firebase doc holds CV shots")
    p.add_argument("--cv-from-json", type=Path,
                   help="Read Firebase doc from JSON file (offline mode)")
    p.add_argument("--plays-from-json", type=Path,
                   help="Read Supabase plays from JSON file (offline mode)")
    p.add_argument("--window", type=float, default=1.0,
                   help="Match window in seconds (default 1.0 — ground "
                        "truth comes from video-locked timestamps)")
    p.add_argument("--out-dir", type=Path, default=Path("results/cv_accuracy"),
                   help="Output directory for the {game_id}.{md,csv} files")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    offline = args.cv_from_json is not None
    online = args.game_id is not None

    if not offline and not online:
        print("error: provide either (--game-id [--supabase-game-id]) or "
              "(--cv-from-json + --plays-from-json)", file=sys.stderr)
        return 2

    # Fetch data.
    if offline:
        if not args.plays_from_json:
            print("error: --cv-from-json requires --plays-from-json", file=sys.stderr)
            return 2
        with args.cv_from_json.open() as f:
            game = json.load(f)
        with args.plays_from_json.open() as f:
            plays = json.load(f)
        gid = game.get("id") or args.cv_from_json.stem
    else:
        game = _fetch_firebase_game(args.game_id)
        sb_gid = args.supabase_game_id or game.get("uballGameId")
        if not sb_gid:
            print("error: --supabase-game-id required (or set uballGameId "
                  "on the Firebase doc)", file=sys.stderr)
            return 2
        plays = _fetch_supabase_plays(sb_gid)
        gid = args.game_id

    cv_shots = extract_cv_shots(game, source_array=args.cv_source)
    human_shots = extract_human_shots_from_plays(
        plays, game_start_iso=game.get("createdAt"))

    report = build_report(
        game_id=gid,
        cv_shots=cv_shots,
        human_shots=human_shots,
        window=args.window,
        game_meta={k: game.get(k) for k in ("leftTeam", "rightTeam", "createdAt")},
    )

    md_path, csv_path = write_report(report, args.out_dir)
    m = report.metrics
    print(f"\nWrote {md_path}")
    print(f"Wrote {csv_path}")
    print(f"\n{report.game_id}:  precision={m.precision:.1%}  "
          f"recall={m.recall:.1%}  "
          f"made/missed acc={m.classification_accuracy:.1%}  "
          f"(tp={m.tp} fp={m.fp} fn={m.fn})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
