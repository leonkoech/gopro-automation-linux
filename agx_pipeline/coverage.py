"""One coverage record per game: how much work each stage expected, and how much it did.

A count on its own ("163 shots") says a stage ran. It does not say whether the
stage got through the whole game, because there is nothing to compare it
against. This module holds that comparison, in one place and one shape:

    {"game_id": "...", "status": "closed", "stages": {
        "shot_detect": {"expected": 1966, "processed": 1939, "complete": false, ...},
        "typing":      {"expected": 47,   "processed": 47,   "complete": true,  ...}}}

Every stage reports the same three fields, so a reader can check any of them
without knowing what a segment or a clip is. The record also carries a status:

    absent    no stage ever started
    running   a stage started and the game has not finished
    closed    the game ended and every stage reported

A stage that is switched on but never reports is written in as ``silent``.

Stages report in one of two ways. A stage that knows its totals at the end calls
`record()` directly. A stage that counts as it goes -- the typing queue, for
instance -- calls `register_source()` once, and `finalize()` pulls the snapshot
when the game ends.

Every write is best-effort and swallows its own errors so instrumentation cannot
stop a game.

Env:
    SHOT_COVERAGE_DIR        where records are written (default: <output>/coverage)
    SHOT_COVERAGE_STALE_MIN  how long a "running" record may sit before the run
                             is treated as having died (default 30)
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from logging_service import get_logger

logger = get_logger("agx.coverage")

# Stage name -> the env flag that turns it on. A stage is expected to report iff
# its flag is on, which is what makes silence detectable: we know it should have
# spoken. Keep this list in step with the flags the service actually reads.
STAGE_FLAGS: Dict[str, str] = {
    "shot_detect": "SHOT_LIVE_ENABLED",
    "typing": "SHOT_LIVE_TYPING",
    "who": "SHOT_LIVE_WHO_SCAN",
    "live_stream": "LIVE_STREAM_ENABLED",
}

_DEFAULT_OUTPUT = "/home/dev/app/recordings"
# How long a record may sit marked "running" before we treat the run as
# having died rather than still going.
STALE_AFTER_MIN = float(os.getenv("SHOT_COVERAGE_STALE_MIN", "30"))

_lock = threading.Lock()
_output_dir: Optional[str] = None
# stage -> callable returning (expected, processed, detail). Registered by
# long-lived stages so finalize() can pull totals without a handle on them.
_sources: Dict[str, Callable[[], Tuple[Optional[int], Optional[int], Dict]]] = {}


def _on(flag: str) -> bool:
    return os.getenv(flag, "false").strip().lower() in ("1", "true", "yes", "on")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def configure(output_dir: str) -> None:
    """Point coverage at the recordings root (the service knows it; this module
    should not have to guess). SHOT_COVERAGE_DIR still wins if it is set."""
    global _output_dir
    _output_dir = output_dir


def root() -> str:
    env = os.getenv("SHOT_COVERAGE_DIR", "").strip()
    if env:
        return env
    return os.path.join(_output_dir or _DEFAULT_OUTPUT, "coverage")


def path_for(game_id: str) -> str:
    # game ids come from upstream; keep them from escaping the directory.
    safe = str(game_id).replace(os.sep, "_").replace("..", "_") or "unknown"
    return os.path.join(root(), f"{safe}.json")


def enabled_stages() -> Dict[str, bool]:
    """Which stages were switched on for this run."""
    return {stage: _on(flag) for stage, flag in STAGE_FLAGS.items()}


def read(game_id: str) -> Dict:
    """The record as it stands, or {} when there is none. Never raises."""
    try:
        with open(path_for(game_id)) as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001 -- a missing or half-written record is not an error here
        return {}


def register_source(
    stage: str,
    fn: Callable[[], Tuple[Optional[int], Optional[int], Dict]],
) -> None:
    """Let `finalize()` pull (expected, processed, detail) from a running stage.

    Used by stages that count as they go and have no natural end-of-game hook of
    their own. Registering twice replaces the earlier source, so a restarted
    stage does not report stale totals.
    """
    with _lock:
        _sources[stage] = fn


def record(game_id: str, stage: str, expected: Optional[int],
           processed: Optional[int], **detail) -> None:
    """Write one stage's coverage into this game's record. Never raises.

    `expected` is what the stage should have handled; `processed` is what it
    did. Either may be None when a stage genuinely cannot know -- that is
    reported as unknown rather than guessed, because a fabricated denominator is
    worse than an honest gap.
    """
    if not game_id:
        return          # unattached recording: no game to attribute coverage to
    try:
        _merge(game_id, {stage: _entry(expected, processed, detail)})
    except Exception as exc:  # noqa: BLE001 -- coverage must never break the pipeline
        logger.warning("coverage record failed for %s/%s: %s", game_id, stage, exc)


def finalize(game_id: str) -> Dict:
    """Close the record: pull every registered source, mark enabled-but-silent
    stages, log one line, and return the record. Never raises.

    The silent-stage pass is the whole point. A stage that was on and never
    reported is the shape both real incidents took, and it is only visible by
    comparing what was enabled against what spoke.
    """
    if not game_id:
        return {}       # unattached recording: nothing was ever recorded for it
    try:
        with _lock:
            sources = dict(_sources)
        pulled: Dict[str, Dict] = {}
        for stage, fn in sources.items():
            try:
                expected, processed, detail = fn()
                pulled[stage] = _entry(expected, processed, detail or {})
            except Exception as exc:  # noqa: BLE001 -- one bad source must not lose the rest
                logger.warning("coverage source %s failed: %s", stage, exc)

        doc = _merge(game_id, pulled, closing=True)
        stages = doc.get("stages", {})
        silent = []
        for stage, on in enabled_stages().items():
            if not on:
                continue
            if stage not in stages:
                stages[stage] = _entry(None, None, {}, silent=True)
                silent.append(stage)
        if silent:
            doc["stages"] = stages
            _write(game_id, doc)
            logger.error(
                "COVERAGE: %s was enabled and reported nothing for game=%s — that "
                "is the shape of a stage that cannot run, not of a quiet night",
                ", ".join(sorted(silent)), game_id)

        logger.info("coverage game=%s %s", game_id, summarize(doc))
        return doc
    except Exception as exc:  # noqa: BLE001
        logger.warning("coverage finalize failed for %s: %s", game_id, exc)
        return {}


def recent(hours: float = 24.0) -> List[Dict]:
    """Every record touched within the window, newest first. Never raises."""
    out: List[Dict] = []
    try:
        cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
        for name in os.listdir(root()):
            if not name.endswith(".json"):
                continue
            try:
                path = os.path.join(root(), name)
                if os.path.getmtime(path) < cutoff:
                    continue
                with open(path) as fh:
                    out.append(json.load(fh))
            except Exception:  # noqa: BLE001 -- one unreadable record loses only itself
                continue
    except Exception:  # noqa: BLE001 -- no directory yet is not an error
        return []
    return sorted(out, key=lambda d: d.get("updated_at", ""), reverse=True)


def _age_min(iso: str) -> Optional[float]:
    try:
        when = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - when).total_seconds() / 60.0
    except Exception:  # noqa: BLE001
        return None


def problems(hours: float = 24.0) -> List[str]:
    """What is wrong with the settled games in the window, in words.

    A record is settled once it is closed, or once it has gone quiet for
    STALE_AFTER_MIN while still marked running -- the second case being a run
    that ended without reaching its stop path. A game still being played is
    skipped, so this can be polled as often as anything else without reporting
    work that is simply still going.
    """
    found: List[str] = []
    for doc in recent(hours):
        game = doc.get("game_id", "?")
        age = _age_min(doc.get("updated_at", ""))
        running = doc.get("status") != "closed"
        if running and (age is None or age < STALE_AFTER_MIN):
            continue                      # still in progress; nothing to say yet
        if running:
            found.append(f"{game}: started and never finished "
                         f"(no update for {age:.0f} min) — {summarize(doc)}")
        for stage, e in sorted(doc.get("stages", {}).items()):
            if e.get("silent"):
                found.append(f"{game}: {stage} was enabled and never reported")
            elif e.get("complete") is False:
                found.append(f"{game}: {stage} covered {e.get('processed')} of "
                             f"{e.get('expected')}")
    return found


def summarize(doc: Dict) -> str:
    """One readable line: `shot_detect 1939/1966 typing 47/47`."""
    parts = []
    for stage, e in sorted(doc.get("stages", {}).items()):
        if e.get("silent"):
            parts.append(f"{stage} SILENT")
        else:
            exp, proc = e.get("expected"), e.get("processed")
            parts.append(f"{stage} {'?' if proc is None else proc}/"
                         f"{'?' if exp is None else exp}")
    return " ".join(parts) or "no stages reported"


# ---- internals ----------------------------------------------------------- #

def _entry(expected: Optional[int], processed: Optional[int],
           detail: Dict, silent: bool = False) -> Dict:
    e: Dict = {
        "expected": None if expected is None else int(expected),
        "processed": None if processed is None else int(processed),
        "at": _now(),
    }
    # `complete` is only meaningful when both numbers are known; None says "we
    # cannot tell", which a checker must treat differently from False.
    if e["expected"] is None or e["processed"] is None:
        e["complete"] = None
    else:
        e["complete"] = e["processed"] >= e["expected"]
    if silent:
        e["silent"] = True
    if detail:
        e["detail"] = detail
    return e


def _merge(game_id: str, stages: Dict[str, Dict], closing: bool = False) -> Dict:
    doc = read(game_id) or {"game_id": game_id, "started_at": _now(), "stages": {}}
    doc.setdefault("stages", {}).update(stages)
    doc["updated_at"] = _now()
    # The status is what separates the three cases a checker has to tell apart:
    #   no record at all      -- nothing ever started (or the box died before it could)
    #   status "running"      -- a stage began and never closed: killed, OOM, power
    #   status "closed"       -- the game ended and every stage had its say
    # Only the third is a night you can read coverage numbers off with confidence,
    # and the second is invisible unless the record is written as the game runs
    # rather than only at the end.
    # Closed is sticky. Stages stop on their own threads and a slow one can
    # report after the game has been closed out; that late number is worth
    # keeping, but it must not put the record back into "running" and make a
    # finished game look like one that died.
    if closing:
        doc["status"] = "closed"
        doc["closed_at"] = doc["updated_at"]
    elif doc.get("status") != "closed":
        doc["status"] = "running"
    _write(game_id, doc)
    return doc


def _write(game_id: str, doc: Dict) -> None:
    """Atomic replace, so a reader never sees a half-written record."""
    path = path_for(game_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
