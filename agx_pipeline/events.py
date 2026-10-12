"""Event stream, written alongside what the pipeline already writes.

This emits the shape proposed in docs/EVENT_SCHEMA.md — one envelope for
everything, four kinds of message, sport-neutral observations separated from the
basketball interpretation of them. Nothing reads what it writes.

That is the point. The schema is a proposal argued on paper, and the only honest
test of it is whether real games fit. So the detector writes its existing record
exactly as before and, when EVENT_STREAM_ENABLED is on, also writes events;
afterwards scripts/event_stream_compare.py checks whether the events reconstruct
the record they were derived from. Fields that turn out to be unfillable, or
filled with something meaningless, are the schema being wrong — which is cheaper
to find here than after the pipeline has been restructured around it.

It is deliberately a dual write rather than a replacement. Nothing downstream
changes, nothing on the live path depends on this, and the flag is off by
default. If the comparison holds over a few games, this same writer becomes the
first phase of the migration; if it does not, deleting this file costs nothing.

Every write is best-effort and swallows its own errors — an unread side-channel
must never be able to interfere with a game.

Env:
    EVENT_STREAM_ENABLED   "true" to write events (default false)
    EVENT_STREAM_DIR       where they go (default: <output>/events)
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

from logging_service import get_logger

logger = get_logger("agx.events")

SCHEMA = "uai.event.v1"
_DEFAULT_OUTPUT = "/home/dev/app/recordings"

_lock = threading.Lock()
_output_dir: Optional[str] = None


def enabled() -> bool:
    return os.getenv("EVENT_STREAM_ENABLED", "false").strip().lower() in (
        "1", "true", "yes", "on")


def configure(output_dir: str) -> None:
    global _output_dir
    _output_dir = output_dir


def root() -> str:
    env = os.getenv("EVENT_STREAM_DIR", "").strip()
    return env or os.path.join(_output_dir or _DEFAULT_OUTPUT, "events")


def path_for(game_id: str) -> str:
    safe = str(game_id).replace(os.sep, "_").replace("..", "_") or "unknown"
    return os.path.join(root(), f"{safe}.jsonl")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def event_id(site_id: str, surface_id: str, stage: str, t_start: str,
             structure: str) -> str:
    """A derived id, per the schema: the same footage scanned twice yields the
    same id instead of a duplicate. Reprocessing is routine here — backfills,
    re-cuts, the deferred scan of segments the live loop never reached — so a
    random id would create a second event where a match is wanted."""
    key = "|".join(str(x) for x in (site_id, surface_id, stage, t_start, structure))
    return "ev_" + hashlib.sha256(key.encode()).hexdigest()[:12]


def emit(game_id: str, events: List[Dict]) -> None:
    """Append events for one game. Never raises.

    Appends rather than rewrites so a killed run keeps whatever it had already
    written, and so this file can be read while a game is still being played.
    """
    if not game_id or not events or not enabled():
        return
    try:
        path = path_for(game_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        line = "".join(json.dumps(e, sort_keys=True) + "\n" for e in events)
        with _lock:
            with open(path, "a") as fh:
                fh.write(line)
                fh.flush()
                os.fsync(fh.fileno())
    except Exception as exc:  # noqa: BLE001 -- a side-channel must never break a game
        logger.warning("event emit failed for %s: %s", game_id, exc)


def read(game_id: str) -> List[Dict]:
    """Every event written for a game, in order. Never raises."""
    out: List[Dict] = []
    try:
        with open(path_for(game_id)) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue      # a torn final line after a kill; keep the rest
    except Exception:  # noqa: BLE001 -- no file yet is not an error
        return out
    return out


def _envelope(kind: str, game_id: str, site_id: str, surface_id: str,
              t_start: str, t_end: str, stage: str, version: str, method: str,
              observed_by: List[str], confidence: Optional[float],
              scale: str, ev_id: str, external_ids: Optional[Dict] = None,
              phase: str = "unknown") -> Dict:
    e = {
        "id": ev_id,
        "schema": SCHEMA,
        "game_id": game_id,
        "site_id": site_id,
        "surface_id": surface_id,
        "kind": kind,
        "t": {"start": t_start, "end": t_end},
        "observed_by": observed_by,
        "produced_by": {"stage": stage, "version": version, "method": method},
        "emitted_at": _now(),
        # Not a probability. It gates human review, and the scale says how much
        # weight the number can carry -- the detector's is not calibrated.
        "confidence": {"value": confidence, "scale": scale},
        # Nothing live knows the phase yet; the post-game pass corrects it
        # through an enrichment rather than editing this event.
        "phase": phase,
    }
    if external_ids:
        e["external_ids"] = external_ids
    return e


def from_shot(rec: Dict, verdict: Dict, game_id: str, site_id: str,
              surface_id: str, version: str,
              external_ids: Optional[Dict] = None) -> List[Dict]:
    """One detected shot, as the schema would carry it.

    `rec` is the shadow record the detector already builds and `verdict` the
    dict logic.decide returned. Two messages come out of one shot, which is the
    separation the whole schema turns on: a sport-neutral observation that the
    ball crossed the hoop plane, and a basketball interpretation of what that
    was worth. Only the second mentions scoring.

    The shot is an instant, so t.start == t.end. A rally would not be, which is
    why the field is a pair even here.
    """
    wc = rec.get("wallclock")
    if not wc:
        return []     # no wall-clock anchor: nothing downstream could use this
    structure = "goal_" + str(rec.get("side") or "unknown")
    obs_id = event_id(site_id, surface_id, "shot_detect", wc, structure)
    angle = rec.get("cam")

    obs = _envelope(
        "observation.plane_cross", game_id, site_id, surface_id, wc, wc,
        "shot_detect", version, str(verdict.get("decided_by") or "unknown"),
        [angle] if angle else [], None, "none", obs_id, external_ids)
    obs["where"] = {"structure": structure, "direction": "downward"}
    # rho is the ball's distance from the rim centre at the crossing. It stays
    # here, opaque to anything generic, rather than being flattened into a
    # confidence it is not comparable with.
    obs["evidence"] = {"offset": verdict.get("rho"),
                       "geo": verdict.get("geo"),
                       "segment": rec.get("seg"),
                       "t_in_window_s": rec.get("t_shot")}

    out = [obs]
    if rec.get("made"):
        interp = _envelope(
            "interpretation.score", game_id, site_id, surface_id, wc, wc,
            "shot_detect", version, "verdict",
            [angle] if angle else [], None, "none",
            event_id(site_id, surface_id, "interp", wc, structure))
        interp["derived_from"] = [obs_id]
        # Which physical hoop, not which team. The team that benefits depends on
        # the period and which end they started at, and side_attribution.py owns
        # that; putting a hoop into credit.team is the confusion this schema is
        # trying not to inherit.
        interp["credit"] = {"team": None, "player": None}
        # Point value is typing's answer, and it arrives much later as an
        # enrichment. The detector only knows a basket was made.
        interp["value"] = {"points": None, "class": None}
        out.append(interp)
    return out
