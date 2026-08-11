"""
Ingestion status writer — the `ingestion-runs` Firebase collection.

A dedicated, UI-friendly document (separate from the legacy `pipeline-runs`)
that models ingestion as ordered stages (transcode → upload → register), each
with a status + per-angle detail, plus a `logs` array so the frontend surfaces
*where* something failed without anyone reading backend logs.

Doc shape (id = pipeline_id):
{
  pipeline_id, jetson_id, firebase_game_id, uball_game_id, video_name, date,
  status: 'running'|'completed'|'failed', progress: 0..100,
  angles: ['FL','FR','NL','NR'],
  stages: { transcode|upload|register: {status:'pending'|'running'|'done'|'failed',
                                        done, total, error} },
  angle_status: { FL: {transcode,upload,register}, ... },   # per-angle per-stage
  logs: [ {ts, level:'info'|'warn'|'error', msg} ],
  error, started_at, completed_at, updated_at
}
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

COLLECTION = "ingestion-runs"
STAGES = ("transcode", "upload", "register")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class IngestionRun:
    """Manages one ingestion-runs document. All writes are best-effort."""

    def __init__(self, fb, pipeline_id: str, meta: Dict, angles: List[str],
                 register_angles: Optional[List[str]] = None):
        self.fb = fb
        self.id = pipeline_id
        self.angles = angles
        # only some angles are registered (annotation is 2-angle today)
        self._reg = register_angles if register_angles is not None else ["FL", "FR"]
        self.logs: List[Dict] = []
        totals = {"transcode": len(angles), "upload": len(angles),
                  "register": len([a for a in angles if a in self._reg])}
        self.doc: Dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "jetson_id": meta.get("jetson_id"),
            "firebase_game_id": meta.get("firebase_game_id"),
            "uball_game_id": None,
            "video_name": meta.get("video_name"),
            "date": meta.get("date"),
            "status": "running",
            "progress": 0,
            "angles": angles,
            # where this run's files land in S3 ({bucket, prefix}) + per-angle
            # uploaded keys — so operators can find the footage from the UI
            "s3": None,
            "uploads": {},
            "stages": {s: {"status": "pending", "done": 0, "total": totals[s], "error": None}
                       for s in STAGES},
            "angle_status": {a: {s: "pending" for s in STAGES} for a in angles},
            # Post-register milestones (single ops, not per-angle) — shown as
            # their own nodes on the ingestion card so operators see the full
            # annotation handoff: the game created + the play cards seeded.
            "register_game": {"status": "pending", "error": None},
            "register_plays": {"status": "pending", "created": 0,
                               "with_players": 0, "by_label": {}, "error": None},
            # shot-detection QA (SHOT_QA_ENABLED): the scorekeeper's scored makes
            # checked against the high-fps SL/SR CV; flags scored-but-CV-says-miss.
            # Its own node on the card. status: pending|running|done|failed|disabled.
            "shot_qa": {"status": "pending", "n_scored": 0, "n_confirmed": 0,
                        "n_disagree": 0, "secs": None, "error": None},
            # shot-detection (CV) — the high-fps SL/SR detector's OWN shot count for
            # this game (shadow), independent of the scorekeeper: every make/miss it
            # saw. Its own node on the card so operators see what the CV caught even
            # when nobody scored. status: pending|done|none. Shadow only.
            "shot_detection": {"status": "pending", "n_shots": 0, "n_make": 0,
                               "n_miss": 0, "n_sl": 0, "n_sr": 0, "source": None},
            # shot-detection (FLIR) footage — outcome per SL/SR angle. Separate
            # from the transcode/upload/register stages, which shot footage skips.
            "shots": {},
            # FL<->FR audio cross-correlation sync (offset_frames/offset_sec) or None
            "audio_sync": None,
            "logs": [],
            "error": None,
            "started_at": _now(),
            "completed_at": None,
            "updated_at": _now(),
        }
        self._write(create=True)

    # ---- persistence ----
    def _ref(self):
        return self.fb.db.collection(COLLECTION).document(self.id)

    def _write(self, create: bool = False) -> None:
        if not self.fb:
            return
        self.doc["updated_at"] = _now()
        try:
            self._ref().set(self.doc) if create else self._ref().update(self.doc)
        except Exception:  # noqa: BLE001 — status is best-effort, never break ingestion
            pass

    def _recompute_progress(self) -> None:
        total = sum(s["total"] for s in self.doc["stages"].values()) or 1
        done = sum(s["done"] for s in self.doc["stages"].values())
        self.doc["progress"] = min(99, int(done * 100 / total)) if self.doc["status"] == "running" else self.doc["progress"]

    # ---- events ----
    def log(self, level: str, msg: str) -> None:
        entry = {"ts": _now(), "level": level, "msg": msg}
        self.logs.append(entry)
        self.doc["logs"] = self.logs[-100:]
        self._write()

    def set_uball_game(self, uball_game_id: Optional[str]) -> None:
        self.doc["uball_game_id"] = uball_game_id
        self._write()

    def set_register_game(self, ok: bool, error: Optional[str] = None) -> None:
        """Milestone: the annotation game was (or wasn't) created. The UI builds
        the click-through link from `uball_game_id`."""
        self.doc["register_game"] = {"status": "done" if ok else "failed", "error": error}
        self.log("info", "register game: done") if ok else self.log("error", f"register game: {error}")

    def set_register_plays(self, created: int, with_players: int,
                           by_label: Optional[Dict] = None, ok: bool = True,
                           error: Optional[str] = None) -> None:
        """Milestone: N annotation cards seeded from the scoreboard log,
        `with_players` of them already player-attributed (the rest the annotator
        tags). `by_label` feeds the node's hover breakdown."""
        self.doc["register_plays"] = {
            "status": "done" if ok else "failed",
            "created": created, "with_players": with_players,
            "by_label": by_label or {}, "error": error,
        }
        if ok:
            self.log("info", f"register plays: {created} card(s), {with_players} with players")
        else:
            self.log("error", f"register plays: {error}")

    def set_shot_qa(self, status: str, n_scored: int = 0, n_confirmed: int = 0,
                    n_disagree: int = 0, secs: Optional[float] = None,
                    error: Optional[str] = None) -> None:
        """Milestone: post-game QA of the scorekeeper's scored makes against the
        high-fps shot cams. status: running|done|failed|disabled|skipped. Shadow
        only — never mutates scores/cards; `n_disagree` = scored-but-CV-says-miss."""
        self.doc["shot_qa"] = {"status": status, "n_scored": n_scored,
                               "n_confirmed": n_confirmed, "n_disagree": n_disagree,
                               "secs": secs, "error": error}
        if status == "done":
            self.log("info", f"shot QA: {n_confirmed}/{n_scored} makes CV-confirmed, "
                             f"{n_disagree} flagged for review")
        elif status == "failed":
            self.log("error", f"shot QA: {error}")
        elif status == "running":
            self.log("info", f"shot QA: validating {n_scored} scored makes…")
        else:
            self.log("info", f"shot QA: {status}")

    def set_shot_detection(self, n_shots: int, n_make: int, n_miss: int,
                           n_sl: int, n_sr: int, source: str = "live",
                           status: str = "done") -> None:
        """Milestone: the high-fps CV's own shot count for this game (shadow) —
        every make/miss it saw on SL/SR, independent of the scorekeeper. status:
        done|none. `source` = live (real-time shadow) or auto (post-game scan)."""
        self.doc["shot_detection"] = {"status": status, "n_shots": n_shots,
                                      "n_make": n_make, "n_miss": n_miss,
                                      "n_sl": n_sl, "n_sr": n_sr, "source": source}
        if status == "done":
            self.log("info", f"shot detection (CV): {n_shots} shots "
                             f"({n_sl} SL / {n_sr} SR), {n_make} make / {n_miss} miss")
        else:
            self._write()

    def set_s3(self, bucket: str, prefix: str) -> None:
        """Record the S3 folder this run uploads into (shown in the UI)."""
        self.doc["s3"] = {"bucket": bucket, "prefix": prefix}
        self._write()

    def set_upload(self, angle: str, s3_key: str, size: Optional[int] = None) -> None:
        """Record one angle's uploaded S3 key (shown in the UI)."""
        entry = {"s3_key": s3_key}
        if size is not None:
            entry["size"] = size
        self.doc["uploads"][angle] = entry
        self._write()

    def set_shot(self, angle: str, status: str, **meta) -> None:
        """Record a shot-detection (FLIR) camera's footage outcome for the UI.
        status: 'uploaded' | 'kept_local' | 'failed'. meta carries fps,
        resolution, basket_side, and s3_key/path/error as available."""
        self.doc["shots"][angle] = {"status": status,
                                    **{k: v for k, v in meta.items() if v is not None}}
        self._write()

    def set_audio_sync(self, data: Dict) -> None:
        """Record the FL<->FR audio cross-correlation result (per-game frame offset)."""
        self.doc["audio_sync"] = data
        self._write()

    def start_stage(self, stage: str) -> None:
        self.doc["stages"][stage]["status"] = "running"
        self.log("info", f"{stage}: started")

    def angle_done(self, stage: str, angle: str) -> None:
        self.doc["angle_status"][angle][stage] = "done"
        self.doc["stages"][stage]["done"] += 1
        self._recompute_progress()
        self._write()

    def angle_failed(self, stage: str, angle: str, err: str) -> None:
        self.doc["angle_status"][angle][stage] = "failed"
        self.doc["stages"][stage]["error"] = err
        self.log("error", f"{stage} {angle}: {err}")

    def finish_stage(self, stage: str) -> None:
        st = self.doc["stages"][stage]
        st["status"] = "failed" if any(
            self.doc["angle_status"][a].get(stage) == "failed"
            for a in self.angles) else "done"
        self.log("info", f"{stage}: {st['status']} ({st['done']}/{st['total']})")

    def complete(self, message: str = "") -> None:
        any_failed = any(s["status"] == "failed" for s in self.doc["stages"].values())
        self.doc["status"] = "completed_with_errors" if any_failed else "completed"
        self.doc["progress"] = 100
        self.doc["completed_at"] = _now()
        self.log("info", message or f"ingestion {self.doc['status']}")

    def fail(self, err: str) -> None:
        self.doc["status"] = "failed"
        self.doc["error"] = err
        self.doc["completed_at"] = _now()
        self.log("error", f"ingestion failed: {err}")
