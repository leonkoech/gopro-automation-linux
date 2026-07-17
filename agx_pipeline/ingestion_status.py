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
from typing import Dict, List, Optional

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
        self.doc = {
            "pipeline_id": pipeline_id,
            "jetson_id": meta.get("jetson_id"),
            "firebase_game_id": meta.get("firebase_game_id"),
            "uball_game_id": None,
            "video_name": meta.get("video_name"),
            "date": meta.get("date"),
            "status": "running",
            "progress": 0,
            "angles": angles,
            "stages": {s: {"status": "pending", "done": 0, "total": totals[s], "error": None}
                       for s in STAGES},
            "angle_status": {a: {s: "pending" for s in STAGES} for a in angles},
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
