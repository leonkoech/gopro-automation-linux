"""
AGX recording-session tracking in Firebase (game-scoped).

Writes `recording-sessions` docs the existing frontend already renders, but in
the AGX shape: angle set explicitly (not derived from a GoPro ap_ssid), one
continuous file per angle (not chapters), and linked to a `firebase_game_id`
up front (the operator starts recording *for* a specific check-in game).

Reuses the shared FirebaseService connection + collection names; writes the
docs directly because register_recording_start() is GoPro-shaped.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from firebase_service import FirebaseService

BASKETBALL_GAMES = "basketball-games"


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def get_active_game(fb: FirebaseService) -> Optional[Dict]:
    """Most-recent un-ended check-in game (status 'active', endedAt null).

    Filters on status only (single-field index, always present) and sorts by
    createdAt in memory — avoids the composite index the order_by query needs.
    """
    try:
        cands = []
        for doc in fb.db.collection(BASKETBALL_GAMES).where("status", "==", "active").stream():
            d = doc.to_dict()
            if not d.get("endedAt"):
                d["id"] = doc.id
                cands.append(d)
        if cands:
            cands.sort(key=lambda x: x.get("createdAt") or "", reverse=True)
            return cands[0]
    except Exception:  # noqa: BLE001
        pass
    return None


class AgxSessionTracker:
    """One recording-sessions doc per angle, game-scoped."""

    def __init__(self, fb: FirebaseService, jetson_id: str):
        self.fb = fb
        self.jetson_id = jetson_id
        self.col = fb.RECORDING_SESSIONS_COLLECTION

    def open(self, label: str, outputs: List[Dict], firebase_game_id: Optional[str]) -> Dict[str, str]:
        """Create a 'recording' session per angle. Returns {angle: session_doc_id}."""
        session_ids: Dict[str, str] = {}
        started = _utcnow()
        for o in outputs:
            angle = o["angle"]
            doc = {
                "jetsonId": self.jetson_id,
                "cameraName": f"AGX {angle}",
                "angleCode": angle,
                "firebaseGameId": firebase_game_id,
                "startedAt": started,
                "endedAt": None,
                "segmentSession": f"{label}_{angle}",
                "interfaceId": o.get("id", ""),        # NDI camera id
                "recordingFile": o["path"],            # local MP4 path (AGX)
                "totalChapters": 1,                    # one continuous file
                "totalSizeBytes": 0,
                "status": "recording",
                "processedGames": [],
            }
            _, ref = self.fb.db.collection(self.col).add(doc)
            session_ids[angle] = ref.id
        return session_ids

    def close(self, session_ids: Dict[str, str], files: List[Dict]) -> None:
        """Mark each angle's session 'stopped' with finalized size/duration."""
        by_angle = {f["angle"]: f for f in files}
        ended = _utcnow()
        for angle, sid in session_ids.items():
            f = by_angle.get(angle, {})
            self.fb.db.collection(self.col).document(sid).update({
                "endedAt": ended,
                "status": "stopped",
                "totalSizeBytes": int(f.get("size") or 0),
                "durationSeconds": f.get("duration"),
                "chapterFiles": [{
                    "filename": (f.get("path") or "").split("/")[-1],
                    "directory": "recordings",
                    "size": int(f.get("size") or 0),
                }],
                "finalizeOk": bool(f.get("ok")),
            })

    def set_status(self, session_ids: Dict[str, str], status: str) -> None:
        for sid in session_ids.values():
            self.fb.db.collection(self.col).document(sid).update({"status": status})

    def set_s3_prefix(self, session_ids: Dict[str, str], s3_prefix: str) -> None:
        for sid in session_ids.values():
            self.fb.db.collection(self.col).document(sid).update({
                "s3Prefix": s3_prefix, "s3UploadedAt": _utcnow(), "status": "uploaded",
            })
