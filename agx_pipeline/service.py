"""
AGX ingestion service (Flask) — P0 recording surface.

Serves the frontend-compatible HTTP routes on the AGX so the existing
gopro-automation-wb dashboard drives it unchanged, and writes Firebase
`recording-sessions` + `pipeline-runs`. Recording is game-scoped: start
resolves the active check-in game and links every angle to its
`firebase_game_id`.

Ingestion (transcode -> black-fill -> upload -> annotation register) is P1;
`_start_ingestion` is the hook where it plugs in.

Run on the AGX from the repo root:
    FIREBASE_CREDENTIALS_PATH=./uball-gopro-fleet-firebase-adminsdk.json \
      python3 -m agx_pipeline.service
"""

from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from flask import Flask, jsonify, request

from firebase_service import get_firebase_service
from agx_pipeline.recording import RecordingController, load_config
from agx_pipeline.camrec_controller import CamrecController
from agx_pipeline.sessions import AgxSessionTracker, get_active_game
from agx_pipeline.notifier import CameraAlerter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
logger = logging.getLogger("agx.service")

app = Flask(__name__)
CFG = load_config()
# Recording backend: "camrec" drives geoffbauer's per-camera FastAPI recorder
# (robust, watchdog-supervised); "gstreamer" (default) is our own single
# gst-launch. Both expose the same start()/stop() interface.
_BACKEND = os.getenv("RECORDING_BACKEND", "gstreamer").lower()
CONTROLLER = CamrecController(CFG) if _BACKEND == "camrec" else RecordingController(CFG)
logger.info("recording backend: %s", _BACKEND)
FB = get_firebase_service()
TRACKER = AgxSessionTracker(FB, CFG.jetson_id) if FB else None
ALERTER = CameraAlerter(CFG.jetson_id, CFG.location)  # emails UAI on camera up<->down

# in-memory current recording / pipeline state (Firebase is the durable copy)
_lock = threading.Lock()
_current: Dict[str, object] = {}   # {label, firebase_game_id, container, outputs, session_ids, started_at}
_last_pipeline: Optional[Dict] = None
_RELAY = None                      # Firebase relay (status heartbeat + command consumer)
_auto_recorded: set = set()        # firebase_game_ids already auto-handled (avoid re-recording)
AUTO_RECORD = os.getenv("AUTO_RECORD_ON_GAME", "true").lower() in ("1", "true", "yes")
AUTO_MAX_AGE_MIN = int(os.getenv("AUTO_RECORD_MAX_AGE_MIN", "45"))       # ignore games older than this
AUTO_MAX_DURATION_MIN = int(os.getenv("AUTO_RECORD_MAX_DURATION_MIN", "180"))  # safety auto-stop


def _age_seconds(iso: Optional[str]) -> float:
    if not iso:
        return 1e12
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:  # noqa: BLE001
        return 1e12


def _auto_follow() -> None:
    """Auto start/stop recording from the check-in game lifecycle (runs every ~3s).

    Start when a game freshly goes 'active' in check-in; stop when that game ends
    (or a max-duration safety net trips). Freshness guard avoids recording a
    stale left-active game forever on service restart.
    """
    if not AUTO_RECORD or not FB:
        return
    cur_id = _current.get("firebase_game_id") if _current else None
    if cur_id:
        g = FB.get_game(cur_id)
        ended = (not g) or g.get("status") != "active" or g.get("endedAt")
        too_long = _age_seconds(_current.get("started_at")) > AUTO_MAX_DURATION_MIN * 60
        if ended or too_long:
            logger.info("auto-stop: game %s (%s)", cur_id, "ended" if ended else "max-duration")
            _auto_recorded.add(cur_id)
            _do_stop()
    else:
        active = get_active_game(FB)
        aid = active["id"] if active else None
        if aid and aid not in _auto_recorded:
            if _age_seconds(active.get("createdAt")) <= AUTO_MAX_AGE_MIN * 60:
                logger.info("auto-start: check-in game %s went active", aid)
                _auto_recorded.add(aid)
                _do_start(aid, None)
            else:
                _auto_recorded.add(aid)  # stale left-active game — ignore, never auto-record


_conn_cache: Dict[str, tuple] = {}   # ip -> (up: bool, monotonic_ts)


def _camera_up(ip: str) -> bool:
    """Is the camera reachable? Cached ~12s so the heartbeat stays cheap."""
    now = time.monotonic()
    cached = _conn_cache.get(ip)
    if cached and now - cached[1] < 12:
        return cached[0]
    try:
        up = subprocess.run(["ping", "-c", "1", "-W", "1", ip],
                            capture_output=True, timeout=3).returncode == 0
    except Exception:  # noqa: BLE001
        up = False
    _conn_cache[ip] = (up, now)
    return up


def _device_state() -> Dict:
    """Snapshot for agx-devices/{jetson_id}: cameras + recording + current ingestion."""
    rec = bool(_current)
    cams = [{"id": c.id, "angle": c.angle, "ip": c.ip, "up": _camera_up(c.ip)}
            for c in CFG.cameras]
    ALERTER.check(cams)  # email UAI on any camera up<->down transition (debounced)
    return {
        "cameras": cams,
        "recording": {
            "active": rec,
            "label": _current.get("label") if rec else None,
            "firebase_game_id": _current.get("firebase_game_id") if rec else None,
            "angles": [o["angle"] for o in _current.get("outputs", [])] if rec else [],
            "started_at": _current.get("started_at") if rec else None,
        },
        "current_ingestion": (_last_pipeline or {}).get("pipeline_id"),
        "location": CFG.location,
    }


def _publish() -> None:
    if _RELAY:
        _RELAY.publish()


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _label() -> str:
    return "game_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# --------------------------------------------------------------------------- #
# Camera / system info
# --------------------------------------------------------------------------- #
@app.route("/health")
def health():
    return jsonify({"status": "ok", "jetson_id": CFG.jetson_id,
                    "firebase": FB is not None, "recording": bool(_current)})


@app.route("/api/gopros")
def gopros():
    recording = bool(_current)
    cams = [{
        "id": c.id, "name": f"AGX {c.angle}", "interface": c.angle,
        "ip": c.ip, "status": "connected", "is_recording": recording,
    } for c in CFG.cameras]
    return jsonify({"success": True, "gopros": cams})


@app.route("/api/system/info")
def system_info():
    total, used, free = shutil.disk_usage(CFG.output_dir if os.path.isdir(CFG.output_dir) else "/")
    return jsonify({"success": True, "hostname": socket.gethostname(),
                    "jetson_id": CFG.jetson_id,
                    "disk": {"total": total, "used": used, "free": free}})


# --------------------------------------------------------------------------- #
# Recording control
# --------------------------------------------------------------------------- #
def _do_start(game_id=None, label=None, force=False):
    """Start recording. Shared by the HTTP route and the Firebase relay.

    `force` is the operator "Start anyway" override: it bypasses the
    no-active-game guard (records unattached, `firebase_game_id=None`, so it can
    be linked later) and never blocks on camera reachability — if no camera
    pings it attempts all of them anyway, since a ping is not a reliable signal
    that a camera can (or cannot) stream RTSP.
    """
    with _lock:
        if _current:
            return {"success": False, "error": "already recording",
                    "label": _current.get("label")}, 409
        if not game_id and FB:
            g = get_active_game(FB)
            game_id = g["id"] if g else None
        if not game_id and not force:
            return {"success": False,
                    "error": "no firebase_game_id and no active check-in game"}, 400
        label = label or _label()
        # Record only cameras that are reachable, so one down camera doesn't
        # break the whole recording (single gst-launch fails if any rtspsrc dies).
        up_cams = [c.id for c in CFG.cameras if _camera_up(c.ip)]
        if force:
            # Override never blocks on cameras: use the reachable set, or fall
            # back to every configured camera when none ping (ping != streaming).
            cam_ids = up_cams or [c.id for c in CFG.cameras]
        else:
            cam_ids = up_cams
            if not cam_ids:
                return {"success": False, "error": "no cameras reachable — cannot record"}, 503
        down = [c.angle for c in CFG.cameras if c.id not in cam_ids]
        if down:
            logger.warning("recording without offline cameras: %s", down)
        try:
            started = CONTROLLER.start(label, camera_ids=cam_ids)
        except Exception as e:  # noqa: BLE001
            logger.error("start failed: %s", e)
            return {"success": False, "error": str(e)}, 500
        session_ids = TRACKER.open(label, started["outputs"], game_id) if TRACKER else {}
        _current.update({
            "label": label, "firebase_game_id": game_id,
            "container": started["container"], "outputs": started["outputs"],
            "session_ids": session_ids, "started_at": started["started_at"],
            "forced": bool(force),
        })
    logger.info("recording started label=%s game=%s force=%s angles=%s",
                label, game_id, bool(force), [o["angle"] for o in started["outputs"]])
    _publish()
    return {"success": True, "label": label, "firebase_game_id": game_id, "forced": bool(force),
            "angles": [o["angle"] for o in started["outputs"]], "sessions": session_ids}, 200


def _do_stop():
    """Stop recording + kick off ingestion. Shared by the HTTP route and the relay."""
    with _lock:
        if not _current:
            return {"success": False, "error": "not recording"}, 409
        state = dict(_current)
        _current.clear()
    try:
        stopped = CONTROLLER.stop(state["label"], outputs=state["outputs"])
    except Exception as e:  # noqa: BLE001
        logger.error("stop failed: %s", e)
        return {"success": False, "error": str(e)}, 500
    if TRACKER:
        TRACKER.close(state["session_ids"], stopped["files"])
    pipeline_id = uuid.uuid4().hex[:8]
    _create_pipeline_run(pipeline_id, state, stopped)
    threading.Thread(target=_start_ingestion, name=f"ingest-{pipeline_id}",
                     args=(pipeline_id, state, stopped), daemon=True).start()
    _publish()
    finalized = sum(1 for f in stopped["files"] if f.get("ok"))
    return {"success": True, "pipeline_id": pipeline_id,
            "sessions_count": len(state["session_ids"]),
            "angles_finalized": finalized,
            "files": [{"angle": f["angle"], "ok": f.get("ok"),
                       "duration": f.get("duration")} for f in stopped["files"]]}, 200


@app.route("/api/recording/start", methods=["POST"])
def start_recording():
    body = request.get_json(silent=True) or {}
    payload, status = _do_start(body.get("firebase_game_id"), body.get("label"),
                                bool(body.get("force")))
    return jsonify(payload), status


@app.route("/api/recording/stop-all-and-process", methods=["POST"])
def stop_all_and_process():
    payload, status = _do_stop()
    return jsonify(payload), status


@app.route("/api/recording/pipeline-status")
def pipeline_status():
    recording = {}
    if _current:
        for o in _current.get("outputs", []):
            recording[o["id"]] = {"is_recording": True, "is_stopping": False,
                                  "stage": "recording", "stage_message": "Recording",
                                  "angle_code": o["angle"]}
    return jsonify({"success": True, "recording": recording, "pipeline": _last_pipeline})


# --------------------------------------------------------------------------- #
# Pipeline-run (Firebase) — matches the frontend PipelineRun schema
# --------------------------------------------------------------------------- #
def _create_pipeline_run(pipeline_id: str, state: Dict, stopped: Dict) -> None:
    global _last_pipeline
    sessions = {sid: {"session_id": sid, "angle_code": angle,
                      "display_label": f"{datetime.now(timezone.utc):%m/%d/%Y} {angle}",
                      "status": "pending", "chapters_total": 1, "chapters_uploaded": 0,
                      "bytes_uploaded": 0}
                for angle, sid in state["session_ids"].items()}
    run = {
        "pipeline_id": pipeline_id, "jetson_id": CFG.jetson_id, "jetson_name": CFG.jetson_id,
        "status": "running", "stage": "processing_games",
        "stage_message": "Ingestion queued (transcode → upload → register)",
        "progress": 5, "sessions_total": len(sessions), "games_total": 1,
        "started_at": _utcnow(), "sessions": sessions,
        "games": {state["firebase_game_id"]: {
            "firebase_game_id": state["firebase_game_id"], "game_number": 1,
            "status": "pending", "batch_jobs": []}},
    }
    _last_pipeline = run
    if FB:
        try:
            FB.create_pipeline_run(run)
        except Exception as e:  # noqa: BLE001
            logger.error("create_pipeline_run failed: %s", e)


def _start_ingestion(pipeline_id: str, state: Dict, stopped: Dict) -> None:
    """Run P1 ingestion (transcode → upload → register), driving the ingestion-runs doc."""
    from agx_pipeline.ingest import run_ingestion
    try:
        run_ingestion(FB, CFG, pipeline_id, state, stopped, TRACKER)
    except Exception as e:  # noqa: BLE001
        logger.error("ingestion %s crashed: %s", pipeline_id, e)


if __name__ == "__main__":
    port = int(os.getenv("AGX_SERVICE_PORT", "5000"))
    if FB:
        from agx_pipeline.relay import Relay
        _RELAY = Relay(FB, CFG.jetson_id, _device_state, _do_start, _do_stop, auto_fn=_auto_follow)
        _RELAY.start()
    logger.info("AGX service starting on :%d (firebase=%s, cameras=%d, relay=%s, auto_record=%s)",
                port, FB is not None, len(CFG.cameras), _RELAY is not None, AUTO_RECORD)
    app.run(host="0.0.0.0", port=port)
