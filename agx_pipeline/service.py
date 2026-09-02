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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, Optional

from flask import Flask, jsonify, request

from firebase_service import get_firebase_service
from agx_pipeline.recording import RecordingController, load_config
from agx_pipeline.camrec_controller import CamrecController
from agx_pipeline.shot_recording import AravisRecorder
from agx_pipeline.highlight import HighlightBuffer, cut_highlight
from agx_pipeline.grafana_annotate import annotate
from agx_pipeline.shot_detect.live import LiveShotScorer, live_enabled
from agx_pipeline.side_attribution import scoring_hoop_side
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
# Shot-detection cameras (FLIR/GigE) record via a separate host-side Aravis
# recorder, alongside whichever backend records the tracking (RTSP) angles.
SHOT = AravisRecorder(CFG) if CFG.shot_cameras else None
# Highlight sidecar: closed short segments per hoop side so a clip can be cut
# mid-game (raw masters have no moov until stop) from the camera that filmed the
# scoring team's basket (issue #4). Best-effort.
HIGHLIGHT = (HighlightBuffer(CFG)
             if os.getenv("HIGHLIGHT_RECORDER", "true").lower() in ("1", "true", "yes")
             else None)
logger.info("recording backend: %s (shot cameras: %d)", _BACKEND, len(CFG.shot_cameras))
FB = get_firebase_service()
TRACKER = AgxSessionTracker(FB, CFG.jetson_id) if FB else None
ALERTER = CameraAlerter(CFG.jetson_id, CFG.location)  # emails UAI on camera up<->down

# in-memory current recording / pipeline state (Firebase is the durable copy)
_lock = threading.Lock()
_current: Dict[str, object] = {}   # {label, firebase_game_id, container, outputs, session_ids, started_at}
_last_pipeline: Optional[Dict] = None
_RELAY = None                      # Firebase relay (status heartbeat + command consumer)
_auto_recorded: set = set()        # firebase_game_ids already auto-handled (avoid re-recording)
_active_ingests = 0                # in-flight ingestion threads (GPU HW-transcode)


def is_gpu_free() -> bool:
    """True only when nothing is recording AND no ingestion is transcoding — the
    gate the deferred shot-QA worker honors so it NEVER competes with a live game
    for the GPU. Recording + live highlights always win."""
    with _lock:
        return not _current and _active_ingests == 0


def transcode_active() -> bool:
    """True only while an ingestion is in its GPU TRANSCODE stage — NOT its long
    upload/register phases. The LIVE shot loop pauses on this so it yields the GPU
    for the actual transcode but keeps detecting through a prior game's upload
    window (ingests run ~30min but only ~7-15min is transcode; games are ~10min
    apart, so the coarse whole-ingest gate would starve games 2/3 of live data).
    It runs DURING recording by design; recording (NVENC) is unaffected regardless."""
    try:
        from agx_pipeline.ingest import is_transcoding
        return is_transcoding()
    except Exception:  # noqa: BLE001
        return False


def _live_should_pause() -> bool:
    """Yield to an ingest transcode ONLY when no game is recording. During a
    live game the detector's verdicts ARE the product (green button + CV
    scorecard) and its CUDA scan barely touches the NVENC/NVDEC engines the
    transcode uses. Night 1 (2026-08-13): the previous game's 54-min transcode
    started 6s before the next tip-off and the old unconditional pause starved
    the detector for the whole first half — 55-min verdict lag, every clip
    outside the highlight buffer, zero green buttons."""
    with _lock:
        rec = bool(_current)
    return (not rec) and transcode_active()


# Real-time shot detector over the live SL/SR segments (shadow-first, off unless
# SHOT_LIVE_ENABLED). Reads only the segments produced when SHOT_SEGMENT_ENABLED
# is on; yields to an ingest transcode only between games (_live_should_pause).
LIVE = (LiveShotScorer(CFG, FB, should_pause=_live_should_pause,
                       on_make=lambda cmd: _do_highlight(cmd))
        if SHOT and live_enabled() else None)
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
    cams = [{"id": c.id, "angle": c.angle, "ip": c.ip, "up": _camera_up(c.ip),
             "role": "tracking"} for c in CFG.cameras]
    cams += [{"id": c.id, "angle": c.angle, "ip": c.ip, "up": _camera_up(c.ip),
              "role": "shot_detection", "basket_side": c.basket_side}
             for c in CFG.shot_cameras]
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
        "highlight": HIGHLIGHT.status() if HIGHLIGHT else None,
        "shot_live": LIVE.status() if LIVE else None,
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
    } for c in list(CFG.cameras) + list(CFG.shot_cameras)]
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
        # Start the tracking + shot recorders CONCURRENTLY so the two camera
        # families begin capturing within ~1s of each other, not the variable
        # 3-15s skew a sequential start produced (tracking's launch+verify used
        # to delay the shot start, and its duration varied per game). Residual
        # latency (RTSP jitter-buffer vs Aravis) is small and correctable
        # downstream. The matching concurrent stop is in _do_stop.
        started_at = _utcnow()
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_track = ex.submit(CONTROLLER.start, label, cam_ids)
            # Shot cams (FLIR/GigE): best-effort, never blocks the game. Not
            # ping-gated — a busy/missing GigE cam's gst-launch dies in the
            # settle window and is dropped; all configured shot cams are tried.
            f_shot = (ex.submit(SHOT.start, label, [c.id for c in CFG.shot_cameras])
                      if SHOT and CFG.shot_cameras else None)
            try:
                started = f_track.result()
            except Exception as e:  # noqa: BLE001
                logger.error("start failed: %s", e)
                if f_shot:
                    try:
                        f_shot.result()   # let it finish launching, then
                        SHOT.stop(label)  # don't leave shot cams recording
                    except Exception:  # noqa: BLE001
                        pass
                return {"success": False, "error": str(e)}, 500
            outputs = [{**o, "role": "tracking"} for o in started["outputs"]]
            if f_shot:
                try:
                    outputs += f_shot.result()["outputs"]
                except Exception as e:  # noqa: BLE001
                    logger.error("shot cameras did not start (recording tracking only): %s", e)
        session_ids = TRACKER.open(label, outputs, game_id) if TRACKER else {}
        _current.update({
            "label": label, "firebase_game_id": game_id,
            "container": started["container"], "outputs": outputs,
            "session_ids": session_ids, "started_at": started_at,
            "forced": bool(force),
        })
        if HIGHLIGHT:
            try:
                HIGHLIGHT.start(label)
            except Exception as e:  # noqa: BLE001
                logger.warning("highlight recorder failed to start: %s", e)
        if LIVE:
            try:
                LIVE.start(label, game_id, _starting_side_team1(game_id))
            except Exception as e:  # noqa: BLE001
                logger.warning("shot-live failed to start: %s", e)
    logger.info("recording started label=%s game=%s force=%s angles=%s",
                label, game_id, bool(force), [o["angle"] for o in outputs])
    _publish()
    return {"success": True, "label": label, "firebase_game_id": game_id, "forced": bool(force),
            "angles": [o["angle"] for o in outputs], "sessions": session_ids}, 200


def _do_stop():
    """Stop recording + kick off ingestion. Shared by the HTTP route and the relay."""
    with _lock:
        if not _current:
            return {"success": False, "error": "not recording"}, 409
        state = dict(_current)
        _current.clear()
        # Tear BOTH recorders down under the lock: (1) a failure stopping the
        # tracking recorder must not skip the shot recorder — an un-signalled
        # gst-launch keeps the GigE cameras locked forever; (2) holding the lock
        # through teardown stops a concurrent start (relay auto-loop or operator)
        # from grabbing a camera whose previous process hasn't exited yet.
        outs = state["outputs"]
        track_outs = [o for o in outs if o.get("role") != "shot_detection"]
        shot_outs = [o for o in outs if o.get("role") == "shot_detection"]
        stopped = {"label": state["label"], "files": []}
        track_err = None
        # Stop both recorders CONCURRENTLY so the SIGINT (which ends capture)
        # lands at ~the same instant for both families — matching the concurrent
        # start above, so both start- and end-skew stay small.
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_track = ex.submit(CONTROLLER.stop, state["label"], track_outs)
            f_shot = (ex.submit(SHOT.stop, state["label"], shot_outs)
                      if SHOT and shot_outs else None)
            try:
                stopped = f_track.result()
            except Exception as e:  # noqa: BLE001
                track_err = str(e)
                logger.error("tracking stop failed: %s", e)
            if f_shot:
                try:
                    stopped.setdefault("files", []).extend(f_shot.result()["files"])
                except Exception as e:  # noqa: BLE001
                    logger.error("shot stop failed: %s", e)
        if HIGHLIGHT:
            try:
                HIGHLIGHT.stop()
            except Exception as e:  # noqa: BLE001
                logger.warning("highlight recorder stop failed: %s", e)
        if LIVE:
            try:
                LIVE.stop()
            except Exception as e:  # noqa: BLE001
                logger.warning("shot-live stop failed: %s", e)
    if track_err is not None:
        return {"success": False, "error": track_err}, 500
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


def _do_preview():
    """Snapshot every camera -> S3 -> presigned URLs for the dashboard preview
    grid. Shared by the HTTP route and the relay. Best-effort per camera."""
    from agx_pipeline.preview import capture_all
    try:
        previews = capture_all(CFG)
        return {"success": True, "previews": previews,
                "up_count": sum(1 for p in previews if p["up"]), "total": len(previews)}, 200
    except Exception as e:  # noqa: BLE001
        logger.error("preview failed: %s", e)
        return {"success": False, "error": str(e)[:300]}, 500


_start_side_cache: Dict[str, str] = {}


def _starting_side_team1(game_id: str) -> Optional[str]:
    """startingSideTeam1 ('left'/'right') off the game doc — which hoop team1
    attacks at tip-off. Set by the operator at check-in, so it may be unset at
    the very first score; we cache only valid values and re-read until it
    appears. Returns None when unknown (caller then falls back side-agnostically)."""
    cached = _start_side_cache.get(game_id)
    if cached in ("left", "right"):
        return cached
    if not FB:
        return None
    try:
        snap = FB.db.collection("basketball-games").document(game_id).get()
        val = (snap.to_dict() or {}).get("startingSideTeam1") if snap.exists else None
    except Exception as e:  # noqa: BLE001
        logger.warning("startingSideTeam1 read failed (%s): %s", game_id, e)
        return None
    if val in ("left", "right"):
        _start_side_cache[game_id] = val
        return val
    return None


def _do_highlight(cmd: Dict):
    """Queue a highlight-clip cut (docs/HIGHLIGHT_CLIP_INTEGRATION_PLAN.md
    Stage 1). ACKs immediately; the cut runs on a daemon thread and reports
    via basketball-games/{game}.highlights.{logId}. Shared by the HTTP route
    and the Firebase relay."""
    if not HIGHLIGHT:
        return {"success": False, "error": "highlight recorder disabled"}, 501
    log_id = cmd.get("logId") or cmd.get("log_id")
    ts = cmd.get("ts")
    if log_id:
        # Earliest point in the whole chain: a make was detected (or a
        # scorekeeper log) and this request reached the service.
        annotate(f"highlight detected: {log_id}", ["highlight", "detected", str(log_id)])
    with _lock:
        game_id = cmd.get("firebase_game_id") or _current.get("firebase_game_id")
        label = _current.get("label")
    if not (log_id and ts and game_id):
        return {"success": False, "error": "logId, ts and firebase_game_id required"}, 400
    try:
        t_epoch = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return {"success": False, "error": f"unparseable ts: {ts}"}, 400
    # Route the cut to the camera on the scoring team's current hoop side
    # (issue #4): team+period ride on the command, startingSideTeam1 is on the
    # game doc. Unknown side → left recorder (the prior single-buffer default).
    sst = _starting_side_team1(game_id)
    # CV-triggered cuts carry the hoop side directly (SL=left / SR=right);
    # scorekeeper cuts derive it from team+period as before.
    side = (cmd.get("side") if cmd.get("side") in ("left", "right")
            else scoring_hoop_side(cmd.get("team"), cmd.get("period"), sst))
    recorder = HIGHLIGHT.recorder_for(side)
    # Post-night retries (2026-08-19): with the recorder stopped, a cut is
    # still valid as long as the label's buffer segments are on disk — the
    # night-end retry sweep runs exactly then. cmd may carry the label.
    retry_label = cmd.get("label") or label
    if not HIGHLIGHT.active():
        if not (retry_label and recorder.segments(retry_label)):
            return {"success": False,
                    "error": "recorder not running and no segments for label"}, 409
        label = retry_label
    req = {"game_id": game_id, "log_id": str(log_id), "ts_epoch": t_epoch,
           "label": label or recorder.status().get("label"),
           "pre": cmd.get("pre"), "post": cmd.get("post"),
           "made": cmd.get("made"), "verdict": cmd.get("verdict")}
    annotate(f"highlight queued: {log_id}", ["highlight", "queued", str(log_id)])
    threading.Thread(target=cut_highlight, args=(FB, CFG, recorder, req),
                     name=f"highlight-{str(log_id)[:8]}", daemon=True).start()
    # Env-gated shadow shot-detection validation (SHOT_VALIDATION_ENABLED, default
    # OFF). Best-effort on its own daemon thread — never affects the clip above.
    try:
        from agx_pipeline.shot_detect.node import validate_async
        validate_async(FB, CFG, cmd, game_id, label, sst)
    except Exception:  # noqa: BLE001
        pass
    return {"success": True, "queued": True, "logId": str(log_id),
            "side": side, "angle": recorder.angle()}, 202


@app.route("/api/highlight", methods=["POST"])
def highlight_clip():
    payload, status = _do_highlight(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/preview", methods=["POST", "GET"])
def preview_cameras():
    payload, status = _do_preview()
    return jsonify(payload), status


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


# One ingestion at a time: with the night-end gate, several games' ingests all
# queue and would otherwise stampede the encoder together when the gate opens.
_ingest_serial = threading.Semaphore(1)


def _wait_for_night_end(pipeline_id: str) -> None:
    """Block until recording has been idle INGEST_DEFER_MIN consecutive minutes.

    2026-08-19: ingestion transcodes running DURING later games starved game-3's
    capture (frozen frames, broken DTS) and failed 27 clip cuts. A between-games
    gap is 10-15 min, so 30 quiet minutes == the night is over. Set
    INGEST_DEFER_MIN=0 to restore immediate ingestion."""
    defer_min = float(os.getenv("INGEST_DEFER_MIN", "30"))
    if defer_min <= 0:
        return
    quiet_since = time.time()
    last_log = 0.0
    while True:
        with _lock:
            rec = bool(_current)
        now = time.time()
        if rec:
            quiet_since = now
        elif now - quiet_since >= defer_min * 60:
            logger.info("ingestion %s: night-end gate open", pipeline_id)
            return
        if now - last_log > 600:
            logger.info("ingestion %s deferred (recording=%s, quiet %.0fm/%.0fm)",
                        pipeline_id, rec, (now - quiet_since) / 60, defer_min)
            last_log = now


def _retry_failed_highlights(state: Dict) -> None:
    """Re-cut this game's error-status highlight clips now that the encoder is
    idle (they failed mid-game under load). Best-effort; runs BEFORE ingestion
    so the buffer segments are still on disk."""
    game_id = state.get("firebase_game_id")
    if not (FB and game_id):
        return
    try:
        doc = FB.db.collection("basketball-games").document(game_id).get()
        d = doc.to_dict() or {}
        errors = [k for k, v in (d.get("highlights") or {}).items()
                  if isinstance(v, dict) and v.get("status") == "error"]
        if not errors:
            return
        logs_by_id = {l.get("id"): l for l in (d.get("logs") or []) if l.get("id")}
        logger.info("retrying %d failed highlight cuts for %s", len(errors), game_id)
        for k in errors:
            if k.startswith("cv_"):
                parts = k.split("_")
                epoch, side = int(parts[1]), parts[2]
                ts = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
                cmd = {"firebase_game_id": game_id, "logId": k, "ts": ts,
                       "side": side, "label": state.get("label")}
            else:
                log = logs_by_id.get(k)
                if not log:
                    continue
                cmd = {"firebase_game_id": game_id, "logId": k,
                       "ts": log.get("timestamp"), "team": log.get("team"),
                       "period": log.get("period"), "pre": 6.5, "post": -1.5,
                       "label": state.get("label")}
            try:
                _do_highlight(cmd)
                time.sleep(20)   # serialized-ish: one cut lands before the next
            except Exception as e:  # noqa: BLE001
                logger.warning("highlight retry failed for %s: %s", k, e)
    except Exception as e:  # noqa: BLE001
        logger.warning("highlight retry sweep failed for %s: %s", game_id, e)


def _start_ingestion(pipeline_id: str, state: Dict, stopped: Dict) -> None:
    """Run P1 ingestion (transcode → upload → register), driving the ingestion-runs doc."""
    from agx_pipeline.ingest import run_ingestion
    global _active_ingests
    _wait_for_night_end(pipeline_id)
    with _ingest_serial:
        _retry_failed_highlights(state)
        with _lock:
            _active_ingests += 1                  # gate the shot-QA worker off the GPU
        try:
            run_ingestion(FB, CFG, pipeline_id, state, stopped, TRACKER)
        except Exception as e:  # noqa: BLE001
            logger.error("ingestion %s crashed: %s", pipeline_id, e)
        finally:
            with _lock:
                _active_ingests = max(0, _active_ingests - 1)


def _finalize_for_shutdown(signum, _frame) -> None:
    """SIGTERM/SIGINT: finalize the in-flight recording before we die.

    `systemctl restart` sends SIGTERM to the WHOLE cgroup (KillMode=control-group
    is systemd's default), so every child ffmpeg gets SIGTERM too — and ffmpeg on
    SIGTERM exits WITHOUT writing the moov atom. That is why this codebase signals
    its own recorders with SIGINT everywhere (_ff_finalize, _teardown_locked,
    `--signal=INT`): SIGINT finalizes, SIGTERM truncates.

    Cost of not doing this, measured: on 2026-08-21 a restart 7 minutes into a
    57-minute game left all five angles headerless ("moov atom not found") and
    recording never resumed — ~50 minutes of that game never existed. The files
    were recoverable only by rebuilding the index by hand.

    So: SIGINT the recorders and let them write their headers. Ingestion is NOT
    started — this process is going away and would take it with us; the finalized
    masters stay on disk, and auto-follow resumes recording on the way back up.

    Pair this with `KillMode=mixed` + a generous `TimeoutStopSec` in the unit
    (deploy/agx-ingestion.service.d/), or systemd will SIGTERM the children out
    from under us before this runs."""
    logger.warning("signal %d — finalizing recording before shutdown", signum)
    with _lock:
        state = dict(_current)
        _current.clear()
    if state:
        outs = state.get("outputs") or []
        track = [o for o in outs if o.get("role") != "shot_detection"]
        shot = [o for o in outs if o.get("role") == "shot_detection"]
        # Concurrent, same as the normal stop, so both families' SIGINT lands at
        # ~the same instant and the angles keep matching end times.
        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(CONTROLLER.stop, state["label"], track)]
            if SHOT and shot:
                futs.append(ex.submit(SHOT.stop, state["label"], shot))
            for f in futs:
                try:
                    f.result()
                except Exception as e:  # noqa: BLE001
                    logger.error("shutdown finalize failed: %s", e)
        logger.warning("recording %s finalized on shutdown — masters kept on disk, "
                       "ingestion NOT started", state.get("label"))
    for name, obj in (("highlight", HIGHLIGHT), ("live", LIVE)):
        try:
            if obj:
                obj.stop()
        except Exception as e:  # noqa: BLE001
            logger.warning("%s stop failed on shutdown: %s", name, e)
    os._exit(0)


if __name__ == "__main__":
    import signal as _signal

    for _sig in (_signal.SIGTERM, _signal.SIGINT):
        _signal.signal(_sig, _finalize_for_shutdown)
    port = int(os.getenv("AGX_SERVICE_PORT", "5000"))
    if FB:
        from agx_pipeline.relay import Relay
        _RELAY = Relay(FB, CFG.jetson_id, _device_state, _do_start, _do_stop,
                       auto_fn=_auto_follow, on_preview=_do_preview,
                       on_highlight=_do_highlight)
        _RELAY.start()
        # Publish the Courtside schedule + roster to Firestore. The frontend's
        # cloud IP is WAF/geo-blocked by the league site; the AGX (at the venue,
        # allowed IP) can fetch it, so it scrapes and the frontend reads Firestore.
        if os.getenv("COURTSIDE_SCRAPE", "true").lower() in ("1", "true", "yes"):
            from agx_pipeline.courtside import start_refresh
            start_refresh(FB, int(os.getenv("COURTSIDE_REFRESH_MIN", "30")))
        # Deferred shot-detection QA worker: drains the shot-qa-queue only while
        # is_gpu_free() (nothing recording/ingesting). Dormant unless SHOT_QA_ENABLED.
        from agx_pipeline.shot_detect.qa import start_worker as _start_qa_worker
        _start_qa_worker(FB, CFG, is_gpu_free)
    logger.info("AGX service starting on :%d (firebase=%s, cameras=%d, relay=%s, auto_record=%s)",
                port, FB is not None, len(CFG.cameras), _RELAY is not None, AUTO_RECORD)
    app.run(host="0.0.0.0", port=port)
