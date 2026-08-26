"""
P1 ingestion.

Per stopped recording: transcode ALL angles -> 1080p (software libx264), upload
ONLY the 1080p to S3 (no 4K upload — that's the slow step we skip), register the
game + FL/FR videos in the annotation tool (reusing the check-in game's
leftTeamId/rightTeamId), then delete the 4K masters from the AGX
(DELETE_RAW_AFTER_TRANSCODE) to keep the box fast and free.

NL/NR are transcoded + uploaded (1080p) too, but not registered yet — the
annotation player is 2-angle today; they're ready for the 4-angle rollout.

Shot-detection footage (FLIR near-rim, role="shot_detection", angles SL/SR) is
handled separately: it is already H.264 at native (small) resolution, so it is
uploaded AS-IS to the SAME game folder (no downscale/transcode, no annotation
register), joining the game by uuid for the shot-detection CV. Set
SHOTDET_UPLOAD_S3=false to keep it on the AGX for local processing instead.

4K-masters toggle: the dashboard writes
`agx-settings/{jetson_id}.transcode = false` and ingestion runs the NORMAL
pipeline (1080p proxies, annotation game + register — nothing about the usual
flow changes) and ALSO uploads the raw 4K masters (`_4K` filename suffix) into
the same game folder, logging each one's full s3://bucket/key to the run log
(shown on the dashboard's ingestion card). Use case: marketing clips get cut
from the 4K masters later, while the game still flows to annotation as usual.
The raw master is only deleted from the AGX once BOTH its uploads are
confirmed. Default is normal-only; TRANSCODE_ENABLED env is the no-Firebase
fallback (field is named `transcode` for compatibility: false = keep 4K).
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import subprocess
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from uball_client import get_uball_client
from email_notifier import GameNotification, send_games_ready_email
from agx_pipeline.ingestion_status import IngestionRun

logger = logging.getLogger("agx.ingest")

BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
REGION = os.getenv("UPLOAD_REGION", "us-east-1")
LOCATION = os.getenv("COURT_LOCATION", "court-a")
COURT_TZ = os.getenv("COURT_TZ", "America/New_York")
DELETE_RAW = os.getenv("DELETE_RAW_AFTER_TRANSCODE", "true").lower() in ("1", "true", "yes")
# Shot-detection (FLIR) footage: upload as-is to S3 now (to build the CV
# pipeline against real footage). Flip to false later to process it locally on
# the AGX only — one env change, no code change.
SHOTDET_UPLOAD_S3 = os.getenv("SHOTDET_UPLOAD_S3", "true").lower() in ("1", "true", "yes")
# Auto-seed annotation cards from the CV's shot_live detections (one card per shot
# the SL/SR detector saw), so a game is carded even when the scorekeeper didn't
# score. Off by default — it seeds ~150-200 review-flagged (source="cv") cards per
# game into the annotators' workflow, so it's an explicit opt-in.
SHOT_CARDS_ENABLED = os.getenv("SHOT_CARDS_ENABLED", "false").lower() in ("1", "true", "yes")
CRF = os.getenv("TRANSCODE_CRF", "23")
PRESET = os.getenv("TRANSCODE_PRESET", "veryfast")
HW_BITRATE = os.getenv("TRANSCODE_HW_BITRATE", "8000000")  # NVENC bits/sec for 1080p
MAX_PARALLEL = int(os.getenv("TRANSCODE_PARALLEL", "2"))
UBALL_ANGLE = {"FL": "LEFT", "FR": "RIGHT"}  # registered angles (annotation is 2-angle today)
SETTINGS_COLLECTION = "agx-settings"
TRANSCODE_DEFAULT = os.getenv("TRANSCODE_ENABLED", "true").lower() in ("1", "true", "yes")

# GPU-transcode activity gate. `_active_ingests` (in service.py) is True for the
# WHOLE ~30min ingest lifecycle (transcode + the long S3 upload + register); the
# live shot loop only needs to yield during the ACTUAL GPU transcode (~7-15min),
# so it can keep detecting through a prior game's upload window. This counter is
# raised only around the transcode stage and read via is_transcoding().
_transcode_lock = threading.Lock()
_transcoding = 0


def is_transcoding() -> bool:
    """True while any ingestion is in its GPU transcode stage (NOT upload/register)."""
    with _transcode_lock:
        return _transcoding > 0


def _transcode_begin() -> None:
    global _transcoding
    with _transcode_lock:
        _transcoding += 1


def _transcode_end() -> None:
    global _transcoding
    with _transcode_lock:
        _transcoding = max(0, _transcoding - 1)


def _transcode_enabled(fb, jetson_id: str) -> bool:
    """Operator toggle: `agx-settings/{jetson_id}.transcode` (dashboard-writable).

    True (default) = normal ingest only. False = 4K mode: the normal pipeline
    runs unchanged AND the raw 4K masters are also uploaded. Read fresh at each
    ingestion, so flipping the toggle while a recording is still running
    applies at stop. Any read failure falls back to TRANSCODE_ENABLED — a
    Firebase blip must never silently change what gets uploaded.
    """
    if not fb:
        return TRANSCODE_DEFAULT
    try:
        snap = fb.db.collection(SETTINGS_COLLECTION).document(jetson_id).get()
        val = (snap.to_dict() or {}).get("transcode") if snap.exists else None
        return TRANSCODE_DEFAULT if val is None else bool(val)
    except Exception:  # noqa: BLE001
        logger.warning("agx-settings read failed — using default transcode=%s", TRANSCODE_DEFAULT)
        return TRANSCODE_DEFAULT


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _local_date(label: str) -> str:
    """label 'game_YYYYMMDD_HHMMSS' (UTC) -> court-local date YYYY-MM-DD."""
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.strptime(label.replace("game_", ""), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo(COURT_TZ)).strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _s3_key(date: str, folder: str, angle: str) -> tuple:
    fn = f"{date}_{folder}_{angle}.mp4"
    return f"{LOCATION}/{date}/{folder}/{fn}", fn


def _to_container_path(host_path: str, app_mount: str) -> str:
    return os.path.join("/app/data", os.path.relpath(host_path, app_mount))


def _transcode_hw(src: str, dst: str, cfg) -> bool:
    """GStreamer hardware transcode in the NDI container: NVDEC → scale 1080p → NVENC H.264.
    ~4x real-time on the AGX and runs on the GPU, so it doesn't fight CPU work."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    cin, cout = _to_container_path(src, cfg.app_mount), _to_container_path(dst, cfg.app_mount)
    cmd = list(cfg.docker_cmd) + [
        "run", "--rm", "--privileged", "--runtime", "nvidia", "--net=host",
        "-v", f"{cfg.app_mount}:/app/data", "--workdir", "/app/data", cfg.docker_image,
        "gst-launch-1.0", "-e",
        "filesrc", f"location={cin}", "!", "qtdemux", "!", "h265parse", "!",
        "nvv4l2decoder", "!", "nvvideoconvert", "!",
        "video/x-raw(memory:NVMM),width=1920,height=1080", "!",
        # idrinterval matters, not just iframeinterval: browsers/ffmpeg can only
        # seek to IDR frames, and nvv4l2h264enc defaults idrinterval to 256
        # (~8.5s at 30fps) — the cause of multi-second seek stalls in the
        # annotation editor. IDR every 30 frames = seekable every second.
        "nvv4l2h264enc", f"bitrate={HW_BITRATE}", "iframeinterval=30", "idrinterval=30", "!",
        "h264parse", "!", "mp4mux", "!", "filesink", f"location={cout}",
    ]
    cp = subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=10800)
    if cp.returncode != 0:
        logger.warning("HW transcode failed %s: %s", src, cp.stderr.strip()[-200:])
        return False
    return os.path.isfile(dst) and os.path.getsize(dst) > 0


def _transcode_sw(src: str, dst: str) -> bool:
    """Software libx264 fallback (CPU-heavy; correct but saturates the box)."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    # -g 30 -sc_threshold 0: keyframe every second (libx264 defaults to 250,
    # ~8.3s — unseekable in the annotation editor). Matches the HW path's
    # iframeinterval/idrinterval=30.
    cmd = ["ffmpeg", "-nostdin", "-y", "-i", src, "-vf", "scale=-2:1080",
           "-c:v", "libx264", "-preset", PRESET, "-crf", CRF,
           "-g", "30", "-sc_threshold", "0",
           "-movflags", "+faststart", "-an", dst]
    cp = subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=10800)
    if cp.returncode != 0:
        logger.error("SW transcode failed %s: %s", src, cp.stderr.strip()[-300:])
        return False
    return os.path.isfile(dst) and os.path.getsize(dst) > 0


def _transcode_1080p(src: str, dst: str, cfg) -> bool:
    """Hardware transcode by default, software fallback. TRANSCODE_MODE=hw|sw|auto."""
    mode = os.getenv("TRANSCODE_MODE", "auto")
    if mode in ("hw", "auto"):
        if _transcode_hw(src, dst, cfg):
            return True
        if mode == "hw":
            return False
        logger.warning("falling back to software transcode: %s", src)
    return _transcode_sw(src, dst)


def _probe_dur(path: str) -> Optional[float]:
    cp = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                         "-of", "default=nokey=1:noprint_wrappers=1", path],
                        capture_output=True, text=True, stdin=subprocess.DEVNULL)
    try:
        return float(cp.stdout.strip())
    except ValueError:
        return None


def _upload(local: str, key: str, content_type: str = "video/mp4") -> None:
    import boto3
    from boto3.s3.transfer import TransferConfig
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(local, BUCKET, key, ExtraArgs={"ContentType": content_type},
                   Config=TransferConfig(multipart_threshold=64 * 1024 * 1024,
                                         multipart_chunksize=64 * 1024 * 1024, max_concurrency=4))


def _ingest_audio_sync(run, date: str, folder: str, audio_list) -> None:
    """Cross-correlate FL & FR court audio into a per-game frame offset, archive
    the small audio side-files next to the game's videos in S3, and record the
    result on the ingestion-runs doc. Best-effort: any failure only logs."""
    paths = {a["angle"]: a["path"] for a in (audio_list or [])
             if a.get("path") and os.path.isfile(a["path"])}
    if not paths:
        return
    for ang, path in paths.items():  # archive the (small) audio alongside the game
        try:
            key = _s3_key(date, folder, ang)[0][:-4] + ".m4a"
            _upload(path, key, content_type="audio/mp4")
        except Exception as e:  # noqa: BLE001
            run.log("warn", f"audio {ang}: upload failed ({str(e)[:120]})")
    if not ({"FL", "FR"} <= paths.keys()):
        run.log("info", f"audio sync skipped: need FL+FR, have {sorted(paths)}")
        return
    try:
        from agx_pipeline import audio_sync  # lazy: keep numpy/scipy off the hot path
        res = audio_sync.measure_offset(paths["FL"], paths["FR"], fps=30.0)
        run.set_audio_sync(res)
        if res.get("ok"):
            run.log("info",
                    f"audio sync FL<->FR: {res['offset_frames']:+.2f} frames "
                    f"({res['offset_sec']:+.3f}s), drift {res.get('drift_frames')}f, "
                    f"{res['n_confident']} confident windows")
        else:
            run.log("warn", f"audio sync inconclusive: {res.get('reason')}")
    except Exception as e:  # noqa: BLE001
        run.log("warn", f"audio sync error: {str(e)[:150]}")


def _resolve_checkin_roster(fb, game: Dict) -> "tuple[Optional[list], Optional[list]]":
    """Pick the roster to register on the annotation game (team1, team2).

    Prefer the LIVE check-in slot ``game_schedules/<scheduleSlotId>`` filtered to
    checked-in players, so attendance/roster edits the operator makes on the
    check-in page reach the annotation tool. The per-game ``basketball-games``
    ``rosterTeamN`` is only a snapshot frozen at "Start Game" time, so edits made
    after that never propagate through it. Fall back to that snapshot when the
    slot is missing/unreadable or has no checked-in players.

    Mirrors the proven logic in ``main.py`` ``/api/games/sync`` (:1953-1984) so
    the AGX auto-ingestion no longer creates games with an empty roster. This is
    best-effort: any failure reading the slot falls back to the snapshot and
    never breaks ingestion.
    """
    roster1 = game.get("rosterTeam1")
    roster2 = game.get("rosterTeam2")
    slot_id = game.get("scheduleSlotId")
    if not (fb and getattr(fb, "db", None) and slot_id):
        return roster1, roster2
    try:
        slot_doc = fb.db.collection("game_schedules").document(slot_id).get()
        if not slot_doc.exists:
            logger.info("ingest roster: slot %s not found — using snapshot", slot_id)
            return roster1, roster2
        slot = slot_doc.to_dict() or {}
        live1 = [p for p in (slot.get("rosterTeam1") or []) if p.get("checked_in")]
        live2 = [p for p in (slot.get("rosterTeam2") or []) if p.get("checked_in")]
        if live1 or live2:
            logger.info("ingest roster: using checked-in slot roster %s (%d/%d players)",
                        slot_id, len(live1), len(live2))
            return (live1 or roster1), (live2 or roster2)
        logger.info("ingest roster: slot %s has no checked-in players — using snapshot", slot_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("ingest roster: slot %s fetch failed (%s) — using snapshot", slot_id, e)
    return roster1, roster2


def _create_or_get_game(client, fb, game: Dict, firebase_game_id: str, date: str) -> Optional[Dict]:
    existing = client.get_game_by_firebase_id(firebase_game_id) if firebase_game_id else None
    # Reuse ONLY on an exact firebase-id match — a fuzzy/arbitrary match here
    # attaches this recording's uploads to another game's S3 keys (the
    # 2026-08-13 manual-recording overwrite). No firebase game => always a NEW
    # annotation game.
    if existing and existing.get("firebase_game_id") == firebase_game_id:
        return existing
    left, right = game.get("leftTeam", {}) or {}, game.get("rightTeam", {}) or {}
    roster1, roster2 = _resolve_checkin_roster(fb, game)
    payload = {
        "date": date,
        "team1_id": game.get("leftTeamId"),   # reuse the check-in game's annotation team UUIDs
        "team2_id": game.get("rightTeamId"),
        "team1_color": left.get("jerseyColorName"),
        "team2_color": right.get("jerseyColorName"),
        "team1_display_name": left.get("displayName") or left.get("name"),
        "team2_display_name": right.get("displayName") or right.get("name"),
        "video_name": f"{left.get('name', 'Team 1')} vs {right.get('name', 'Team 2')}",
        "firebase_game_id": firebase_game_id,
        "source": "agx",
        # AGX games are annotated on the synced two-angle editor: plays are marked
        # in base/master-angle time, and each angle carries a manual sync offset to
        # align it. The clip pipeline (Sync-to-UBall) only ADDS those per-angle
        # offsets when this flag is true — otherwise it cuts at raw timestamps and
        # the annotator's manual sync is silently ignored. AGX games therefore MUST
        # be born with the new convention; the annotation tool only sets it for
        # games created through its own UI, not for ones we create here.
        "timestamps_in_base_coords": True,
        "team1_score": left.get("finalScore"),
        "team2_score": right.get("finalScore"),
    }
    # Only send rosters when we actually have them, so an empty list never
    # overwrites anything and matches the /api/games/sync payload shape.
    if roster1:
        payload["roster_team1"] = roster1
    if roster2:
        payload["roster_team2"] = roster2
    return client.create_game(payload)


def _notify_annotators_ready(cfg, date: str, game: Dict, game_uuid: Optional[str]) -> None:
    """Email the annotators that a game is ready to annotate (both angles up).

    Best-effort and self-contained: SMTP creds + recipients come from env
    (SMTP_*, ANNOTATOR_NOTIFY_EMAIL/CC); incomplete config disables it silently.
    Never raises — a mail failure must not fail the ingestion run."""
    try:
        left = game.get("leftTeam", {}) or {}
        right = game.get("rightTeam", {}) or {}
        try:
            game_number = int(game.get("gameNumber") or game.get("game_number") or 0)
        except (TypeError, ValueError):
            game_number = 0
        notif = GameNotification(
            game_number=game_number,
            team_a_name=left.get("displayName") or left.get("name") or "Team 1",
            team_b_name=right.get("displayName") or right.get("name") or "Team 2",
            uball_game_id=game_uuid,
        )
        sent = send_games_ready_email(
            jetson_name=getattr(cfg, "jetson_id", "") or "",
            recording_date=date,
            ready_games=[notif],
            failed_games=[],
        )
        logger.info("annotator email notify: sent=%s game=%s", sent, game_uuid)
    except Exception as e:  # noqa: BLE001
        logger.warning("annotator email notify failed: %s", e)


def run_ingestion(fb, cfg, pipeline_id: str, state: Dict, stopped: Dict, tracker) -> None:
    """Transcode → upload 1080p → register → delete 4K, driving the ingestion-runs doc."""
    firebase_game_id = state["firebase_game_id"]
    label = state["label"]
    date = _local_date(label)
    ok_files = [f for f in stopped["files"] if f.get("ok")]
    # Split by CV role: tracking (Zowietek) goes through transcode→upload→register
    # below; shot-detection (FLIR) is uploaded as-is separately (STAGE 4).
    shot_files = [f for f in ok_files if f.get("role") == "shot_detection"]
    files = [f for f in ok_files if f.get("role") != "shot_detection"]
    angles = [f["angle"] for f in files]

    game = (fb.get_game(firebase_game_id) if fb else None) or {}
    left, right = game.get("leftTeam", {}) or {}, game.get("rightTeam", {}) or {}
    video_name = f"{left.get('name', 'Team 1')} vs {right.get('name', 'Team 2')}"

    keep_4k = not _transcode_enabled(fb, cfg.jetson_id)
    reg_angles = [a for a in angles if a in UBALL_ANGLE]
    run = IngestionRun(fb, pipeline_id, {
        "jetson_id": cfg.jetson_id, "firebase_game_id": firebase_game_id,
        "video_name": video_name, "date": date}, angles,
        register_angles=reg_angles)
    logger.info("ingest %s game=%s date=%s angles=%s delete_raw=%s keep_4k=%s",
                pipeline_id, firebase_game_id, date, angles, DELETE_RAW, keep_4k)

    try:
        # annotation game (needed for the S3 folder = uball game uuid)
        client = get_uball_client()
        uball_game = None
        if keep_4k:
            run.log("info", "4K mode (agx-settings) — normal pipeline runs as usual, "
                            "and the raw 4K masters will ALSO be uploaded (paths in this log)")
        if not client:
            run.log("warn", "UBALL creds not configured — will transcode+upload but not register")
        else:
            try:
                uball_game = _create_or_get_game(client, fb, game, firebase_game_id, date)
            except Exception as e:  # noqa: BLE001
                run.log("error", f"create annotation game: {e}")
        game_uuid = (uball_game or {}).get("id")
        run.set_uball_game(game_uuid)
        run.set_register_game(
            ok=bool(game_uuid),
            error=None if game_uuid else
            ("UBALL creds not configured" if not client else "annotation game not created"))
        folder = "-".join(game_uuid.split("-")[:4]) if game_uuid else f"agx-{label}"
        run.set_s3(BUCKET, f"{LOCATION}/{date}/{folder}/")
        work_dir = os.path.join(cfg.output_dir, label, "1080p")

        # STAGE 1 — transcode (parallel, bounded); mark each angle as it finishes.
        # _transcode_begin/end flag the GPU-busy window so the live shot loop yields
        # only for the transcode, not the whole (upload-heavy) ingest.
        run.start_stage("transcode")

        def _do(f: Dict) -> tuple:
            key, fn = _s3_key(date, folder, f["angle"])
            dst = os.path.join(work_dir, fn)
            ok = _transcode_1080p(f["path"], dst, cfg)
            return f["angle"], {"src": f["path"], "dst": dst, "filename": fn,
                                "key": key, "ok": ok,
                                "dur": _probe_dur(dst) if ok else None,
                                "size": os.path.getsize(dst) if ok and os.path.exists(dst) else 0,
                                "uploaded": False}
        tr: Dict[str, Dict] = {}
        _transcode_begin()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
                futs = [ex.submit(_do, f) for f in files]
                for fut in concurrent.futures.as_completed(futs):
                    angle, r = fut.result()
                    tr[angle] = r
                    run.angle_done("transcode", angle) if r["ok"] else \
                        run.angle_failed("transcode", angle, "ffmpeg failed")
        finally:
            _transcode_end()
        run.finish_stage("transcode")

        # STAGE 2 — upload 1080p to S3
        run.start_stage("upload")
        for angle, r in tr.items():
            if not r["ok"]:
                continue
            try:
                _upload(r["dst"], r["key"])
                r["uploaded"] = True
                run.set_upload(angle, r["key"], r.get("size"))
                # full copy-pasteable path in the run log — this is how the
                # operator finds the footage (especially 4K passthrough, which
                # never touches the annotation tool)
                mb = f" ({r['size'] / 1e6:.0f} MB)" if r.get("size") else ""
                run.log("info", f"{angle} uploaded -> s3://{BUCKET}/{r['key']}{mb}")
                run.angle_done("upload", angle)
            except Exception as e:  # noqa: BLE001
                run.angle_failed("upload", angle, str(e)[:200])
        run.finish_stage("upload")

        # STAGE 3 — register FL/FR in the annotation tool
        run.start_stage("register")
        registered_angles = []
        for angle in reg_angles:
            r = tr.get(angle, {})
            if not r.get("uploaded"):
                run.angle_failed("register", angle, "not uploaded")
                continue
            if not (client and game_uuid):
                run.angle_failed("register", angle, "no annotation game (UBALL creds?)")
                continue
            try:
                client.register_video(game_id=game_uuid, s3_key=r["key"], angle=UBALL_ANGLE[angle],
                                      filename=r["filename"], duration=r["dur"], file_size=r["size"])
                run.angle_done("register", angle)
                registered_angles.append(angle)
            except Exception as e:  # noqa: BLE001
                run.angle_failed("register", angle, str(e)[:200])
        run.finish_stage("register")

        # The game is ready for annotation once EVERY registered angle (both FL +
        # FR today) is in the annotation tool — i.e. both camera angles are up in
        # the cloud. That is the moment to email the annotators. Best-effort:
        # recipients + SMTP come from env, missing config disables it silently,
        # and a mail failure never fails ingestion.
        if reg_angles and len(registered_angles) == len(reg_angles):
            run.log("info", "both angles registered — game ready; notifying annotators")
            _notify_annotators_ready(cfg, date, game, game_uuid)

        # Register Plays — turn the scoreboard's score log into annotation cards
        # (clip-ready, canonical labels, player pre-filled where the scorekeeper
        # tapped a player, else left for the annotator) so a game opens already
        # carded. Best-effort + idempotent: a failure never fails the ingestion
        # run, and the converter no-ops if the game already has plays.
        if client and game_uuid:
            try:
                from plays_sync import create_plays_from_firebase_logs
                summary: Dict = {}
                n_plays = create_plays_from_firebase_logs(client, game_uuid, game, summary=summary)
                run.set_register_plays(created=n_plays,
                                       with_players=summary.get("with_players", 0),
                                       by_label=summary.get("by_label"))
            except Exception as e:  # noqa: BLE001
                run.set_register_plays(0, 0, ok=False, error=str(e)[:200])
        else:
            run.set_register_plays(0, 0, ok=False,
                                   error="no annotation game (UBALL creds?)")

        # STAGE 3a — re-derive the shots from the SL/SR masters, now that the
        # game is over and nothing is racing the clock.
        #
        # The live detector works to a deadline and drops segments when it falls
        # behind, so it loses shots — one 2026-08-24 game recorded 4 live against
        # 196 real ones. It also reconstructs the shot time from a segment index,
        # which is what put clips minutes away from their shot. Neither applies
        # here: the master holds every frame, and `cross_frame / measured_fps` is
        # the position outright.
        #
        # Runs AFTER transcode (both want the GPU) and BEFORE the Core publish
        # and card creation below, which read straight from Firebase and so pick
        # up the rebuilt data without knowing anything changed.
        #
        # The live clips are left in S3 untouched — they served the green button
        # during the game. This only changes what Core and the cards READ.
        try:
            from agx_pipeline import shot_rebuild
            if shot_rebuild.enabled() and shot_files and fb:
                run.log("info", "shot-rebuild: re-detecting from SL/SR masters")
                rb = shot_rebuild.rebuild(fb, firebase_game_id, date,
                                          shot_files, tr, work_dir)
                run.log("info",
                        f"shot-rebuild: {rb['detected']} shots "
                        f"({rb['makes']} make), {rb['clips']} clips re-cut "
                        f"from {','.join(rb['angles']) or 'no shot cams'}")
        except Exception as e:  # noqa: BLE001 — never fail ingestion on this
            run.log("error", f"shot-rebuild failed: {str(e)[:200]}")

        # Publish Game Highlight to Core — a whole-game reel assembled from the
        # AGX highlight clips already cut + uploaded this game (references their
        # CloudFront URLs; no re-upload, no annotation / Sync-to-UBall
        # dependency). Lands in Core's admin publish queue behind the gate.
        # Best-effort + idempotent (Core upserts on firebase_game_id); re-fetch
        # the game first so clips that became ready late are included.
        try:
            from agx_pipeline.core_highlight import publish_core_highlight
            fresh = (fb.get_game(firebase_game_id) if fb else None) or game
            n_hl = publish_core_highlight(firebase_game_id, fresh, date)
            if n_hl:
                run.log("info", f"core game-highlight published: {n_hl} clips")
        except Exception as e:  # noqa: BLE001 — never fail ingestion on this
            run.log("error", f"core game-highlight publish failed: {str(e)[:200]}")

        # STAGE 3b — 4K mode: ALSO upload the raw masters into the same game
        # folder. Runs AFTER register so annotation availability is never
        # delayed by the big files. Marketing clips get cut from these later;
        # the s3:// path in the run log is how operators find them. A failed
        # 4K upload keeps the raw on the AGX (see cleanup) but does not fail
        # the run — the normal 1080p flow already succeeded.
        if keep_4k:
            for angle, r in tr.items():
                if not r.get("ok"):
                    continue
                src = r["src"]
                key, _fn = _s3_key(date, folder, angle + "_4K")
                try:
                    _upload(src, key)
                    r["uploaded_4k"] = True
                    size = os.path.getsize(src) if os.path.exists(src) else None
                    run.set_upload(f"{angle}_4K", key, size)
                    mb = f" ({size / 1e6:.0f} MB)" if size else ""
                    run.log("info", f"{angle} 4K master uploaded -> s3://{BUCKET}/{key}{mb}")
                except Exception as e:  # noqa: BLE001
                    run.log("error", f"{angle} 4K master upload failed — raw kept on "
                                     f"AGX for retry: {str(e)[:150]}")

        # STAGE 4 — shot-detection footage (FLIR SL/SR): upload as-is to the same
        # game folder, or keep it local. Runs before cleanup so kept-local files
        # are moved out of the session dir first (survive the rmtree below).
        if shot_files:
            _ingest_shot(run, cfg, shot_files, date, folder)
            # STAGE 4.5 — ENQUEUE the scorekeeper's scored makes for DEFERRED QA
            # (gated by SHOT_QA_ENABLED). Reads the timing sidecar (still local) +
            # writes a shot-qa-queue job; the GPU work runs later in the shot-qa
            # worker, only when nothing is recording/ingesting. No GPU here, and it
            # must NEVER block or break ingestion.
            try:
                from agx_pipeline.shot_detect.qa import enqueue as qa_enqueue
                qa_enqueue(fb, cfg, firebase_game_id, label,
                           f"{LOCATION}/{date}/{folder}", date, folder,
                           game.get("startingSideTeam1"), pipeline_id, run=run)
            except Exception as e:  # noqa: BLE001
                run.log("warn", f"shot-qa enqueue skipped: {str(e)[:150]}")

        # STAGE 4.6 — the live CV's OWN shot detections (shadow): (a) surface the
        # count on the card, and (b) SHOT_CARDS_ENABLED: seed one annotation card
        # per detected shot (source="cv", review-flagged) so a game is carded even
        # when nobody scored. Read the game doc fresh (the live loop finalized it at
        # game stop, before this ingest). Best-effort — never breaks ingestion.
        if fb and firebase_game_id:
            try:
                gd = fb.db.collection("basketball-games").document(firebase_game_id).get()
                fresh = (gd.to_dict() or {}) if gd.exists else {}
                live = fresh.get("shot_live") or {}
                if live.get("n_shots"):
                    run.set_shot_detection(
                        n_shots=int(live.get("n_shots", 0)), n_make=int(live.get("n_make", 0)),
                        n_miss=int(live.get("n_miss", 0)), n_sl=int(live.get("n_sl", 0)),
                        n_sr=int(live.get("n_sr", 0)), source="live")
                else:
                    run.set_shot_detection(0, 0, 0, 0, 0, source="live", status="none")
                if SHOT_CARDS_ENABLED and client and game_uuid and live.get("shots"):
                    from plays_sync import create_plays_from_shot_live
                    csum: Dict = {}
                    n_cv = create_plays_from_shot_live(client, game_uuid, fresh, summary=csum)
                    run.log("info", f"CV cards: {n_cv} created"
                            + (" (skipped — already carded)" if csum.get("skipped_existing") else ""))
            except Exception as e:  # noqa: BLE001
                run.log("warn", f"shot-detection cards/summary skipped: {str(e)[:120]}")

        # audio cross-correlation sync (FL<->FR) from the host-captured side-files;
        # runs before cleanup so the session-dir .m4a files still exist.
        _ingest_audio_sync(run, date, folder, stopped.get("audio"))

        # cleanup: 1080p is in S3; drop it + the raw master (env-controlled) to keep
        # the AGX free. CRITICAL: only delete the raw master once EVERY upload that
        # needs it is CONFIRMED — the 1080p (r["uploaded"]) and, in 4K mode, the 4K
        # master copy (r["uploaded_4k"]) — NOT merely once the transcode succeeded
        # (r["ok"]). A good transcode whose upload failed must never lose the only
        # copy. For the same reason, only rmtree the session dir when every raw is
        # safe; otherwise an un-uploaded raw would be destroyed along with it.
        def _raw_safe_to_delete(r: Dict) -> bool:
            return bool(r.get("uploaded") and (not keep_4k or r.get("uploaded_4k")))
        for r in tr.values():
            if r.get("uploaded"):
                _rm(r["dst"])
                if DELETE_RAW and _raw_safe_to_delete(r):
                    _rm(r["src"])
            else:
                run.log("error", f"{r['filename']}: S3 upload not confirmed — keeping raw "
                                 f"master on disk for retry ({r['src']})")
        if DELETE_RAW and all(_raw_safe_to_delete(r) for r in tr.values()):
            import shutil
            shutil.rmtree(os.path.join(cfg.output_dir, label), ignore_errors=True)

        if tracker:
            tracker.set_s3_prefix(state["session_ids"], f"{LOCATION}/{date}/{folder}/")
        run.complete()
        if fb:
            fb.complete_pipeline_run(pipeline_id, {
                "status": run.doc["status"], "stage": "completed",
                "stage_message": video_name, "progress": 100, "completed_at": _now()})
        logger.info("ingest %s done status=%s uball_game=%s", pipeline_id, run.doc["status"], game_uuid)
    except Exception as e:  # noqa: BLE001
        run.fail(str(e)[:300])
        if fb:
            fb.complete_pipeline_run(pipeline_id, {"status": "failed", "stage": "failed",
                                                   "stage_message": str(e)[:200], "progress": 100,
                                                   "completed_at": _now()})
        raise


def _ingest_shot(run, cfg, shot_files, date: str, folder: str) -> None:
    """Shot-detection footage (FLIR SL/SR) — deliberately NOT the tracking path.

    The clips are already H.264 at native (small) resolution, so there's no
    downscale/transcode and no annotation register. Default (SHOTDET_UPLOAD_S3):
    upload the file AS-IS to the SAME game folder in S3 under its SL/SR angle, so
    it joins the game by uuid for the shot-detection CV. Otherwise preserve it on
    the AGX under <output_dir>/shotdet_local/<folder>/ for local processing (and
    out of the session dir so the caller's cleanup rmtree can't delete it)."""
    for f in shot_files:
        angle = f["angle"]
        res = f"{f['width']}x{f['height']}" if f.get("width") and f.get("height") else None
        fps, side = f.get("fps"), f.get("basket_side")
        meta = f"{res}@{fps}fps"
        base = dict(fps=fps, resolution=res, basket_side=side)
        if SHOTDET_UPLOAD_S3:
            key, _fn = _s3_key(date, folder, angle)
            try:
                _upload(f["path"], key)
                run.set_shot(angle, "uploaded", s3_key=key, **base)
                run.log("info", f"shot {angle} ({meta}): uploaded as-is -> s3://{BUCKET}/{key}")
            except Exception as e:  # noqa: BLE001
                # Upload failed — the recorded clip still lives in the session dir,
                # which run_ingestion is about to rmtree. Preserve it locally so a
                # transient S3 error can't permanently lose the only copy.
                dst = _preserve_shot_local(cfg, f["path"], folder)
                if dst:
                    run.set_shot(angle, "kept_local", path=dst,
                                 error=f"upload failed: {str(e)[:150]}", **base)
                    run.log("error", f"shot {angle}: upload failed ({str(e)[:150]}); kept local -> {dst}")
                else:
                    run.set_shot(angle, "failed", error=str(e)[:200], **base)
                    run.log("error", f"shot {angle}: upload AND keep-local failed: {str(e)[:200]}")
        else:
            dst = _preserve_shot_local(cfg, f["path"], folder)
            if dst:
                run.set_shot(angle, "kept_local", path=dst, **base)
                run.log("info", f"shot {angle} ({meta}): kept local for shot detection -> {dst}")
            else:
                run.set_shot(angle, "failed", error="keep-local move failed", **base)
                run.log("error", f"shot {angle}: keep-local failed")


def _preserve_shot_local(cfg, src_path: str, folder: str) -> Optional[str]:
    """Move a shot clip OUT of the session dir (which run_ingestion rmtree's) into
    <output_dir>/shotdet_local/<folder>/ so it survives cleanup. Returns the new
    path, or None if the move failed."""
    keep_dir = os.path.join(cfg.output_dir, "shotdet_local", folder)
    try:
        import shutil
        os.makedirs(keep_dir, exist_ok=True)
        dst = os.path.join(keep_dir, os.path.basename(src_path))
        shutil.move(src_path, dst)
        return dst
    except OSError:
        return None


def _rm(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
