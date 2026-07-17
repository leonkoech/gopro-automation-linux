"""
P1 ingestion.

Per stopped recording: transcode ALL angles -> 1080p (software libx264), upload
ONLY the 1080p to S3 (no 4K upload — that's the slow step we skip), register the
game + FL/FR videos in the annotation tool (reusing the check-in game's
leftTeamId/rightTeamId), then delete the 4K masters from the AGX
(DELETE_RAW_AFTER_TRANSCODE) to keep the box fast and free.

NL/NR are transcoded + uploaded (1080p) too, but not registered yet — the
annotation player is 2-angle today; they're ready for the 4-angle rollout.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Dict, Optional

from uball_client import get_uball_client
from agx_pipeline.ingestion_status import IngestionRun

logger = logging.getLogger("agx.ingest")

BUCKET = os.getenv("UPLOAD_BUCKET", "uball-videos-production")
REGION = os.getenv("UPLOAD_REGION", "us-east-1")
LOCATION = os.getenv("COURT_LOCATION", "court-a")
COURT_TZ = os.getenv("COURT_TZ", "America/New_York")
DELETE_RAW = os.getenv("DELETE_RAW_AFTER_TRANSCODE", "true").lower() in ("1", "true", "yes")
CRF = os.getenv("TRANSCODE_CRF", "23")
PRESET = os.getenv("TRANSCODE_PRESET", "veryfast")
HW_BITRATE = os.getenv("TRANSCODE_HW_BITRATE", "8000000")  # NVENC bits/sec for 1080p
MAX_PARALLEL = int(os.getenv("TRANSCODE_PARALLEL", "2"))
UBALL_ANGLE = {"FL": "LEFT", "FR": "RIGHT"}  # registered angles (annotation is 2-angle today)


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
        "nvv4l2h264enc", f"bitrate={HW_BITRATE}", "iframeinterval=30", "!",
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
    cmd = ["ffmpeg", "-nostdin", "-y", "-i", src, "-vf", "scale=-2:1080",
           "-c:v", "libx264", "-preset", PRESET, "-crf", CRF,
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


def _upload(local: str, key: str) -> None:
    import boto3
    from boto3.s3.transfer import TransferConfig
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(local, BUCKET, key, ExtraArgs={"ContentType": "video/mp4"},
                   Config=TransferConfig(multipart_threshold=64 * 1024 * 1024,
                                         multipart_chunksize=64 * 1024 * 1024, max_concurrency=4))


def _create_or_get_game(client, game: Dict, firebase_game_id: str, date: str) -> Optional[Dict]:
    existing = client.get_game_by_firebase_id(firebase_game_id)
    if existing:
        return existing
    left, right = game.get("leftTeam", {}) or {}, game.get("rightTeam", {}) or {}
    return client.create_game({
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
        "team1_score": left.get("finalScore"),
        "team2_score": right.get("finalScore"),
    })


def run_ingestion(fb, cfg, pipeline_id: str, state: Dict, stopped: Dict, tracker) -> None:
    """Transcode → upload 1080p → register → delete 4K, driving the ingestion-runs doc."""
    firebase_game_id = state["firebase_game_id"]
    label = state["label"]
    date = _local_date(label)
    files = [f for f in stopped["files"] if f.get("ok")]
    angles = [f["angle"] for f in files]

    game = (fb.get_game(firebase_game_id) if fb else None) or {}
    left, right = game.get("leftTeam", {}) or {}, game.get("rightTeam", {}) or {}
    video_name = f"{left.get('name', 'Team 1')} vs {right.get('name', 'Team 2')}"

    run = IngestionRun(fb, pipeline_id, {
        "jetson_id": cfg.jetson_id, "firebase_game_id": firebase_game_id,
        "video_name": video_name, "date": date}, angles,
        register_angles=[a for a in angles if a in UBALL_ANGLE])
    logger.info("ingest %s game=%s date=%s angles=%s delete_raw=%s",
                pipeline_id, firebase_game_id, date, angles, DELETE_RAW)

    try:
        # annotation game (needed for the S3 folder = uball game uuid)
        client = get_uball_client()
        uball_game = None
        if not client:
            run.log("warn", "UBALL creds not configured — will transcode+upload but not register")
        else:
            try:
                uball_game = _create_or_get_game(client, game, firebase_game_id, date)
            except Exception as e:  # noqa: BLE001
                run.log("error", f"create annotation game: {e}")
        game_uuid = (uball_game or {}).get("id")
        run.set_uball_game(game_uuid)
        folder = "-".join(game_uuid.split("-")[:4]) if game_uuid else f"agx-{label}"
        work_dir = os.path.join(cfg.output_dir, label, "1080p")

        # STAGE 1 — transcode (parallel, bounded); mark each angle as it finishes
        run.start_stage("transcode")

        def _do(f: Dict) -> tuple:
            _, fn = _s3_key(date, folder, f["angle"])
            dst = os.path.join(work_dir, fn)
            ok = _transcode_1080p(f["path"], dst, cfg)
            return f["angle"], {"src": f["path"], "dst": dst, "filename": fn,
                                "key": _s3_key(date, folder, f["angle"])[0], "ok": ok,
                                "dur": _probe_dur(dst) if ok else None,
                                "size": os.path.getsize(dst) if ok and os.path.exists(dst) else 0,
                                "uploaded": False}
        tr: Dict[str, Dict] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
            futs = [ex.submit(_do, f) for f in files]
            for fut in concurrent.futures.as_completed(futs):
                angle, r = fut.result()
                tr[angle] = r
                run.angle_done("transcode", angle) if r["ok"] else \
                    run.angle_failed("transcode", angle, "ffmpeg failed")
        run.finish_stage("transcode")

        # STAGE 2 — upload 1080p to S3
        run.start_stage("upload")
        for angle, r in tr.items():
            if not r["ok"]:
                continue
            try:
                _upload(r["dst"], r["key"])
                r["uploaded"] = True
                run.angle_done("upload", angle)
            except Exception as e:  # noqa: BLE001
                run.angle_failed("upload", angle, str(e)[:200])
        run.finish_stage("upload")

        # STAGE 3 — register FL/FR in the annotation tool
        run.start_stage("register")
        for angle in [a for a in angles if a in UBALL_ANGLE]:
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
            except Exception as e:  # noqa: BLE001
                run.angle_failed("register", angle, str(e)[:200])
        run.finish_stage("register")

        # cleanup: 1080p is in S3; drop it + the 4K master (env-controlled) to keep the AGX free
        for r in tr.values():
            _rm(r["dst"])
            if DELETE_RAW and r["ok"]:
                _rm(r["src"])
        if DELETE_RAW:
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


def _rm(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
