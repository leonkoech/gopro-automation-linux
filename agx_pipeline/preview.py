"""On-demand camera snapshots for the dashboard "Preview all cameras" grid.

Grabs ONE JPEG frame from every camera (Zowietek via RTSP/ffmpeg, FLIR via
Aravis/gstreamer), uploads them to S3, and returns presigned URLs the dashboard
renders as a CCTV-style grid with the angle labeled under each tile.

Best-effort and parallel: a camera that can't be grabbed comes back with
`up=False, url=None` so the grid can show a placeholder instead of failing.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import boto3

logger = logging.getLogger("agx.preview")

S3_BUCKET = os.getenv("S3_BUCKET", "uball-videos-production")
PREVIEW_PREFIX = os.getenv("PREVIEW_S3_PREFIX", "camera-previews")
PREVIEW_TTL = int(os.getenv("PREVIEW_URL_TTL", "3600"))  # 1h presigned link
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def _rtsp_snapshot(ip: str, port: int, path: str, out: str, timeout: int = 12) -> bool:
    """One JPEG frame from a Zowietek RTSP stream, scaled to 640px wide."""
    url = f"rtsp://{ip}:{port}{path}"
    cmd = ["ffmpeg", "-nostdin", "-y", "-rtsp_transport", "tcp", "-i", url,
           "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "4", out]
    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except Exception as e:  # noqa: BLE001
        logger.warning("rtsp snapshot %s failed: %s", ip, e)
    return os.path.exists(out) and os.path.getsize(out) > 0


def _aravis_snapshot(camera_name: str, out: str, timeout: int = 15) -> bool:
    """One JPEG frame from a FLIR Aravis (GigE Vision) camera."""
    cmd = ["gst-launch-1.0", "aravissrc", f"camera-name={camera_name}", "num-buffers=1",
           "!", "videoconvert", "!", "jpegenc", "!", "filesink", f"location={out}"]
    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except Exception as e:  # noqa: BLE001
        logger.warning("aravis snapshot %s failed: %s", camera_name, e)
    return os.path.exists(out) and os.path.getsize(out) > 0


def capture_all(cfg) -> List[Dict]:
    """Snapshot every camera in parallel -> S3 -> presigned URL. Returns
    [{angle, role, basket_side, up, url}] ordered tracking-first then shot."""
    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    s3 = boto3.client("s3", region_name=AWS_REGION)
    cams = [(c, False) for c in cfg.cameras] + [(c, True) for c in cfg.shot_cameras]
    results: List[Optional[Dict]] = [None] * len(cams)

    with tempfile.TemporaryDirectory() as tmp:
        def work(i: int, cam, is_shot: bool) -> None:
            out = os.path.join(tmp, f"{cam.angle}.jpg")
            ok = (_aravis_snapshot(cam.camera_name or cam.ip, out) if is_shot
                  else _rtsp_snapshot(cam.ip, cfg.rtsp_port, cfg.rtsp_path, out))
            entry = {"angle": cam.angle,
                     "role": "shot_detection" if is_shot else "tracking",
                     "basket_side": cam.basket_side or None,
                     "up": ok, "url": None}
            if ok:
                key = f"{PREVIEW_PREFIX}/{cfg.jetson_id}/{ts}/{cam.angle}.jpg"
                try:
                    s3.upload_file(out, S3_BUCKET, key, ExtraArgs={"ContentType": "image/jpeg"})
                    entry["url"] = s3.generate_presigned_url(
                        "get_object", Params={"Bucket": S3_BUCKET, "Key": key},
                        ExpiresIn=PREVIEW_TTL)
                except Exception as e:  # noqa: BLE001
                    logger.error("preview upload %s failed: %s", cam.angle, e)
                    entry["up"] = False
            results[i] = entry

        with ThreadPoolExecutor(max_workers=max(1, len(cams))) as ex:
            for i, (cam, is_shot) in enumerate(cams):
                ex.submit(work, i, cam, is_shot)

    out_list = [r for r in results if r]
    logger.info("preview: %d/%d cameras captured", sum(1 for r in out_list if r["up"]), len(out_list))
    return out_list
