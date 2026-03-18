"""
Z-CAM configuration — single source of truth.

All Z-CAM-specific values are loaded from environment variables.
No hardcoded IPs, bucket names, or stream names anywhere else.
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger('zcam.config')


@dataclass
class ZCamCamera:
    """Configuration for a single Z-CAM E2-N camera."""
    cam_id: str            # e.g., "zcam-1"
    ip: str                # e.g., "192.168.1.101"
    ndi_name: str          # e.g., "ZCAM-E2-N (cam1)"
    kvs_stream_name: str   # e.g., "zcam-court-left"
    label: str             # human-friendly, e.g., "Court Left"


@dataclass
class ZCamConfig:
    """Full Z-CAM pipeline configuration."""
    cameras: List[ZCamCamera] = field(default_factory=list)

    # Hardware platform: "jetson" or "orangepi" (auto-detected if not set)
    platform: str = "auto"

    # GStreamer encoding
    bitrate: int = 4_000_000        # bits/sec for NVENC (Jetson)
    sw_bitrate: int = 4000          # kbps for x264enc (Orange Pi)
    iframe_interval: int = 30

    # AWS KVS
    kvs_region: str = "us-east-1"
    kvs_fragment_duration: int = 1000  # ms

    # AWS S3 (4K uploads)
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    s3_prefix: str = "zcam-recordings"

    # Camera watcher
    poll_interval_sec: float = 5.0

    # 4K file staging
    download_dir: str = ""
    chunk_size: int = 262144  # 256KB, matches existing DOWNLOAD_CHUNK_SIZE

    # MJPEG
    mjpeg_quality: int = 75
    mjpeg_max_fps: int = 15


def load_zcam_config() -> ZCamConfig:
    """Load Z-CAM config from environment variables."""
    # Parse ZCAM_CAMERAS JSON
    cameras_json = os.getenv('ZCAM_CAMERAS', '[]')
    try:
        cameras_raw = json.loads(cameras_json)
        cameras = [ZCamCamera(**c) for c in cameras_raw]
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Failed to parse ZCAM_CAMERAS: {e}")
        cameras = []

    # Detect platform
    platform = os.getenv('ZCAM_PLATFORM', 'auto')
    if platform == 'auto':
        platform = _detect_platform()

    config = ZCamConfig(
        cameras=cameras,
        platform=platform,
        bitrate=int(os.getenv('ZCAM_BITRATE', '4000000')),
        sw_bitrate=int(os.getenv('ZCAM_SW_BITRATE', '4000')),
        iframe_interval=int(os.getenv('ZCAM_IFRAME_INTERVAL', '30')),
        kvs_region=os.getenv('ZCAM_KVS_REGION', os.getenv('UPLOAD_REGION', 'us-east-1')),
        s3_bucket=os.getenv('ZCAM_S3_BUCKET', os.getenv('UPLOAD_BUCKET', 'jetson-videos-uai')),
        s3_region=os.getenv('ZCAM_S3_REGION', os.getenv('UPLOAD_REGION', 'us-east-1')),
        s3_prefix=os.getenv('ZCAM_S3_PREFIX', 'zcam-recordings'),
        poll_interval_sec=float(os.getenv('ZCAM_POLL_INTERVAL', '5')),
        download_dir=os.getenv('ZCAM_DOWNLOAD_DIR', os.path.expanduser('~/zcam_downloads')),
        mjpeg_quality=int(os.getenv('ZCAM_MJPEG_QUALITY', '75')),
    )

    if cameras:
        logger.info(f"Loaded {len(cameras)} Z-CAM cameras (platform={config.platform})")
    else:
        logger.warning("No Z-CAM cameras configured (set ZCAM_CAMERAS env var)")

    return config


def _detect_platform() -> str:
    """Detect Jetson vs Orange Pi by checking for NVENC hardware."""
    if os.path.exists('/dev/nvhost-nvenc'):
        return 'jetson'
    # Check for Jetson via tegra chip info
    try:
        if os.path.exists('/proc/device-tree/model'):
            with open('/proc/device-tree/model', 'r') as f:
                model = f.read().lower()
                if 'jetson' in model or 'tegra' in model:
                    return 'jetson'
    except Exception:
        pass
    return 'orangepi'
