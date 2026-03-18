"""
Z-CAM service initialization and lifecycle management.

Lazy-init singleton pattern: services are created on first access
and started/stopped via explicit API calls (not on import).

This is the Z-CAM equivalent of the module-level service initialization
in main.py (lines 33-89), but isolated to the blueprint.
"""

import logging
from typing import Optional, Dict

from .config import load_zcam_config, ZCamConfig
from .flask_viewer.stream import FrameBuffer

logger = logging.getLogger('zcam.services')

# Module-level singletons
_config: Optional[ZCamConfig] = None
_pipeline_manager = None  # PipelineManager (lazy import)
_frame_buffers: Dict[str, FrameBuffer] = {}
_poller = None  # ZCamApiPoller (lazy import)
_uploader = None  # ZCamUploader (lazy import)
_initialized = False
_services_running = False


def get_config() -> ZCamConfig:
    """Get or create the Z-CAM configuration."""
    global _config
    if _config is None:
        _config = load_zcam_config()
    return _config


def get_frame_buffers() -> Dict[str, FrameBuffer]:
    """Get frame buffers (one per camera). Creates them if needed."""
    _ensure_initialized()
    return _frame_buffers


def get_pipeline_manager():
    """Get the pipeline manager. Returns None if no cameras configured."""
    _ensure_initialized()
    return _pipeline_manager


def get_poller():
    """Get the API poller. Returns None if no cameras configured."""
    _ensure_initialized()
    return _poller


def get_uploader():
    """Get the uploader. Returns None if no cameras configured."""
    _ensure_initialized()
    return _uploader


def _ensure_initialized():
    """Create all service instances (but don't start them yet)."""
    global _initialized, _config, _pipeline_manager, _frame_buffers, _poller, _uploader

    if _initialized:
        return

    _config = get_config()

    if not _config.cameras:
        logger.warning("No Z-CAM cameras configured — services not initialized")
        _initialized = True
        return

    # Create frame buffers for each camera
    _frame_buffers = {cam.cam_id: FrameBuffer(cam.cam_id) for cam in _config.cameras}

    # Create pipeline manager (GStreamer imports deferred inside)
    try:
        from .gstreamer.pipeline import PipelineManager
        _pipeline_manager = PipelineManager(_config)
    except ImportError as e:
        logger.warning(f"GStreamer not available — pipelines disabled: {e}")
        _pipeline_manager = None

    # Create uploader
    try:
        from .camera_watcher.uploader import ZCamUploader
        _uploader = ZCamUploader(_config)
    except Exception as e:
        logger.warning(f"Uploader init failed: {e}")
        _uploader = None

    # Create poller — wired to enqueue files to uploader on recording end
    try:
        from .camera_watcher.api_poller import ZCamApiPoller

        def _on_recording_ended(camera, files):
            if _uploader:
                for f in files:
                    _uploader.enqueue(camera, f)

        _poller = ZCamApiPoller(_config, on_recording_ended=_on_recording_ended)
    except Exception as e:
        logger.warning(f"Poller init failed: {e}")
        _poller = None

    _initialized = True
    logger.info(
        f"Z-CAM services initialized: {len(_config.cameras)} cameras, "
        f"platform={_config.platform}, "
        f"pipeline={'yes' if _pipeline_manager else 'no'}, "
        f"poller={'yes' if _poller else 'no'}, "
        f"uploader={'yes' if _uploader else 'no'}"
    )


def start_all_services():
    """Start pipelines, poller, and uploader."""
    global _services_running
    _ensure_initialized()

    if _services_running:
        logger.info("Z-CAM services already running")
        return

    # Initialize and start GStreamer pipelines
    if _pipeline_manager:
        if not _pipeline_manager._gst_initialized:
            _pipeline_manager.initialize(_frame_buffers)
        _pipeline_manager.start_all()

    # Start camera state poller
    if _poller:
        _poller.start()

    # Start 4K upload worker
    if _uploader:
        _uploader.start()

    _services_running = True
    logger.info("All Z-CAM services started")


def stop_all_services():
    """Stop all services gracefully."""
    global _services_running

    if not _services_running:
        return

    if _pipeline_manager:
        _pipeline_manager.stop_all()

    if _poller:
        _poller.stop()

    if _uploader:
        _uploader.stop()

    _services_running = False
    logger.info("All Z-CAM services stopped")


def is_running() -> bool:
    """Check if services are currently running."""
    return _services_running
