"""
Z-CAM Blueprint routes — all /api/zcam/* endpoints.

Follows the same pattern as main.py routes:
@zcam_bp.route(...), jsonify() responses, try/except error handling.
"""

import logging
from flask import jsonify, Response

from . import zcam_bp
from .services import (
    get_config, get_frame_buffers, get_pipeline_manager,
    get_poller, get_uploader, start_all_services, stop_all_services,
    is_running,
)
from .flask_viewer.stream import generate_mjpeg_stream

logger = logging.getLogger('zcam.routes')


# === Health / Status ===

@zcam_bp.route('/status', methods=['GET'])
def zcam_status():
    """Get overall Z-CAM system status."""
    config = get_config()
    pm = get_pipeline_manager()
    poller = get_poller()
    uploader = get_uploader()
    fbs = get_frame_buffers()

    cameras = []
    for cam in config.cameras:
        cam_info = {
            'cam_id': cam.cam_id,
            'ip': cam.ip,
            'label': cam.label,
            'ndi_name': cam.ndi_name,
            'kvs_stream': cam.kvs_stream_name,
            'recording_state': poller.get_camera_state(cam.cam_id) if poller else 'unknown',
            'pipeline_state': 'unknown',
            'stream_health': None,
        }
        # Pipeline state
        if pm and cam.cam_id in pm.pipelines:
            pipeline = pm.pipelines[cam.cam_id]
            cam_info['pipeline_state'] = pipeline.state
            cam_info['restart_count'] = pipeline._restart_count
        # Frame buffer health
        fb = fbs.get(cam.cam_id)
        if fb:
            cam_info['stream_health'] = fb.get_status()
        cameras.append(cam_info)

    return jsonify({
        'success': True,
        'running': is_running(),
        'platform': config.platform,
        'camera_count': len(config.cameras),
        'cameras': cameras,
        'upload_jobs': uploader.get_all_jobs() if uploader else [],
    })


@zcam_bp.route('/config', methods=['GET'])
def get_zcam_config():
    """Get current Z-CAM configuration (non-sensitive values only)."""
    config = get_config()
    return jsonify({
        'success': True,
        'platform': config.platform,
        'camera_count': len(config.cameras),
        'cameras': [
            {
                'cam_id': c.cam_id,
                'ip': c.ip,
                'label': c.label,
                'ndi_name': c.ndi_name,
                'kvs_stream': c.kvs_stream_name,
            }
            for c in config.cameras
        ],
        'kvs_region': config.kvs_region,
        's3_bucket': config.s3_bucket,
    })


# === Pipeline Control ===

@zcam_bp.route('/pipelines/start', methods=['POST'])
def start_pipelines():
    """Start all GStreamer pipelines + poller + uploader."""
    try:
        start_all_services()
        return jsonify({'success': True, 'message': 'All Z-CAM services started'})
    except Exception as e:
        logger.error(f"Failed to start pipelines: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@zcam_bp.route('/pipelines/stop', methods=['POST'])
def stop_pipelines():
    """Stop all GStreamer pipelines + poller + uploader."""
    try:
        stop_all_services()
        return jsonify({'success': True, 'message': 'All Z-CAM services stopped'})
    except Exception as e:
        logger.error(f"Failed to stop pipelines: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@zcam_bp.route('/pipelines/<cam_id>/start', methods=['POST'])
def start_camera_pipeline(cam_id):
    """Start pipeline for a single camera."""
    pm = get_pipeline_manager()
    if not pm or cam_id not in pm.pipelines:
        return jsonify({'success': False, 'error': f'Camera {cam_id} not found'}), 404
    try:
        pm.start_camera(cam_id)
        return jsonify({'success': True, 'cam_id': cam_id})
    except Exception as e:
        logger.error(f"Failed to start pipeline for {cam_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@zcam_bp.route('/pipelines/<cam_id>/stop', methods=['POST'])
def stop_camera_pipeline(cam_id):
    """Stop pipeline for a single camera."""
    pm = get_pipeline_manager()
    if not pm or cam_id not in pm.pipelines:
        return jsonify({'success': False, 'error': f'Camera {cam_id} not found'}), 404
    try:
        pm.stop_camera(cam_id)
        return jsonify({'success': True, 'cam_id': cam_id})
    except Exception as e:
        logger.error(f"Failed to stop pipeline for {cam_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# === MJPEG Live Viewer ===

@zcam_bp.route('/stream/<cam_id>/mjpeg', methods=['GET'])
def mjpeg_stream(cam_id):
    """Live MJPEG stream for a camera. Consumed by <img> tag in frontend."""
    fbs = get_frame_buffers()
    if cam_id not in fbs:
        return jsonify({'success': False, 'error': f'Camera {cam_id} not found'}), 404

    return Response(
        generate_mjpeg_stream(fbs[cam_id]),
        mimetype='multipart/x-mixed-replace; boundary=frame',
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
        },
    )


# === KVS Playback ===

@zcam_bp.route('/stream/<cam_id>/kvs-url', methods=['GET'])
def get_kvs_url(cam_id):
    """Get HLS playback URL for the KVS stream (signed, 1hr expiry)."""
    config = get_config()
    cam = next((c for c in config.cameras if c.cam_id == cam_id), None)
    if not cam:
        return jsonify({'success': False, 'error': f'Camera {cam_id} not found'}), 404

    try:
        from .kvs.kvs_sink import get_kvs_playback_url
        url = get_kvs_playback_url(cam.kvs_stream_name, config)
        return jsonify({'success': True, 'url': url, 'expires_in': 3600})
    except Exception as e:
        logger.error(f"Failed to get KVS URL for {cam_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# === Camera Recording State ===

@zcam_bp.route('/cameras/<cam_id>/recording-state', methods=['GET'])
def get_recording_state(cam_id):
    """Get recording state from Z-CAM API poller."""
    poller = get_poller()
    if not poller:
        return jsonify({'success': False, 'error': 'Poller not initialized'}), 503

    state = poller.get_camera_state(cam_id)
    return jsonify({'success': True, 'cam_id': cam_id, 'state': state})


# === Upload Jobs ===

@zcam_bp.route('/uploads', methods=['GET'])
def list_uploads():
    """List all 4K upload jobs."""
    uploader = get_uploader()
    if not uploader:
        return jsonify({'success': True, 'jobs': []})
    return jsonify({'success': True, 'jobs': uploader.get_all_jobs()})


@zcam_bp.route('/uploads/<job_id>', methods=['GET'])
def get_upload_status(job_id):
    """Get status of a specific upload job."""
    uploader = get_uploader()
    if not uploader:
        return jsonify({'success': False, 'error': 'Uploader not initialized'}), 503

    job = uploader.get_job(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    return jsonify({'success': True, 'job': job})
