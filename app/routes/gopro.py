"""
GoPro Management Routes

Endpoints:
- GET  /api/gopros                    - List connected GoPros
- GET  /api/gopros/<id>/status        - GoPro status
- POST /api/gopros/<id>/record/start  - Start recording
- POST /api/gopros/<id>/record/stop   - Stop recording
- GET  /api/gopros/<id>/record/status - Recording status

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

gopro_bp = Blueprint('gopro', __name__)


@gopro_bp.route('/', methods=['GET'])
def list_gopros():
    """List all connected GoPro cameras"""
    # TODO: Migrate from main.py
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@gopro_bp.route('/<gopro_id>/status', methods=['GET'])
def gopro_status(gopro_id):
    """Get status of a specific GoPro"""
    return jsonify({'error': 'Not yet migrated'}), 501


@gopro_bp.route('/<gopro_id>/record/start', methods=['POST'])
def start_recording(gopro_id):
    """Start recording on a GoPro"""
    return jsonify({'error': 'Not yet migrated'}), 501


@gopro_bp.route('/<gopro_id>/record/stop', methods=['POST'])
def stop_recording(gopro_id):
    """Stop recording on a GoPro"""
    return jsonify({'error': 'Not yet migrated'}), 501


@gopro_bp.route('/<gopro_id>/record/status', methods=['GET'])
def recording_status(gopro_id):
    """Get recording status"""
    return jsonify({'error': 'Not yet migrated'}), 501
