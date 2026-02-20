"""
Recording and Pipeline Trigger Routes

Endpoints:
- POST /api/recording/stop-all-and-process  - Stop all and start pipeline
- POST /api/recording/process-only          - Process without stopping
- GET  /api/recording/pipeline-status       - Pipeline status
- POST /api/recording/register              - Register recording session
- GET  /api/recording/sessions              - List sessions
- GET  /api/recording/sessions/<id>         - Get session

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

recording_bp = Blueprint('recording', __name__)


@recording_bp.route('/stop-all-and-process', methods=['POST'])
def stop_all_and_process():
    """Stop all recordings and start pipeline"""
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@recording_bp.route('/process-only', methods=['POST'])
def process_only():
    """Process without stopping recordings"""
    return jsonify({'error': 'Not yet migrated'}), 501


@recording_bp.route('/pipeline-status', methods=['GET'])
def get_pipeline_status():
    """Get pipeline status"""
    return jsonify({'error': 'Not yet migrated'}), 501


@recording_bp.route('/register', methods=['POST'])
def register_recording():
    """Register recording session"""
    return jsonify({'error': 'Not yet migrated'}), 501


@recording_bp.route('/sessions', methods=['GET'])
def list_sessions():
    """List recording sessions"""
    return jsonify({'error': 'Not yet migrated'}), 501


@recording_bp.route('/sessions/<session_id>', methods=['GET'])
def get_session(session_id):
    """Get recording session"""
    return jsonify({'error': 'Not yet migrated'}), 501
