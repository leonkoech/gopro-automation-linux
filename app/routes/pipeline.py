"""
Pipeline Routes

Endpoints:
- GET  /api/pipeline/sessions/pending       - Pending sessions
- POST /api/sessions/<id>/upload-chapters   - Upload chapters
- GET  /api/pipeline/<id>/status            - Pipeline job status
- GET  /api/pipeline/jobs                   - List pipeline jobs
- POST /api/pipeline/auto-start             - Auto-start pipeline
- GET  /api/pipeline/full/<id>/status       - Full pipeline status
- GET  /api/pipeline/full/list              - List full pipelines

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

pipeline_bp = Blueprint('pipeline', __name__)


@pipeline_bp.route('/pipeline/sessions/pending', methods=['GET'])
def get_pending():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@pipeline_bp.route('/pipeline/jobs', methods=['GET'])
def list_jobs():
    return jsonify({'error': 'Not yet migrated'}), 501


@pipeline_bp.route('/sessions/<session_id>/upload-chapters', methods=['POST'])
def upload_chapters(session_id):
    return jsonify({'error': 'Not yet migrated'}), 501
