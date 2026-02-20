"""
AWS Batch Routes

Endpoints:
- POST /api/batch/register-completed        - Register completed batch jobs
- GET  /api/batch/transcode/<id>/status     - Batch job status
- POST /api/batch/transcode/<id>/complete   - Complete batch job
- POST /api/batch/transcode/<id>/wait       - Wait for batch job
- GET  /api/batch/transcode/config          - Batch config

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

batch_bp = Blueprint('batch', __name__)


@batch_bp.route('/register-completed', methods=['POST'])
def register_completed():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@batch_bp.route('/transcode/<job_id>/status', methods=['GET'])
def get_status(job_id):
    return jsonify({'error': 'Not yet migrated'}), 501


@batch_bp.route('/transcode/<job_id>/complete', methods=['POST'])
def complete_job(job_id):
    return jsonify({'error': 'Not yet migrated'}), 501


@batch_bp.route('/transcode/config', methods=['GET'])
def get_config():
    return jsonify({'error': 'Not yet migrated'}), 501
