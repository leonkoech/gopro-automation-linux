"""
Admin Job Routes

Endpoints:
- POST /api/admin/jobs             - Start admin job
- GET  /api/admin/jobs             - List admin jobs
- GET  /api/admin/jobs/<id>        - Get admin job
- GET  /api/admin/jobs/<id>/stream - Stream admin job
- POST /api/admin/jobs/<id>/cancel - Cancel admin job

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/jobs', methods=['GET'])
def list_jobs():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@admin_bp.route('/jobs', methods=['POST'])
def start_job():
    return jsonify({'error': 'Not yet migrated'}), 501
