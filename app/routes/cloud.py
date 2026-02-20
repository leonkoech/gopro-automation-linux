"""
Cloud/S3 Routes

Endpoints:
- GET    /api/cloud/videos            - List S3 videos
- GET    /api/cloud/videos/stream     - Stream S3 video
- DELETE /api/cloud/videos            - Delete S3 video
- GET    /api/cloud/locations         - List S3 locations
- GET    /api/cloud/locations/<loc>/dates
- GET    /api/cloud/status

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

cloud_bp = Blueprint('cloud', __name__)


@cloud_bp.route('/videos', methods=['GET'])
def list_videos():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@cloud_bp.route('/status', methods=['GET'])
def get_status():
    return jsonify({'error': 'Not yet migrated'}), 501
