"""
Media Management Routes

Endpoints:
- /api/videos/*           - Local video management
- /api/media/gopro/*      - GoPro file management
- /api/media/local/*      - Local file management
- /api/media/segments/*   - Segment management

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

media_bp = Blueprint('media', __name__)


# Stub - all media routes to be migrated from main.py
@media_bp.route('/videos', methods=['GET'])
def list_videos():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@media_bp.route('/media/segments', methods=['GET'])
def list_segments():
    return jsonify({'error': 'Not yet migrated'}), 501
