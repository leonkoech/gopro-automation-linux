"""
Log Streaming Routes

Endpoints:
- GET /api/logs/stream         - SSE log streaming
- GET /api/logs/recent         - Recent logs
- GET /api/logs/files          - Log files list
- GET /api/logs/files/<name>   - Read log file
- GET /api/logs/search         - Search logs

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

logs_bp = Blueprint('logs', __name__)


@logs_bp.route('/recent', methods=['GET'])
def get_recent():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@logs_bp.route('/files', methods=['GET'])
def list_files():
    return jsonify({'error': 'Not yet migrated'}), 501
