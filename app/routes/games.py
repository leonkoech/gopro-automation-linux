"""
Games and Uball Integration Routes

Endpoints:
- GET  /api/games/list                      - List Firebase games
- GET  /api/games/<id>                      - Get game details
- POST /api/games/sync                      - Sync to Uball
- GET  /api/games/<id>/recordings           - Game recordings
- POST /api/games/process-videos            - Process videos (sync)
- POST /api/games/process-videos/async      - Process videos (async)
- GET  /api/games/process-videos/<job>/status
- GET  /api/games/process-videos/jobs
- GET  /api/games/<id>/preview-extraction
- POST /api/games/register-video
- GET  /api/games/<id>/videos
- GET  /api/uball/status
- GET  /api/uball/teams

TODO: Migrate from main.py
"""
from flask import Blueprint, jsonify

games_bp = Blueprint('games', __name__)


# Stub routes - to be migrated from main.py
@games_bp.route('/games/list', methods=['GET'])
def list_games():
    return jsonify({'error': 'Not yet migrated - use main.py endpoints'}), 501


@games_bp.route('/games/<game_id>', methods=['GET'])
def get_game(game_id):
    return jsonify({'error': 'Not yet migrated'}), 501


@games_bp.route('/games/sync', methods=['POST'])
def sync_game():
    return jsonify({'error': 'Not yet migrated'}), 501


@games_bp.route('/uball/status', methods=['GET'])
def uball_status():
    return jsonify({'error': 'Not yet migrated'}), 501


@games_bp.route('/uball/teams', methods=['GET'])
def list_teams():
    return jsonify({'error': 'Not yet migrated'}), 501
