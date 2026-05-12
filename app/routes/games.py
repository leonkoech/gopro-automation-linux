"""
Games and Uball Integration Routes

Endpoints:
- GET  /api/games/list                      - List Firebase games
- GET  /api/games/<id>                      - Get game details
- GET  /api/games/<id>/score                - Get authoritative final score (cross-repo reference)
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
import os
from datetime import datetime, timezone

from flask import Blueprint, current_app, jsonify, request

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


def _extract_final_score(team_doc):
    """Return team finalScore or None if not present / not numeric."""
    if not isinstance(team_doc, dict):
        return None
    value = team_doc.get('finalScore')
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _extract_legacy_score(score_doc, key):
    """Legacy fallback: score.home / score.away."""
    if not isinstance(score_doc, dict):
        return None
    value = score_doc.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _format_ended_at(value):
    """Normalize ended_at to ISO-8601 UTC string, or return None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    if isinstance(value, str):
        return value
    to_datetime = getattr(value, 'to_datetime', None)
    if callable(to_datetime):
        try:
            return _format_ended_at(to_datetime())
        except Exception:
            return None
    return None


@games_bp.route('/games/<game_id>/score', methods=['GET'])
def get_game_score(game_id):
    """
    Return the authoritative final score for a Firebase game.

    Used by Uball_annotation_tool-Backend as a read-only reference next
    to the annotator-entered score in EditorPage. Auth: X-Internal-Token
    must match GOPRO_INTERNAL_TOKEN.
    """
    expected_token = os.environ.get('GOPRO_INTERNAL_TOKEN')
    if expected_token:
        provided = request.headers.get('X-Internal-Token')
        if provided != expected_token:
            return jsonify({'error': 'unauthorized'}), 401

    firebase = current_app.extensions.get('services', {}).get('firebase')
    if firebase is None:
        return jsonify({'error': 'firebase unavailable'}), 503

    try:
        game = firebase.get_game(game_id)
    except Exception as e:
        current_app.logger.error(f"Firebase get_game failed for {game_id}: {e}")
        return jsonify({'error': 'firebase error'}), 503

    if not game:
        return jsonify({'error': 'game not found'}), 404

    left_team = game.get('leftTeam') or {}
    right_team = game.get('rightTeam') or {}

    home_score = _extract_final_score(left_team)
    away_score = _extract_final_score(right_team)

    if home_score is None or away_score is None:
        legacy = game.get('score')
        if home_score is None:
            home_score = _extract_legacy_score(legacy, 'home')
        if away_score is None:
            away_score = _extract_legacy_score(legacy, 'away')

    return jsonify({
        'game_id': game.get('id', game_id),
        'home': {
            'name': left_team.get('name'),
            'final_score': home_score,
        },
        'away': {
            'name': right_team.get('name'),
            'final_score': away_score,
        },
        'ended_at': _format_ended_at(game.get('endedAt') or game.get('ended_at')),
        'source': 'firebase',
    })
