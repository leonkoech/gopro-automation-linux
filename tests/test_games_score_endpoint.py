"""Tests for GET /api/games/<game_id>/score.

This endpoint exposes the authoritative Firebase final score to the
Uball annotation backend so EditorPage can render a reference score
alongside the annotator-entered totals (UBA-260).

These tests cover:
  * auth: 401 when X-Internal-Token mismatches GOPRO_INTERNAL_TOKEN
  * 404 when Firebase has no doc for the game id
  * happy path: leftTeam/rightTeam finalScore is returned in the documented shape
  * legacy fallback: score.home / score.away when finalScore is missing
  * missing scores: final_score is null on both sides, response is still 200
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The route module imports `flask`, `flask_cors`, but no firebase/boto3 at
# import time. We construct a minimal Flask app that only registers the
# games blueprint and stubs the firebase service in app.extensions.
sys.modules.setdefault('logging_service', MagicMock(get_logger=lambda *_: MagicMock()))


@pytest.fixture
def app(monkeypatch):
    from flask import Flask

    from app.routes.games import games_bp

    monkeypatch.setenv('GOPRO_INTERNAL_TOKEN', 'secret-token')

    flask_app = Flask(__name__)
    flask_app.register_blueprint(games_bp, url_prefix='/api')
    flask_app.extensions['services'] = {'firebase': MagicMock()}
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def firebase(app):
    return app.extensions['services']['firebase']


AUTH_HEADER = {'X-Internal-Token': 'secret-token'}


def test_score_missing_token_returns_401(client, firebase):
    firebase.get_game.return_value = {'leftTeam': {}, 'rightTeam': {}}
    resp = client.get('/api/games/game-1/score')
    assert resp.status_code == 401


def test_score_bad_token_returns_401(client, firebase):
    firebase.get_game.return_value = {'leftTeam': {}, 'rightTeam': {}}
    resp = client.get('/api/games/game-1/score', headers={'X-Internal-Token': 'nope'})
    assert resp.status_code == 401


def test_score_game_not_found_returns_404(client, firebase):
    firebase.get_game.return_value = None
    resp = client.get('/api/games/missing/score', headers=AUTH_HEADER)
    assert resp.status_code == 404
    assert resp.get_json() == {'error': 'game not found'}


def test_score_happy_path_uses_final_score(client, firebase):
    firebase.get_game.return_value = {
        'id': 'fb-1',
        'leftTeam': {'name': 'Lions', 'finalScore': 87},
        'rightTeam': {'name': 'Tigers', 'finalScore': 82},
    }
    resp = client.get('/api/games/fb-1/score', headers=AUTH_HEADER)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['game_id'] == 'fb-1'
    assert body['home'] == {'name': 'Lions', 'final_score': 87}
    assert body['away'] == {'name': 'Tigers', 'final_score': 82}
    assert body['source'] == 'firebase'


def test_score_legacy_fallback(client, firebase):
    firebase.get_game.return_value = {
        'id': 'fb-2',
        'leftTeam': {'name': 'Lions'},
        'rightTeam': {'name': 'Tigers'},
        'score': {'home': 70, 'away': 65},
    }
    resp = client.get('/api/games/fb-2/score', headers=AUTH_HEADER)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['home']['final_score'] == 70
    assert body['away']['final_score'] == 65


def test_score_no_recorded_scores_returns_null(client, firebase):
    firebase.get_game.return_value = {
        'id': 'fb-3',
        'leftTeam': {'name': 'Lions'},
        'rightTeam': {'name': 'Tigers'},
    }
    resp = client.get('/api/games/fb-3/score', headers=AUTH_HEADER)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['home']['final_score'] is None
    assert body['away']['final_score'] is None


def test_score_firebase_unavailable_returns_503(client, app):
    app.extensions['services']['firebase'] = None
    resp = client.get('/api/games/fb-3/score', headers=AUTH_HEADER)
    assert resp.status_code == 503
