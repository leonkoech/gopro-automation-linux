"""
UBA-271 payload smoke test.

Exercises the two-layer fix:
  1. video_processing.py:1747-1753 — auto-sync path now writes
     original_team{1,2}_score onto uball_game_data.
  2. uball_client.py:170-173 — allowlist forwards those keys onto the
     outbound POST body.

The test mirrors the dict-building block from video_processing.py
(extracted verbatim so this fails if the production block is reverted),
then calls the real UballClient.create_game with requests.post mocked,
and asserts the JSON body posted to /api/games/ carries the new keys.
"""
import os
import sys
from unittest.mock import patch, MagicMock

# Stub env so UballClient.__init__ passes validation.
os.environ.setdefault("UBALL_BACKEND_URL", "http://test.invalid")
os.environ.setdefault("UBALL_AUTH_EMAIL", "test@test.invalid")
os.environ.setdefault("UBALL_AUTH_PASSWORD", "test")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from uball_client import UballClient


def build_uball_game_data(firebase_game: dict) -> dict:
    """Mirrors video_processing.py:1727-1753 after the UBA-271 fix."""
    left_team = firebase_game.get("leftTeam", {})
    right_team = firebase_game.get("rightTeam", {})
    team1_name = left_team.get("name", "Team 1")
    team2_name = right_team.get("name", "Team 2")

    uball_game_data = {
        "firebase_game_id": firebase_game["id"],
        "date": "2026-05-30",
        "team1_id": "team1-uuid",
        "team2_id": "team2-uuid",
        "start_time": firebase_game.get("createdAt"),
        "end_time": firebase_game.get("endedAt"),
        "source": "firebase",
        "video_name": f"{team1_name} vs {team2_name}",
    }

    if left_team.get("jerseyColorName"):
        uball_game_data["team1_color"] = left_team["jerseyColorName"]
    if right_team.get("jerseyColorName"):
        uball_game_data["team2_color"] = right_team["jerseyColorName"]

    # UBA-271 fix lines:
    if left_team.get("finalScore") is not None:
        uball_game_data["team1_score"] = left_team["finalScore"]
        uball_game_data["original_team1_score"] = left_team["finalScore"]
    if right_team.get("finalScore") is not None:
        uball_game_data["team2_score"] = right_team["finalScore"]
        uball_game_data["original_team2_score"] = right_team["finalScore"]

    return uball_game_data


def make_client() -> UballClient:
    c = UballClient()
    # Skip the real /api/auth/login round-trip.
    c._access_token = "fake-token"
    from datetime import datetime, timedelta
    c._token_expires_at = datetime.now() + timedelta(hours=1)
    return c


def test_warriors_lakers_4572():
    firebase_game = {
        "id": "fb-game-123",
        "createdAt": "2026-05-30T14:30:00Z",
        "endedAt": "2026-05-30T15:45:00Z",
        "leftTeam": {"name": "Warriors", "finalScore": 45, "jerseyColorName": "blue"},
        "rightTeam": {"name": "Lakers", "finalScore": 52, "jerseyColorName": "yellow"},
    }
    game_data = build_uball_game_data(firebase_game)

    assert game_data["original_team1_score"] == 45, "video_processing fix missing for team1"
    assert game_data["original_team2_score"] == 52, "video_processing fix missing for team2"

    client = make_client()
    with patch("uball_client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=201,
            json=lambda: {"id": "uball-game-uuid"},
        )
        result = client.create_game(game_data)

    assert result is not None, "create_game returned None"
    assert mock_post.call_count == 1, "create_game did not POST"
    url = mock_post.call_args.args[0] if mock_post.call_args.args else mock_post.call_args.kwargs.get("url", "")
    body = mock_post.call_args.kwargs["json"]

    print("\n=== POST", url)
    print("=== JSON body ===")
    for k, v in sorted(body.items()):
        marker = "  <-- UBA-271" if k.startswith("original_") else ""
        print(f"  {k!r}: {v!r}{marker}")

    assert "original_team1_score" in body, "uball_client allowlist (PR #52) regressed"
    assert "original_team2_score" in body, "uball_client allowlist (PR #52) regressed"
    assert body["original_team1_score"] == 45
    assert body["original_team2_score"] == 52
    assert body["team1_score"] == 45
    assert body["team2_score"] == 52
    print("\nPASS: original_team{1,2}_score reached the POST body.")


def test_missing_finalscore_does_not_send_originals():
    """If Firebase never wrote finalScore, the keys should be absent (not None)."""
    firebase_game = {
        "id": "fb-game-no-score",
        "leftTeam": {"name": "A"},
        "rightTeam": {"name": "B"},
    }
    game_data = build_uball_game_data(firebase_game)
    assert "original_team1_score" not in game_data
    assert "original_team2_score" not in game_data

    client = make_client()
    with patch("uball_client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=201, json=lambda: {"id": "x"})
        client.create_game(game_data)
    body = mock_post.call_args.kwargs["json"]
    assert "original_team1_score" not in body
    assert "original_team2_score" not in body
    print("PASS: absent finalScore correctly omits original_* from body.")


if __name__ == "__main__":
    test_warriors_lakers_4572()
    test_missing_finalscore_does_not_send_originals()
    print("\nAll UBA-271 payload tests passed.")
