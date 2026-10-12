"""The live publisher must CREATE the annotation game, not give up on it.

The annotation game is created by ingest, which runs when the footage lands —
hours after tip-off. So at the moment a game starts there is normally no
annotation game yet, and a live session that merely *looks one up* can never
open for a genuinely new game: the stream publishes happily to S3 while the
annotator's Live tab stays empty. That is exactly what happened on
2026-10-09.

These tests pin the two halves of the fix:
  * the publisher creates the game at tip-off, with the final score left NULL
    (0-0 at tip-off is indistinguishable from a real 0-0 and nothing would
    ever correct it);
  * ingest fills that hole in afterwards, and only ever fills a hole.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agx_pipeline import ingest  # noqa: E402

FB_ID = "3XHoZUGYF2kOY45VHDLD"
GAME_UUID = "e3661065-8f6d-4203-b017-32e16e0ddd40"

FB_GAME = {
    "leftTeam": {"name": "Team A", "displayName": "Team A",
                 "jerseyColorName": "Blue", "finalScore": 54},
    "rightTeam": {"name": "Team B", "displayName": "Team B",
                  "jerseyColorName": "Red", "finalScore": 61},
    "leftTeamId": "aaaa", "rightTeamId": "bbbb",
}


def _client(existing=None, created=None):
    c = MagicMock()
    c.get_game_by_firebase_id.return_value = existing
    c.create_game.return_value = created or {"id": GAME_UUID}
    c.update_game.return_value = {"id": GAME_UUID}
    return c


# ── creation at tip-off ─────────────────────────────────────────────────────

def test_live_creation_leaves_the_score_unset():
    """0-0 at tip-off must not be written as if it were the final score."""
    c = _client()
    with patch.object(ingest, "_resolve_checkin_roster", return_value=([], [])):
        ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09", final=False)
    payload = c.create_game.call_args[0][0]
    assert "team1_score" not in payload
    assert "team2_score" not in payload
    # everything else a game needs still goes out
    assert payload["firebase_game_id"] == FB_ID
    assert payload["timestamps_in_base_coords"] is True
    assert payload["video_name"] == "Team A vs Team B"


def test_ingest_creation_still_writes_the_final_score():
    """The ordinary (post-game) path is unchanged."""
    c = _client()
    with patch.object(ingest, "_resolve_checkin_roster", return_value=([], [])):
        ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09")
    payload = c.create_game.call_args[0][0]
    assert payload["team1_score"] == 54
    assert payload["team2_score"] == 61


# ── backfill at ingest ──────────────────────────────────────────────────────

def test_ingest_fills_in_the_score_the_live_game_could_not_know():
    existing = {"id": GAME_UUID, "firebase_game_id": FB_ID,
                "team1_score": None, "team2_score": None,
                "roster_team1": [{"n": 1}], "roster_team2": [{"n": 2}]}
    c = _client(existing=existing)
    got = ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09")

    assert got is existing          # reused, never duplicated
    c.create_game.assert_not_called()
    c.update_game.assert_called_once()
    game_id, fields = c.update_game.call_args[0]
    assert game_id == GAME_UUID
    assert fields == {"team1_score": 54, "team2_score": 61}


def test_backfill_never_overwrites_a_score_that_is_already_there():
    """An annotator's correction, or a second ingest run, must survive."""
    existing = {"id": GAME_UUID, "firebase_game_id": FB_ID,
                "team1_score": 55, "team2_score": 61,
                "roster_team1": [{"n": 1}], "roster_team2": [{"n": 2}]}
    c = _client(existing=existing)
    ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09")
    c.update_game.assert_not_called()


def test_backfill_fills_an_empty_roster():
    """Players check in after tip-off, so the live game's roster may be empty."""
    existing = {"id": GAME_UUID, "firebase_game_id": FB_ID,
                "team1_score": 54, "team2_score": 61,
                "roster_team1": [], "roster_team2": []}
    c = _client(existing=existing)
    with patch.object(ingest, "_resolve_checkin_roster",
                      return_value=([{"n": 7}], [{"n": 9}])):
        ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09")
    _, fields = c.update_game.call_args[0]
    assert fields == {"roster_team1": [{"n": 7}], "roster_team2": [{"n": 9}]}


def test_backfill_failure_never_breaks_ingestion():
    existing = {"id": GAME_UUID, "firebase_game_id": FB_ID,
                "team1_score": None, "team2_score": None,
                "roster_team1": [{"n": 1}], "roster_team2": [{"n": 2}]}
    c = _client(existing=existing)
    c.update_game.side_effect = RuntimeError("backend down")
    # the footage matters more than the score
    assert ingest._create_or_get_game(c, None, FB_GAME, FB_ID, "2026-10-09") is existing


# ── the publisher's own lookup ──────────────────────────────────────────────

def _publisher():
    from agx_pipeline.live_stream import LivePublisher

    return LivePublisher(SimpleNamespace(jetson_id="agx-1", cameras=[]))


def test_publisher_reuses_an_existing_annotation_game():
    pub = _publisher()
    c = _client(existing={"id": GAME_UUID})
    with patch.object(pub, "_client", return_value=c):
        assert pub._game_uuid(FB_ID, "game_20261009_090114") == GAME_UUID
    c.create_game.assert_not_called()


@contextmanager
def _fake_firebase():
    """Stub the firebase_service module — importing it needs firebase_admin,
    which a unit test has no business requiring."""
    fb = MagicMock()
    fb.get_game.return_value = FB_GAME
    stub = SimpleNamespace(get_firebase_service=lambda: fb)
    with patch.dict(sys.modules, {"firebase_service": stub}):
        yield fb


def test_publisher_creates_the_game_when_there_is_none():
    """The 2026-10-09 failure: lookup returns nothing, so nothing was shown."""
    pub = _publisher()
    c = _client(existing=None)
    with patch.object(pub, "_client", return_value=c), _fake_firebase(), \
         patch.object(ingest, "_resolve_checkin_roster", return_value=([], [])):
        assert pub._game_uuid(FB_ID, "game_20261009_090114") == GAME_UUID
    # created for the live path, so without a fabricated 0-0
    payload = c.create_game.call_args[0][0]
    assert "team1_score" not in payload
    assert payload["firebase_game_id"] == FB_ID


def test_publisher_survives_a_creation_failure():
    """A broken backend must not stop the stream — segments still reach S3."""
    pub = _publisher()
    c = _client(existing=None)
    c.create_game.side_effect = RuntimeError("backend down")
    with patch.object(pub, "_client", return_value=c), _fake_firebase(), \
         patch.object(ingest, "_resolve_checkin_roster", return_value=([], [])):
        assert pub._game_uuid(FB_ID, "game_20261009_090114") is None
