"""The tracker's jersey number reaches the CV card as a suggestion in the note only: the card's
type comes from cv_points as before, and no player field is ever filled from it."""
from datetime import datetime, timezone

import pytest

import plays_sync


class FakeClient:
    def __init__(self):
        self.created = []

    def list_plays(self, game_id):
        return []

    def create_play(self, data):
        self.created.append(data)
        return {"id": "p%d" % len(self.created)}


def _game(cv_points):
    wc = datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc)
    return {"createdAt": "2026-09-16T02:07:10Z", "cv_points": cv_points,
            "shot_live": {"shots": [{"made": True, "side": "left", "cam": "SL", "wallclock": wc.isoformat()}]}}


def _run(game, monkeypatch):
    c = FakeClient()
    plays_sync.create_plays_from_shot_live(c, "G", game)
    return c.created


@pytest.mark.unit
def test_who_is_a_note_suggestion_not_a_player(monkeypatch):
    epoch = int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())
    created = _run(_game({"cv_%d_left" % epoch: {"zone": "3PT", "who": "12", "confidence": 0.8}}), monkeypatch)
    card = created[0]
    assert card["classification"] == "3PT_MAKE"
    assert "Tracker suggests #12" in card["note"]
    assert not card.get("player_a")


@pytest.mark.unit
def test_no_who_no_suggestion(monkeypatch):
    epoch = int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())
    created = _run(_game({"cv_%d_left" % epoch: {"zone": "2PT", "confidence": 0.8}}), monkeypatch)
    assert "Tracker suggests" not in created[0]["note"]


@pytest.mark.unit
def test_cv_card_gets_the_team_from_the_half_time_rule(monkeypatch):
    epoch = int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())
    g = _game({})
    g["tracker_teams"] = {"switch_epoch": epoch + 600, "left_basket_first": "right"}
    created = _run(g, monkeypatch)
    assert created[0]["team"] == "team2"            # right team attacks the left basket first
