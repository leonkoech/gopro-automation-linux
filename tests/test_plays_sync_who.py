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


ROSTERS = {"left": {"7": "Giovanni Garcia", "2": "Kevin Garcia"}, "right": {"0": "Nick Rosso", "12": "Ryan Jackson"}}


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
def test_cv_card_never_changes_the_official_score(monkeypatch):
    # the backend adds a make's points to the game score when a play carries `team`
    epoch = int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())
    g = _game({"cv_%d_left" % epoch: {"zone": "3PT", "who": "0", "who_votes": {"0": 9.0}, "line_px": 90.0,
                                       "who_from": "clip"}})
    g["tracker_teams"] = {"switch_epoch": epoch + 600, "left_basket_first": "right"}
    c = FakeClient()
    plays_sync.create_plays_from_shot_live(c, "G", g, rosters=ROSTERS)
    assert "team" not in c.created[0]
    assert "#0 Nick Rosso" in c.created[0]["note"]    # still the right team's roster: right attacks left first



@pytest.mark.unit
def test_roster_suggestion_names_the_player_from_the_shooting_team():
    v = {"who": "7", "who_votes": {"7": 9.0, "12": 2.0}}
    assert plays_sync._who_suggestion(v, "left", ROSTERS) == ("7", "Giovanni Garcia")


@pytest.mark.unit
def test_roster_drops_a_number_the_shooting_team_does_not_have():
    # the raw vote says #12, but #12 plays for the OTHER team: the left roster's #2 wins if strong
    v = {"who": "12", "who_votes": {"12": 9.0, "2": 4.0}}
    assert plays_sync._who_suggestion(v, "left", ROSTERS) == ("2", "Kevin Garcia")
    weak = {"who": "12", "who_votes": {"12": 9.0, "2": 1.0}}
    assert plays_sync._who_suggestion(weak, "left", ROSTERS) is None


@pytest.mark.unit
def test_no_roster_or_no_team_keeps_the_plain_number():
    v = {"who": "7", "who_votes": {"7": 9.0}}
    assert plays_sync._who_suggestion(v, None, ROSTERS) == ("7", None)
    assert plays_sync._who_suggestion(v, "left", {}) == ("7", None)
    assert plays_sync._who_suggestion({}, "left", ROSTERS) is None


@pytest.mark.unit
def test_card_note_carries_the_roster_name(monkeypatch):
    epoch = int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())
    game = _game({"cv_%d_left" % epoch: {"zone": "2PT", "who": "7", "who_votes": {"7": 9.0}, "confidence": 0.8}})
    game["tracker_teams"] = {"switch_epoch": None, "left_basket_first": "left"}
    c = FakeClient()
    plays_sync.create_plays_from_shot_live(c, "G", game, rosters=ROSTERS)
    assert "Tracker suggests #7 Giovanni Garcia" in c.created[0]["note"]
    assert c.created[0]["events"][0]["playerA"] is None


@pytest.mark.unit
def test_rosters_from_annotation_game_maps_team1_to_left():
    g = {"roster_team1": [{"name": "Giovanni Garcia", "jersey_number": 7}, {"name": "No Number", "jersey_number": None}],
         "roster_team2": [{"name": "Nick Rosso", "jersey_number": 0}]}
    assert plays_sync.rosters_from_annotation_game(g) == {"left": {"7": "Giovanni Garcia"}, "right": {"0": "Nick Rosso"}}
    assert plays_sync.rosters_from_annotation_game(None) == {}


class StoreClient(FakeClient):
    """Keeps created cards so refresh can list and patch them."""
    def __init__(self):
        super().__init__()
        self.patched = []

    def list_plays(self, game_id):
        return [dict(c, id="p%d" % i, source="cv") for i, c in enumerate(self.created)]

    def update_play(self, pid, fields):
        self.patched.append((pid, fields))
        self.created[int(pid[1:])].update(fields)


def _flag_game(cv):
    g = _game(cv)
    g["tracker_teams"] = {"switch_epoch": None, "left_basket_first": "left"}
    return g


def _cv_id():
    return "cv_%d_left" % int(datetime(2026, 9, 16, 2, 20, 0, tzinfo=timezone.utc).timestamp())


@pytest.mark.unit
def test_card_carries_its_flag_and_reason():
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(
        c, "G", _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": "7", "who_votes": {"7": 9.0},
                                       "who_from": "clip"}}), rosters=ROSTERS)
    ev = c.created[0]["events"][0]
    assert ev["cv_flag"] == "green" and c.created[0]["confidence"] == 0.9
    assert "GREEN" in c.created[0]["note"] and ev["cv_written"]


@pytest.mark.unit
def test_untyped_card_is_red_then_refresh_upgrades_it():
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(c, "G", _flag_game({}), rosters=ROSTERS)   # tracker not done yet
    assert c.created[0]["events"][0]["cv_flag"] == "red"
    later = _flag_game({_cv_id(): {"zone": "3PT", "line_px": 10.0, "who": "7", "who_votes": {"7": 9.0},
                                   "who_from": "clip"}})
    stats = plays_sync.refresh_cv_cards(c, "G", later, ROSTERS)
    assert stats["updated"] == 1
    card = c.created[0]
    assert card["classification"] == "3PT_MAKE" and card["events"][0]["cv_flag"] == "yellow"


@pytest.mark.unit
def test_refresh_never_touches_a_card_an_annotator_changed():
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(c, "G", _flag_game({}), rosters=ROSTERS)
    c.created[0]["classification"] = "3PT_MAKE"          # annotator fixed it in the editor
    later = _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": "7", "who_votes": {"7": 9.0},
                                   "who_from": "clip"}})
    stats = plays_sync.refresh_cv_cards(c, "G", later, ROSTERS)
    assert stats["edited_by_annotator"] == 1 and not c.patched


@pytest.mark.unit
def test_refresh_leaves_cards_from_before_flags_alone():
    c = StoreClient()
    c.created.append({"classification": "FG_MAKE", "note": "CV: old", "timestamp_seconds": 1.0,
                      "angle": "LEFT", "events": [{"label": "FG_MAKE"}]})
    stats = plays_sync.refresh_cv_cards(c, "G", _flag_game({}), ROSTERS)
    assert not c.patched and stats["updated"] == 0
