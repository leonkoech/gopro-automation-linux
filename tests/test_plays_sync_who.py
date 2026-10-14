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
    g["tracker_teams"] = {"switch_epoch": None, "left_basket_first": "left", "source": "numbers", "number_agreement": 0.8}
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


@pytest.mark.unit
def test_a_miss_in_the_same_second_as_the_make_after_it_does_not_borrow_its_verdict():
    """Ball bounces off (miss) and drops in 0.6 s later (make): both share cv_<epoch>_left."""
    g = _flag_game({_cv_id(): {"zone": "3PT", "line_px": 90.0, "who": "7", "who_votes": {"7": 9.0},
                               "who_from": "clip"}})
    make = g["shot_live"]["shots"][0]
    miss = dict(make, made=False, wallclock=datetime(2026, 9, 16, 2, 20, 0, 100000, tzinfo=timezone.utc).isoformat())
    make["wallclock"] = datetime(2026, 9, 16, 2, 20, 0, 700000, tzinfo=timezone.utc).isoformat()
    g["shot_live"]["shots"] = [miss, make]
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(c, "G", g, rosters=ROSTERS)
    by_cls = {p["classification"]: p for p in c.created}
    assert set(by_cls) == {"FG_MISS", "3PT_MAKE"}
    assert by_cls["FG_MISS"]["events"][0]["cv_flag"] == "red"
    assert by_cls["3PT_MAKE"]["events"][0]["cv_flag"] == "green"


# ---- missed shots typed by the tracker (cv_misses) ---------------------------------------- #
@pytest.mark.unit
def test_a_miss_card_takes_its_type_jersey_and_flag_from_cv_misses():
    g = _flag_game({})
    g["shot_live"]["shots"][0]["made"] = False
    g["cv_misses"] = {_cv_id(): {"zone": "3PT", "line_px": 90.0, "who": "7", "who_votes": {"7": 9.0},
                                 "who_from": "clip", "points": 0, "made": False}}
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(c, "G", g, rosters=ROSTERS)
    card = c.created[0]
    assert card["classification"] == "3PT_MISS" and card["events"][0]["cv_flag"] == "green"
    assert "#7" in card["note"]


@pytest.mark.unit
def test_a_miss_and_the_make_in_the_same_second_each_keep_their_own_verdict():
    g = _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": "2", "who_votes": {"2": 9.0}, "who_from": "clip"}})
    make = g["shot_live"]["shots"][0]
    miss = dict(make, made=False, wallclock=datetime(2026, 9, 16, 2, 20, 0, 100000, tzinfo=timezone.utc).isoformat())
    make["wallclock"] = datetime(2026, 9, 16, 2, 20, 0, 700000, tzinfo=timezone.utc).isoformat()
    g["shot_live"]["shots"] = [miss, make]
    g["cv_misses"] = {_cv_id(): {"zone": "3PT", "line_px": 90.0, "who": "7", "who_votes": {"7": 9.0}, "who_from": "clip"}}
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(c, "G", g, rosters=ROSTERS)
    assert {p["classification"] for p in c.created} == {"3PT_MISS", "FG_MAKE"} or \
        {p["classification"] for p in c.created} == {"3PT_MISS", "2PT_MAKE"}


@pytest.mark.unit
def test_a_miss_never_reads_the_scorecards_cv_points_entry_of_a_make_with_its_id():
    assert plays_sync._typing_verdict({"cv_1_left": {"zone": "3PT"}}, {"side": "left", "made": False,
                                      "wallclock": datetime.fromtimestamp(1, timezone.utc).isoformat()},
                                      {"cv_1_left": {"zone": "2PT"}}) == {"zone": "2PT"}


# ---- shooter named by appearance when no number was read ---------------------------------- #
@pytest.mark.unit
def test_looks_fill_a_silent_card_as_a_yellow_hint():
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(
        c, "G", _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": None, "who_votes": {},
                                       "who_looks": {"n": "7", "conf": 0.8}}}), rosters=ROSTERS)
    card = c.created[0]
    assert "Looks like #7 Giovanni Garcia" in card["note"]
    assert card["events"][0]["cv_flag"] == "yellow" and "shooter" in card["events"][0]["cv_flag_reason"]
    assert card["events"][0]["cv_conf"]["shooter"] == 0.83


@pytest.mark.unit
def test_a_read_number_always_wins_over_looks():
    c = StoreClient()
    plays_sync.create_plays_from_shot_live(
        c, "G", _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": "2", "who_votes": {"2": 9.0},
                                       "who_from": "clip", "who_looks": {"n": "7", "conf": 0.99}}}), rosters=ROSTERS)
    assert "Tracker suggests #2" in c.created[0]["note"] and c.created[0]["events"][0]["cv_flag"] == "green"


@pytest.mark.unit
def test_weak_or_off_roster_looks_are_not_shown():
    for lk in ({"n": "7", "conf": 0.5}, {"n": "12", "conf": 0.9}, "junk"):
        c = StoreClient()
        plays_sync.create_plays_from_shot_live(
            c, "G", _flag_game({_cv_id(): {"zone": "2PT", "line_px": 90.0, "who": None, "who_votes": {}, "who_looks": lk}}),
            rosters=ROSTERS)
        assert "Looks like" not in c.created[0]["note"] and c.created[0]["events"][0]["cv_flag"] == "red"
