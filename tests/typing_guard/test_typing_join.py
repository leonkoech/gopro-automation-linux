"""The typing verdict must reach the annotation card, keyed correctly.

Without this join a CV card says only FG_MAKE / FG_MISS and carries a flat
placeholder confidence, while the typing stage has already worked out 2/3/4PT
and free throws at 96% accuracy (cb9e1294, 2026-09-22) and written it to
cv_points.{logId}. The key is built from the shot's wallclock and side, so a
mistake there silently degrades every card back to the generic label with no
error anywhere — the failure mode is invisible, which is why it is tested.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from plays_sync import _ZONE_CLASS, _typing_verdict  # noqa: E402

WALL = "2026-09-15T00:30:00+00:00"
EPOCH = int(datetime.fromisoformat(WALL).timestamp())


def _shot(**kw):
    return {"wallclock": WALL, "side": "left", "made": True, **kw}


@pytest.mark.unit
def test_verdict_is_found_by_the_key_the_detector_writes():
    """shot_detect/live.py builds cv_{int(epoch)}_{side}."""
    pts = {f"cv_{EPOCH}_left": {"zone": "3PT", "confidence": 0.9}}
    got = _typing_verdict(pts, _shot())
    assert got and got["zone"] == "3PT"


@pytest.mark.unit
def test_the_wrong_side_is_not_matched():
    """Both hoops produce shots at similar times; the side disambiguates."""
    pts = {f"cv_{EPOCH}_right": {"zone": "3PT"}}
    assert _typing_verdict(pts, _shot(side="left")) is None


@pytest.mark.unit
@pytest.mark.parametrize("missing", [{"wallclock": None}, {"side": None}])
def test_a_shot_without_a_key_returns_nothing(missing):
    pts = {f"cv_{EPOCH}_left": {"zone": "2PT"}}
    assert _typing_verdict(pts, _shot(**missing)) is None


@pytest.mark.unit
def test_an_unparseable_wallclock_never_raises():
    """A bad timestamp must degrade to the generic label, not kill the ingest."""
    pts = {f"cv_{EPOCH}_left": {"zone": "2PT"}}
    assert _typing_verdict(pts, _shot(wallclock="not-a-time")) is None


@pytest.mark.unit
def test_empty_cv_points_returns_nothing():
    assert _typing_verdict({}, _shot()) is None
    assert _typing_verdict(None, _shot()) is None


@pytest.mark.unit
def test_a_non_dict_verdict_is_rejected():
    assert _typing_verdict({f"cv_{EPOCH}_left": "3PT"}, _shot()) is None


@pytest.mark.unit
def test_every_zone_maps_to_a_make_and_a_miss_label():
    for zone in ("2PT", "3PT", "4PT", "FREE_THROW"):
        assert _ZONE_CLASS[(zone, True)].endswith("MAKE")
        assert _ZONE_CLASS[(zone, False)].endswith("MISS")


@pytest.mark.unit
def test_free_throws_get_their_own_label_not_a_field_goal():
    """FREE_THROW was absent from the service's _POINTS entirely, so correctly
    typed free throws were being discarded before they could reach a card."""
    assert _ZONE_CLASS[("FREE_THROW", True)] == "FREE_THROW_MAKE"
    assert _ZONE_CLASS[("FREE_THROW", False)] == "FREE_THROW_MISS"


@pytest.mark.unit
def test_two_point_keeps_the_generic_field_goal_label():
    """2PT is recorded as FG_* in the annotation vocabulary, not '2PT_MAKE'."""
    assert _ZONE_CLASS[("2PT", True)] == "FG_MAKE"
    assert _ZONE_CLASS[("2PT", False)] == "FG_MISS"


@pytest.mark.unit
def test_an_unknown_zone_has_no_mapping_so_the_card_stays_generic():
    assert ("UNKNOWN", True) not in _ZONE_CLASS
    assert _ZONE_CLASS.get(("UNKNOWN", True)) is None
