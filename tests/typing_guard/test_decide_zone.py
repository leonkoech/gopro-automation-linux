"""The single place that decides a shot's zone and what it is worth.

It is a function rather than inline code so evaluation harnesses score the SAME
decision production makes. scripts/gt_eval/type_eval.py kept its own copy of the
classify knobs and silently fell a whole stack behind, reporting numbers for a
chain nobody ran; this is that trap one level down.
"""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "agx_pipeline"))

from agx_pipeline.shot_typing_live import (  # noqa: E402
    CONF_COMMITTED, CONF_FALLBACK, decide_zone,
)


def out(new, old="2PT", trust="True", deg="False"):
    return (f"RESULT x: ZONE_OLD={old} dist=500cm ZONE_NEW={new} "
            f"trust={trust} pose_degenerate={deg}")


@pytest.mark.unit
@pytest.mark.parametrize("zone", ["2PT", "3PT", "4PT", "FREE_THROW"])
def test_a_committed_zone_is_taken_at_full_confidence(zone):
    z, c, src = decide_zone(out(zone), 0)
    assert (z, c, src) == (zone, CONF_COMMITTED, "strict")


@pytest.mark.unit
def test_free_throw_is_a_real_zone_not_a_failure():
    """It was missing from _POINTS, so correctly typed free throws were being
    discarded as though the chain had produced nothing."""
    z, _, src = decide_zone(out("FREE_THROW"), 0)
    assert z == "FREE_THROW" and src == "strict"


@pytest.mark.unit
def test_unknown_falls_back_to_the_geometric_zone_at_low_confidence():
    z, c, src = decide_zone(out("UNKNOWN", old="3PT"), 0)
    assert (z, c, src) == ("3PT", CONF_FALLBACK, "geometric_fallback")
    assert c < CONF_COMMITTED


@pytest.mark.unit
def test_the_fallback_is_flagged_below_the_editor_threshold():
    """components/editor/playSource.ts flags green at >= 0.7. A fallback must
    land red, or an annotator reads a geometric guess as a committed call."""
    _, c, _ = decide_zone(out("UNKNOWN"), 0)
    assert c < 0.7


@pytest.mark.unit
def test_no_fallback_when_the_chain_did_not_trust_itself():
    assert decide_zone(out("UNKNOWN", trust="False"), 0) == (None, 0.0, "none")


@pytest.mark.unit
def test_no_fallback_when_the_process_failed():
    """rc != 0 means it never reached a verdict; ZONE_OLD would be meaningless."""
    assert decide_zone(out("UNKNOWN"), 2) == (None, 0.0, "none")


@pytest.mark.unit
def test_no_fallback_when_the_geometric_zone_is_also_unusable():
    assert decide_zone(out("UNKNOWN", old="UNKNOWN"), 0) == (None, 0.0, "none")


@pytest.mark.unit
def test_empty_output_is_not_a_zone():
    assert decide_zone("", 0) == (None, 0.0, "none")
    assert decide_zone("some unrelated log line", 0) == (None, 0.0, "none")


@pytest.mark.unit
def test_a_committed_zone_never_becomes_a_fallback():
    """Order matters: ZONE_NEW wins outright when it is usable, even if
    ZONE_OLD disagrees."""
    z, c, src = decide_zone(out("4PT", old="2PT"), 0)
    assert (z, src) == ("4PT", "strict") and c == CONF_COMMITTED
