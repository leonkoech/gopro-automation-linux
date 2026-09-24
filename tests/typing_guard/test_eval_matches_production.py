"""The TYPE eval must score the chain production actually runs.

scripts/gt_eval/type_eval.py used to keep its own copy of the classify knobs,
under a docstring promising it was "the same recipe as production". It then fell
behind: production moved to the v2 stack (SHOT_ATTRIB=release_pose, ankle+mix
feet, STRICT) while the copy still passed `possession`. An eval run would have
reported a number for a chain nobody runs, and we would have believed it.

These tests pin the two together. If someone reintroduces a local copy, or adds
a knob to production that the eval does not inherit, this fails.
"""

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts" / "gt_eval"))

TYPE_EVAL_SRC = REPO / "scripts" / "gt_eval" / "type_eval.py"


def _load_type_eval():
    """type_eval pulls in the box's GT client (numpy, UballClient). Where that
    is unavailable the knob comparison cannot run, but the source check below
    still can -- so skip rather than fail."""
    try:
        import type_eval  # noqa: PLC0415
        return type_eval
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"type_eval not importable in this environment: {exc}")


def test_eval_does_not_keep_its_own_copy_of_the_knobs():
    """A source-level check, so it holds even where type_eval cannot be imported.

    The eval may set SHOT_RIM_TS -- that is a property of how it cuts clips, not
    of the typing chain -- but nothing else.
    """
    src = TYPE_EVAL_SRC.read_text()
    offenders = [
        line.strip()
        for line in src.splitlines()
        if 'env["SHOT_' in line and 'env["SHOT_RIM_TS"]' not in line
    ]
    assert not offenders, (
        "type_eval must inherit the typing knobs from production, not set its own: "
        f"{offenders}"
    )


def test_eval_inherits_every_production_knob():
    type_eval = _load_type_eval()
    from agx_pipeline.shot_typing_live import _classify_env

    production = _classify_env()
    evaluation = type_eval.classify_env()

    for key, want in production.items():
        if not key.startswith("SHOT_") or key == "SHOT_RIM_TS":
            continue
        assert evaluation.get(key) == want, (
            f"{key}: production runs {want!r} but the eval would use "
            f"{evaluation.get(key)!r} — the eval would score a different chain"
        )


def test_the_v2_attribution_is_what_both_use():
    """Named explicitly: this exact value is what silently diverged before."""
    type_eval = _load_type_eval()
    from agx_pipeline.shot_typing_live import _classify_env

    assert _classify_env()["SHOT_ATTRIB"] == "release_pose"
    assert type_eval.classify_env()["SHOT_ATTRIB"] == "release_pose"


def test_eval_keeps_its_own_rim_anchor():
    """The one legitimate difference: the eval cuts its clips with the rim at
    PRE_S, so it must override SHOT_RIM_TS rather than inherit production's."""
    type_eval = _load_type_eval()
    assert type_eval.classify_env()["SHOT_RIM_TS"] == f"{type_eval.PRE_S:.2f}"


@pytest.mark.unit
def test_production_enables_the_two_game_loop_fixes():
    """Flight gate + keep-FT were adopted on 2026-09-24 after breaking 0 shots on
    either test game. If someone drops them, the eval and production diverge
    from the measured stack -- this names the settings that were measured."""
    type_eval = _load_type_eval()
    from agx_pipeline.shot_typing_live import _classify_env

    env = _classify_env()
    assert env["SHOT_RP_FLIGHT_GATE"] == "1"
    assert (env["SHOT_RP_FLIGHT_A"], env["SHOT_RP_FLIGHT_B"]) == ("0.05", "0")
    assert env["SHOT_RP_FLIGHT_MIN_D_CM"] == "150"
    assert env["SHOT_RP_FLIGHT_SKIP_FT"] == "1"
    assert env["SHOT_STRICT_KEEP_FT"] == "1"
    assert type_eval.classify_env()["SHOT_STRICT_KEEP_FT"] == "1"
