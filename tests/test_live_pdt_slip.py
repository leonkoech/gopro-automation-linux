"""The live clock must not silently fall behind real time.

ffmpeg's HLS muxer writes each PROGRAM-DATE-TIME as the previous one plus its duration, so time
the pipeline loses never reaches the clock. 14 Oct 2026, second E2E game: at 14:40 both angles'
clocks fell 25.6 s behind and stayed there; every live card made afterwards was stamped 26 s
early and landed 26 s early on the master.
"""
import pytest

from agx_pipeline import live_stream
from agx_pipeline.live_stream import PDT_DRIFT_MAX, _pdt_slip


def _run(n, t0=1000.0, seg=4.0, lag=-5.0, lost_at=None, lost=0.0, disc_at=()):
    """Segments whose files are written `lag` s after their clock says, losing `lost` s at one."""
    out, clock, real = [], t0, t0
    for i in range(n):
        if lost_at is not None and i == lost_at:
            real += lost                       # the pipeline paused; the clock did not move
        out.append((clock, seg, real + seg + lag, i in disc_at or i == 0))
        clock += seg
        real += seg
    return out


@pytest.mark.unit
def test_steady_stream_has_no_slip():
    assert abs(_pdt_slip(_run(50))) < 1e-9


@pytest.mark.unit
def test_lost_time_shows_as_slip():
    assert _pdt_slip(_run(50, lost_at=30, lost=25.6)) == pytest.approx(25.6)
    assert _pdt_slip(_run(50, lost_at=30, lost=25.6)) > PDT_DRIFT_MAX


@pytest.mark.unit
def test_jitter_below_the_limit_is_ignored():
    entries = [(p, d, w + (0.8 if i % 7 == 0 else 0.0), disc) for i, (p, d, w, disc) in enumerate(_run(50))]
    assert abs(_pdt_slip(entries)) < PDT_DRIFT_MAX


@pytest.mark.unit
def test_a_restart_rebaselines_on_the_new_run():
    # slipped at 30, restarted (discontinuity) at 32: the new run is judged on its own
    entries = _run(32, lost_at=30, lost=25.6) + [
        (p + 128.0 + 25.6, d, w + 128.0 + 25.6, disc) for p, d, w, disc in _run(10)]
    assert abs(_pdt_slip(entries)) < 1e-9


@pytest.mark.unit
def test_one_late_segment_is_not_a_slip():
    entries = _run(50)
    p, d, w, disc = entries[-1]
    entries[-1] = (p, d, w + 6.0, disc)                 # the newest file closed 6 s late
    assert abs(_pdt_slip(entries)) < PDT_DRIFT_MAX


@pytest.mark.unit
def test_slip_is_confirmed_once_it_holds_for_the_newest_segments():
    n_after = live_stream.PDT_CONFIRM_SEGS
    assert _pdt_slip(_run(30 + n_after - 1, lost_at=30, lost=25.6)) < PDT_DRIFT_MAX
    assert _pdt_slip(_run(30 + n_after, lost_at=30, lost=25.6)) == pytest.approx(25.6)


@pytest.mark.unit
def test_no_verdict_before_a_baseline_exists():
    assert _pdt_slip(_run(live_stream.PDT_BASELINE_SEGS + live_stream.PDT_CONFIRM_SEGS - 1)) is None
    assert _pdt_slip([]) is None
