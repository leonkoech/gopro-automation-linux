"""T2 (core logic) for the ffmpeg timeline watchdog — the regression guard for the
frozen-timeline data loss (two games lost when gst+byte-growth called a stalled camera
'healthy'). Proves the DECISION: restart on a frozen clock / dead process, never on a
healthy advancing clock. No cameras or ffmpeg needed — _ff_finalize / _launch_ffmpeg /
journal are stubbed. Run: `python3 -m pytest tests/test_recorder_ffmpeg_watchdog.py` or
`python3 tests/test_recorder_ffmpeg_watchdog.py`.
"""
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("REC_ENGINE", "ffmpeg")
os.environ.setdefault("REC_FFMPEG_STALL_SEC", "15")

from agx_pipeline.recording import RecordingController, load_config  # noqa: E402

_CFG = os.path.join(os.path.dirname(__file__), "..", "agx_pipeline", "cameras.json")


class _Proc:
    def __init__(self, alive=True):
        self._alive = alive

    def poll(self):
        return None if self._alive else 0


def _make_run(cam, **kw):
    r = {"angle": "FL", "cam": cam, "session_dir": "/tmp/x", "label": "t",
         "path": "/tmp/x/t_FL.mp4", "restart": 0, "segments": ["/tmp/x/t_FL.mp4"],
         "proc": _Proc(True), "out_time": 100.0, "last_advance": time.time()}
    r.update(kw)
    return r


def _watch(ctl, run):
    """Run the ffmpeg watch decision with restart side-effects stubbed; return the
    ordered list of actions it took (['finalize', 'launch'] == a restart)."""
    calls = []
    ctl._ff_finalize = lambda r: calls.append("finalize")
    ctl._launch_ffmpeg = lambda r: (calls.append("launch"), True)[1]
    ctl._seg_path = lambda r, n: f"/tmp/x/t_FL.r{n}.mp4"
    ctl._ff_write_journal = lambda label, runs: None
    ctl._watch_one_ffmpeg(run)
    return calls


def test_timeline_watchdog_decisions():
    ctl = RecordingController(load_config(_CFG))
    assert ctl.engine == "ffmpeg"
    cam = ctl.cfg.cameras[0]

    # frozen timeline: alive but out_time idle > stall -> MUST restart (the missed case)
    assert _watch(ctl, _make_run(cam, last_advance=time.time() - 20)) == ["finalize", "launch"]
    # healthy: clock advancing -> MUST NOT restart
    assert _watch(ctl, _make_run(cam, last_advance=time.time())) == []
    # below the stall threshold -> not yet
    assert _watch(ctl, _make_run(cam, last_advance=time.time() - 10)) == []
    # process died -> restart even with a recent last_advance
    assert _watch(ctl, _make_run(cam, proc=_Proc(False), last_advance=time.time())) == ["finalize", "launch"]
    # max restarts reached -> stop (footage-loss guard, no infinite loop)
    ctl.wd_max_restarts = 3
    assert _watch(ctl, _make_run(cam, last_advance=time.time() - 20, restart=3)) == []


if __name__ == "__main__":
    test_timeline_watchdog_decisions()
    print("test_timeline_watchdog_decisions: PASS")
