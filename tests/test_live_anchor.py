"""Ingest sends each master's start on the live stream's clock, so live cards get rebased.

Without `start_wallclock` the backend's live_rebase has no anchor and every live card keeps its
placeholder time (14 Oct test game: 202 cards, rebased 0).
"""
import os
from datetime import datetime

import pytest

from agx_pipeline import ingest
import uball_client


@pytest.fixture
def master(tmp_path):
    p = tmp_path / "game_FL.mp4"
    p.write_bytes(b"x")
    os.utime(p, (1_791_994_784.0, 1_791_994_784.0))       # last frame written
    return str(p)


@pytest.mark.unit
def test_anchor_is_first_frame_plus_live_lag(master):
    got = datetime.fromisoformat(ingest._live_anchor({"path": master, "duration": 2675.0}))
    assert got.utcoffset().total_seconds() == 0
    assert got.timestamp() == pytest.approx(1_791_994_784.0 - 2675.0 + ingest.LIVE_PDT_LAG_SEC)


@pytest.mark.unit
@pytest.mark.parametrize("extra", [
    {"stall": {"segments": 2}},            # watchdog stitched it: mtime moved after stop
    {"duration": None},
    {"duration": 0},
    {"path": "/nonexistent/master.mp4"},
])
def test_no_anchor_when_the_start_cannot_be_known(master, extra):
    f = dict({"path": master, "duration": 2675.0}, **extra)
    assert ingest._live_anchor(f) is None


class _Resp:
    status_code = 201

    @staticmethod
    def json():
        return {"id": "v1"}


def _register(monkeypatch, **kw):
    sent = {}
    c = uball_client.UballClient(backend_url="http://backend.test", email="e", password="p")
    monkeypatch.setattr(c, "_ensure_authenticated", lambda: True)
    monkeypatch.setattr(c, "_get_headers", lambda: {})
    monkeypatch.setattr(uball_client.requests, "post",
                        lambda url, json, headers, timeout: sent.update(json) or _Resp())
    assert c.register_video(game_id="g", s3_key="k", angle="LEFT", filename="f.mp4", **kw)
    return sent


@pytest.mark.unit
def test_register_sends_start_wallclock(monkeypatch):
    sent = _register(monkeypatch, start_wallclock="2026-10-14T15:35:14.060000+00:00")
    assert sent["start_wallclock"] == "2026-10-14T15:35:14.060000+00:00"


@pytest.mark.unit
def test_register_without_anchor_omits_the_field(monkeypatch):
    assert "start_wallclock" not in _register(monkeypatch)
