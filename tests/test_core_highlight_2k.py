"""The Core reel uses a clip's 2K render only when CORE_REEL_USE_2K is on, and only where
the render exists — a clip without one keeps its plain URL, so switching the flag on can
never drop a highlight from the reel."""
import pytest

from agx_pipeline.core_highlight import build_reel


def _game():
    return {
        "logs": [
            {"id": "a", "actionType": "score_added", "team": "left", "timestamp": "2026-09-19T01:00:00Z",
             "payload": {"points": 3}},
            {"id": "b", "actionType": "score_added", "team": "right", "timestamp": "2026-09-19T01:01:00Z",
             "payload": {"points": 2}},
        ],
        "highlights": {
            "a": {"status": "ready", "url": "https://cdn/a_FL.mp4", "url_2k": "https://cdn/a_FL_2k.mp4",
                  "angle": "FL"},
            "b": {"status": "ready", "url": "https://cdn/b_FR.mp4", "angle": "FR"},
        },
    }


@pytest.mark.unit
def test_plain_clips_when_flag_off(monkeypatch):
    monkeypatch.delenv("CORE_REEL_USE_2K", raising=False)
    urls = [c["url"] for c in build_reel("g", _game())["clips"]]
    assert urls == ["https://cdn/a_FL.mp4", "https://cdn/b_FR.mp4"]


@pytest.mark.unit
def test_2k_where_rendered_when_flag_on(monkeypatch):
    monkeypatch.setenv("CORE_REEL_USE_2K", "true")
    urls = [c["url"] for c in build_reel("g", _game())["clips"]]
    assert urls == ["https://cdn/a_FL_2k.mp4", "https://cdn/b_FR.mp4"]


@pytest.mark.unit
def test_cv_clip_takes_its_type_from_cv_points(monkeypatch):
    monkeypatch.delenv("CORE_REEL_USE_2K", raising=False)
    g = _game()
    g["highlights"]["cv_1_left"] = {"status": "ready", "url": "https://cdn/cv_FL.mp4", "angle": "FL"}
    g["cv_points"] = {"cv_1_left": {"zone": "3PT"}}
    clips = {c["url"]: c for c in build_reel("g", g)["clips"]}
    assert clips["https://cdn/cv_FL.mp4"]["play_type"] == "3PT_MAKE"
    assert clips["https://cdn/a_FL.mp4"]["play_type"] == "3PT_MAKE"      # score log still wins
