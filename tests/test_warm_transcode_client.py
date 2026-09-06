"""Tests for the warm transcode worker's client half (item 1.1).

`_transcode_1080p` prefers the resident camrec service over a fresh container.
Two things make that non-trivial and are most of what is tested here.

Paths: the service resolves everything against its own RECORD_DIR and refuses
anything outside it. The source master is inside by construction (camrec wrote
it) but ingest's destination — output_dir/<label>/1080p — is not, so the output
is staged inside the root and moved afterwards.

Failure: the service must never be the reason a clip does not appear, so every
way it can let us down has to fall through to `_transcode_hw`.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agx_pipeline import ingest  # noqa: E402


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    """app_mount/camrec is camrec's RECORD_DIR — the default _warm_host_root
    picks, matching camrec_controller."""
    (tmp_path / "camrec").mkdir()
    monkeypatch.delenv("CAMREC_HOST_MOUNT", raising=False)
    monkeypatch.delenv("CAMREC_RECORD_DIR", raising=False)
    return SimpleNamespace(app_mount=str(tmp_path), docker_cmd=["docker"],
                           docker_image="img:latest")


def _master(cfg, name="camera1_game.mp4"):
    """A source file where camrec would have written it."""
    p = Path(cfg.app_mount) / "camrec" / name
    p.write_bytes(b"master")
    return str(p)


def _dst(cfg, label="game_1"):
    """Where ingest wants the proxy — deliberately outside camrec's root."""
    return str(Path(cfg.app_mount) / "recordings" / label / "1080p" / "out.mp4")


def _staged(cfg, dst):
    return Path(cfg.app_mount) / "camrec" / ingest.WARM_STAGE_DIR / Path(dst).name


def _resp(status, payload=None, text=""):
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.json.return_value = payload if payload is not None else {}
    return r


def _done(cfg, dst, content=b"warm output"):
    """A service that writes its staged output, as the real one would."""
    def _post(*a, **k):
        s = _staged(cfg, dst)
        s.parent.mkdir(parents=True, exist_ok=True)
        s.write_bytes(content)
        return _resp(201, {"id": "j1"})
    return _post


# --- path translation ------------------------------------------------------


def test_root_defaults_to_camrecs_record_dir(cfg):
    assert ingest._warm_host_root(cfg) == str(Path(cfg.app_mount) / "camrec")


def test_root_honours_camrec_record_dir(cfg, monkeypatch):
    """The one env var camrec_controller already reads — if the deployment
    moved camrec's RECORD_DIR, both must follow it together."""
    monkeypatch.setenv("CAMREC_RECORD_DIR", "/srv/camrec")
    assert ingest._warm_host_root(cfg) == "/srv/camrec"


def test_warm_path_maps_under_the_root(tmp_path):
    got = ingest._warm_path(str(tmp_path / "g1" / "a.mp4"), str(tmp_path))
    assert got == "/app/data/g1/a.mp4"


def test_warm_path_rejects_outside_the_root(tmp_path):
    outside = tmp_path.parent / "elsewhere" / "a.mp4"
    assert ingest._warm_path(str(outside), str(tmp_path)) is None


def test_warm_path_rejects_traversal(tmp_path):
    assert ingest._warm_path(str(tmp_path / ".." / "a.mp4"), str(tmp_path)) is None


# --- staging ---------------------------------------------------------------


def test_output_is_staged_inside_the_root_then_moved(cfg):
    """The destination ingest wants is outside camrec's RECORD_DIR, so the
    service is asked to write inside it and the file is moved after."""
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest.requests, "post", side_effect=_done(cfg, dst)) as post, \
         patch.object(ingest.requests, "get",
                      return_value=_resp(200, {"id": "j1", "state": "done"})):
        assert ingest._transcode_1080p(src, dst, cfg) is True

    asked_for = post.call_args.kwargs["json"]["dst"]
    assert asked_for.startswith("/app/data/" + ingest.WARM_STAGE_DIR + "/")
    assert Path(dst).read_bytes() == b"warm output"
    assert not _staged(cfg, dst).exists(), "staged copy should not be left behind"


def test_source_outside_the_root_is_not_submitted(cfg):
    """A master recorded by the built-in RecordingController lives under
    output_dir, which camrec cannot see. Skip the round trip."""
    src = Path(cfg.app_mount) / "recordings" / "raw.mp4"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"x")
    dst = _dst(cfg)

    with patch.object(ingest.requests, "post") as post, \
         patch.object(ingest, "_transcode_hw", return_value=True):
        assert ingest._transcode_1080p(str(src), dst, cfg) is True

    post.assert_not_called()


# --- fail-safe fallback ----------------------------------------------------


@pytest.mark.parametrize("failure", [
    "unreachable",      # camrec down / wrong port
    "refused",          # 4xx: path outside RECORD_DIR, disk full, bad args
    "no_job_id",        # accepted but malformed response
    "job_failed",       # gstreamer returned non-zero
    "output_missing",   # reported done, but the root is not what we think
])
def test_every_service_failure_falls_back_to_the_container(cfg, failure):
    src, dst = _master(cfg), _dst(cfg)

    post, get = MagicMock(), MagicMock()
    if failure == "unreachable":
        post.side_effect = OSError("connection refused")
    elif failure == "refused":
        post.return_value = _resp(400, text="outside RECORD_DIR")
    elif failure == "no_job_id":
        post.return_value = _resp(201, {})
    elif failure == "job_failed":
        post.side_effect = _done(cfg, dst)   # partial output, then failure
        get.return_value = _resp(200, {"id": "j1", "state": "failed"})
    else:
        post.return_value = _resp(201, {"id": "j1"})   # nothing written
        get.return_value = _resp(200, {"id": "j1", "state": "done"})

    def fake_hw(s, d, c):
        Path(d).parent.mkdir(parents=True, exist_ok=True)
        Path(d).write_bytes(b"container output")
        return True

    with patch.object(ingest.requests, "post", post), \
         patch.object(ingest.requests, "get", get), \
         patch.object(ingest, "_transcode_hw", side_effect=fake_hw) as hw:
        assert ingest._transcode_1080p(src, dst, cfg) is True

    hw.assert_called_once()
    assert Path(dst).read_bytes() == b"container output"


def test_failed_job_leaves_no_staged_file_behind(cfg):
    """A job that dies part-way can leave a short file in the staging dir;
    left there it would accumulate, and a later job with the same name would
    find stale bytes if the service ever declined to overwrite."""
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest.requests, "post", side_effect=_done(cfg, dst, b"partial")), \
         patch.object(ingest.requests, "get",
                      return_value=_resp(200, {"id": "j1", "state": "failed"})), \
         patch.object(ingest, "_transcode_hw", return_value=False), \
         patch.object(ingest, "_transcode_sw", return_value=True):
        ingest._transcode_1080p(src, dst, cfg)

    assert not _staged(cfg, dst).exists()


def test_success_skips_the_container_entirely(cfg):
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest.requests, "post", side_effect=_done(cfg, dst)), \
         patch.object(ingest.requests, "get",
                      return_value=_resp(200, {"id": "j1", "state": "done"})), \
         patch.object(ingest, "_transcode_hw") as hw:
        assert ingest._transcode_1080p(src, dst, cfg) is True

    hw.assert_not_called()


# --- lanes and switches ----------------------------------------------------


def test_ingest_uses_the_low_lane_and_clips_the_high_one(cfg):
    """A clip somebody is waiting on must outrank batch ingest in the queue."""
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest.requests, "post", side_effect=_done(cfg, dst)) as post, \
         patch.object(ingest.requests, "get",
                      return_value=_resp(200, {"id": "j1", "state": "done"})):
        ingest._transcode_1080p(src, dst, cfg)                    # clip
        ingest._transcode_1080p(src, dst, cfg, priority="low")    # ingest

    assert post.call_args_list[0].kwargs["json"]["priority"] == "high"
    assert post.call_args_list[1].kwargs["json"]["priority"] == "low"


def test_worker_off_never_calls_the_service(cfg):
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest, "WARM_MODE", "off"), \
         patch.object(ingest.requests, "post") as post, \
         patch.object(ingest, "_transcode_hw", return_value=True):
        assert ingest._transcode_1080p(src, dst, cfg) is True

    post.assert_not_called()


def test_worker_only_does_not_fall_back(cfg):
    """The A/B measurement mode: no container fallback, so a failure is
    visible rather than silently papered over."""
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest, "WARM_MODE", "only"), \
         patch.object(ingest.requests, "post", side_effect=OSError("down")), \
         patch.object(ingest, "_transcode_hw") as hw, \
         patch.object(ingest, "_transcode_sw") as sw:
        assert ingest._transcode_1080p(src, dst, cfg) is False

    hw.assert_not_called()
    sw.assert_not_called()


def test_request_body_matches_the_container_pipeline(cfg):
    """The service must be asked for the same output the container produces —
    1080p, the same bitrate, and idr_interval=30 so the annotation editor can
    seek to the second."""
    src, dst = _master(cfg), _dst(cfg)

    with patch.object(ingest.requests, "post", side_effect=_done(cfg, dst)) as post, \
         patch.object(ingest.requests, "get",
                      return_value=_resp(200, {"id": "j1", "state": "done"})):
        ingest._transcode_1080p(src, dst, cfg)

    body = post.call_args.kwargs["json"]
    assert (body["width"], body["height"]) == (1920, 1080)
    assert body["bitrate"] == int(ingest.HW_BITRATE)
    assert body["idr_interval"] == 30 and body["iframe_interval"] == 30
    assert body["overwrite"] is True
