"""Tests for the AGX ingest 4K-masters toggle.

`agx-settings/{jetson_id}.transcode = false` = "4K mode": the NORMAL pipeline
runs unchanged (1080p proxies uploaded, annotation game + register) and the raw
4K masters are ALSO uploaded (`_4K` filename suffix) into the same game folder,
each logging its full s3://bucket/key so the operator can copy it from the
dashboard's ingestion card. The raw master must survive on disk until BOTH its
uploads (1080p proxy + 4K copy) are confirmed, even with DELETE_RAW on.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agx_pipeline import ingest  # noqa: E402

GAME_UUID = "12345678-abcd-ef00-1111-222233334444"
FOLDER = "12345678-abcd-ef00-1111"  # first 4 uuid segments


# --- helpers ---------------------------------------------------------------


def _fb_with_settings(transcode):
    """MagicMock Firebase whose agx-settings doc returns the given value.
    `transcode=None` models a missing doc/field."""
    fb = MagicMock()
    snap = MagicMock()
    snap.exists = transcode is not None
    snap.to_dict.return_value = {} if transcode is None else {"transcode": transcode}
    fb.db.collection.return_value.document.return_value.get.return_value = snap
    fb.get_game.return_value = {}
    return fb


def _uball_client():
    client = MagicMock()
    client.get_game_by_firebase_id.return_value = None
    client.create_game.return_value = {"id": GAME_UUID}
    return client


def _make_state_and_files(tmp_path, label="game_20260728_120000"):
    session_dir = tmp_path / label
    session_dir.mkdir(parents=True)
    files = []
    for angle in ("FL", "FR"):
        p = session_dir / f"{angle}.mp4"
        p.write_bytes(b"\x00" * 1024)
        files.append({"angle": angle, "path": str(p), "ok": True})
    state = {"firebase_game_id": "fbg1", "label": label, "session_ids": {}}
    stopped = {"label": label, "files": files, "audio": []}
    return state, stopped, session_dir


def _cfg(tmp_path):
    return SimpleNamespace(jetson_id="agx-01", output_dir=str(tmp_path),
                           app_mount=str(tmp_path), docker_cmd=["docker"],
                           docker_image="img", location="court-a")


def _run_doc(fb):
    return fb.db.collection.return_value.document.return_value.update.call_args.args[0]


@pytest.fixture
def uploads(monkeypatch):
    """Record `_upload` calls; never touch the network or ffprobe."""
    calls = []
    monkeypatch.setattr(ingest, "_upload", lambda local, key, **kw: calls.append((local, key)))
    monkeypatch.setattr(ingest, "_probe_dur", lambda path: 12.3)
    return calls


@pytest.fixture
def fake_transcode(monkeypatch):
    transcoded = []

    def _fake(src, dst, cfg):
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        Path(dst).write_bytes(b"\x00" * 64)
        transcoded.append(src)
        return True

    monkeypatch.setattr(ingest, "_transcode_1080p", _fake)
    return transcoded


# --- _transcode_enabled ----------------------------------------------------


def test_toggle_defaults_on_without_firebase():
    assert ingest._transcode_enabled(None, "agx-01") is True


def test_toggle_reads_false_from_settings_doc():
    assert ingest._transcode_enabled(_fb_with_settings(False), "agx-01") is False


def test_toggle_missing_doc_falls_back_to_default():
    assert ingest._transcode_enabled(_fb_with_settings(None), "agx-01") is True


def test_toggle_read_error_falls_back_to_default():
    fb = MagicMock()
    fb.db.collection.side_effect = RuntimeError("firestore down")
    assert ingest._transcode_enabled(fb, "agx-01") is True


# --- run_ingestion: 4K mode (normal pipeline + 4K masters) -----------------


def test_4k_mode_runs_normal_pipeline_and_also_uploads_masters(
        tmp_path, monkeypatch, uploads, fake_transcode):
    state, stopped, _ = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", False)
    client = _uball_client()
    monkeypatch.setattr(ingest, "get_uball_client", lambda: client)
    fb = _fb_with_settings(False)

    ingest.run_ingestion(fb, _cfg(tmp_path), "pid1", state, stopped, None)

    # normal pipeline intact: transcoded, annotation game created, FL/FR registered
    assert len(fake_transcode) == 2
    client.create_game.assert_called_once()
    assert client.register_video.call_count == 2
    for call in client.register_video.call_args_list:
        assert not call.kwargs["s3_key"].endswith("_4K.mp4")  # registers the 1080p proxy

    # PLUS the raw masters uploaded into the same game folder
    proxy_keys = [k for _, k in uploads if not k.endswith("_4K.mp4")]
    master = {k: local for local, k in uploads if k.endswith("_4K.mp4")}
    assert len(proxy_keys) == 2 and len(master) == 2
    raw_paths = {f["path"] for f in stopped["files"]}
    for key, local in master.items():
        assert f"/{FOLDER}/" in key
        assert local in raw_paths  # the 4K upload ships the raw master itself

    # discoverable: s3:// paths in the run log + structured uploads map
    doc = _run_doc(fb)
    master_logs = [l["msg"] for l in doc["logs"] if "4K master uploaded -> s3://" in l["msg"]]
    assert len(master_logs) == 2
    assert doc["uploads"]["FL_4K"]["s3_key"].endswith("_FL_4K.mp4")
    assert doc["uploads"]["FR_4K"]["s3_key"].endswith("_FR_4K.mp4")
    assert doc["uploads"]["FL"]["s3_key"].endswith("_FL.mp4")

    # DELETE_RAW off -> raws stay on disk
    for p in raw_paths:
        assert Path(p).exists()


def test_4k_mode_raw_deleted_only_after_both_uploads(
        tmp_path, monkeypatch, uploads, fake_transcode):
    """DELETE_RAW on: a raw whose 4K upload failed must survive cleanup (and
    keep the session dir alive); a fully-uploaded raw is deleted."""
    state, stopped, session_dir = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", True)
    monkeypatch.setattr(ingest, "get_uball_client", lambda: _uball_client())

    def _upload_fr_4k_fails(local, key, **kw):
        if key.endswith("_FR_4K.mp4"):
            raise RuntimeError("s3 blip")
        uploads.append((local, key))

    monkeypatch.setattr(ingest, "_upload", _upload_fr_4k_fails)
    fb = _fb_with_settings(False)

    ingest.run_ingestion(fb, _cfg(tmp_path), "pid2", state, stopped, None)

    by_angle = {f["angle"]: Path(f["path"]) for f in stopped["files"]}
    assert not by_angle["FL"].exists()   # both uploads confirmed -> deleted
    assert by_angle["FR"].exists()       # 4K upload failed -> raw kept for retry
    assert session_dir.exists()          # dir with a kept raw must not be rmtree'd
    doc = _run_doc(fb)
    assert any("FR 4K master upload failed" in l["msg"] for l in doc["logs"])
    assert doc["status"] == "completed"  # normal 1080p flow succeeded regardless


# --- run_ingestion: normal mode (toggle on) --------------------------------


def test_normal_mode_uploads_no_4k_masters(tmp_path, monkeypatch, uploads, fake_transcode):
    state, stopped, _ = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", False)
    monkeypatch.setattr(ingest, "get_uball_client", lambda: None)  # no register creds

    ingest.run_ingestion(_fb_with_settings(True), _cfg(tmp_path),
                         "pid3", state, stopped, None)

    assert len(fake_transcode) == 2
    assert len(uploads) == 2
    assert not any(k.endswith("_4K.mp4") for _, k in uploads)
