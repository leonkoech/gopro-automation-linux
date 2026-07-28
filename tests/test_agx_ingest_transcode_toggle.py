"""Tests for the AGX ingest transcode toggle (4K passthrough).

Promo shoots need the footage to STAY 4K: the dashboard writes
`agx-settings/{jetson_id}.transcode = false`, and ingest must then upload the
raw 4K masters as-is (`_4K` filename suffix) instead of the 1080p proxies,
skip the annotation register (the editor needs 1080p H.264), and — critically —
NOT delete the raw master during cleanup when DELETE_RAW is off, even though
in passthrough mode dst == src.
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


# --- helpers ---------------------------------------------------------------


def _fb_with_settings(transcode):
    """MagicMock Firebase whose agx-settings doc returns the given value.
    `transcode=None` models a missing doc/field."""
    fb = MagicMock()
    snap = MagicMock()
    snap.exists = transcode is not None
    snap.to_dict.return_value = {} if transcode is None else {"transcode": transcode}
    fb.db.collection.return_value.document.return_value.get.return_value = snap
    return fb


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


@pytest.fixture
def uploads(monkeypatch):
    """Record `_upload` calls; never touch the network or ffprobe."""
    calls = []
    monkeypatch.setattr(ingest, "_upload", lambda local, key, **kw: calls.append((local, key)))
    monkeypatch.setattr(ingest, "_probe_dur", lambda path: 12.3)
    return calls


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


# --- run_ingestion: 4K passthrough (transcode off) -------------------------


def test_passthrough_uploads_raw_4k_and_keeps_raw_when_delete_raw_off(
        tmp_path, monkeypatch, uploads):
    state, stopped, session_dir = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", False)
    monkeypatch.setattr(ingest, "_transcode_1080p",
                        lambda *a, **k: pytest.fail("must not transcode in passthrough"))
    monkeypatch.setattr(ingest, "get_uball_client",
                        lambda: pytest.fail("must not touch annotation tool in passthrough"))

    ingest.run_ingestion(_fb_with_settings(False), _cfg(tmp_path),
                         "pid1", state, stopped, None)

    # both raw masters uploaded as-is under the _4K suffix, no-game folder
    assert sorted(k.rsplit("_", 2)[-2] for _, k in uploads) == ["FL", "FR"]
    for local, key in uploads:
        assert key.endswith("_4K.mp4")
        assert "agx-game_20260728_120000" in key
        assert local in [f["path"] for f in stopped["files"]]  # uploaded the raw itself
    # dst == src in passthrough: cleanup must not delete the raw when DELETE_RAW is off
    for f in stopped["files"]:
        assert Path(f["path"]).exists()
    assert session_dir.exists()


def test_passthrough_deletes_raw_after_upload_when_delete_raw_on(
        tmp_path, monkeypatch, uploads):
    state, stopped, session_dir = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", True)
    monkeypatch.setattr(ingest, "_transcode_1080p",
                        lambda *a, **k: pytest.fail("must not transcode in passthrough"))
    monkeypatch.setattr(ingest, "get_uball_client", lambda: None)

    ingest.run_ingestion(_fb_with_settings(False), _cfg(tmp_path),
                         "pid2", state, stopped, None)

    assert len(uploads) == 2
    assert not session_dir.exists()  # raws uploaded -> session dir cleaned up


def test_passthrough_logs_copyable_s3_paths_and_records_uploads(
        tmp_path, monkeypatch, uploads):
    """The operator finds 4K footage from the run log: every uploaded angle logs
    its full s3://bucket/key (shown on the dashboard's ingestion card), and the
    run doc records the s3 prefix + per-angle keys."""
    state, stopped, _ = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", False)
    monkeypatch.setattr(ingest, "_transcode_1080p",
                        lambda *a, **k: pytest.fail("must not transcode in passthrough"))
    monkeypatch.setattr(ingest, "get_uball_client",
                        lambda: pytest.fail("must not touch annotation tool in passthrough"))
    fb = _fb_with_settings(False)

    ingest.run_ingestion(fb, _cfg(tmp_path), "pid4", state, stopped, None)

    run_doc = fb.db.collection.return_value.document.return_value.update.call_args.args[0]
    s3_logs = [l["msg"] for l in run_doc["logs"] if "uploaded -> s3://" in l["msg"]]
    assert len(s3_logs) == 2
    fl = next(m for m in s3_logs if m.startswith("FL uploaded"))
    assert f"s3://{ingest.BUCKET}/" in fl and "_FL_4K.mp4" in fl
    assert run_doc["s3"]["prefix"].endswith("/agx-game_20260728_120000/")
    assert run_doc["uploads"]["FL"]["s3_key"].endswith("_FL_4K.mp4")
    assert run_doc["uploads"]["FR"]["s3_key"].endswith("_FR_4K.mp4")


# --- run_ingestion: normal path (transcode on) -----------------------------


def test_transcode_on_produces_1080p_names_and_transcodes(tmp_path, monkeypatch, uploads):
    state, stopped, _ = _make_state_and_files(tmp_path)
    monkeypatch.setattr(ingest, "DELETE_RAW", False)
    transcoded = []

    def fake_transcode(src, dst, cfg):
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        Path(dst).write_bytes(b"\x00" * 64)
        transcoded.append(src)
        return True

    monkeypatch.setattr(ingest, "_transcode_1080p", fake_transcode)
    monkeypatch.setattr(ingest, "get_uball_client", lambda: None)  # no register creds

    ingest.run_ingestion(_fb_with_settings(True), _cfg(tmp_path),
                         "pid3", state, stopped, None)

    assert len(transcoded) == 2
    assert len(uploads) == 2
    for _, key in uploads:
        assert not key.endswith("_4K.mp4")
