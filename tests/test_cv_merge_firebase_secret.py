"""Unit tests for the Firebase Admin SDK Secrets Manager fetch in the
merge entrypoint (Phase 4 / UBA-219).

Focus areas:
  * Happy path — valid service-account JSON in the secret → written to /tmp,
    FIREBASE_CREDENTIALS_PATH points at it.
  * Placeholder rejection — secret with `"PLACEHOLDER"` marker raises a
    descriptive error so the operator gets a clear message.
  * JSON parse error → descriptive RuntimeError.
  * Non-service-account JSON (wrong `type`) → descriptive RuntimeError.
  * LOCAL_MODE + existing FIREBASE_CREDENTIALS_PATH → fetch skipped
    (lets dev runs work without AWS creds).

All AWS calls are stubbed; no boto3 credentials or network needed.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "deploy" / "cv-merge"))

# entrypoint imports `boto3` lazily inside the helper, so we don't need to
# stub it at module import time. The fixture below replaces it per-test.
import entrypoint as ep  # noqa: E402


# ---------------------------------------------------------------- helpers
def _valid_sa_json() -> str:
    return json.dumps({
        "type": "service_account",
        "project_id": "uball-gopro-fleet",
        "private_key_id": "abc123",
        "private_key": "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
        "client_email": "firebase-adminsdk@uball-gopro-fleet.iam.gserviceaccount.com",
    })


def _placeholder_json() -> str:
    return json.dumps({"PLACEHOLDER": "fill via aws secretsmanager put-secret-value"})


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch, tmp_path):
    """Clear cred-related env vars + redirect /tmp/firebase-admin.json into
    a tmp dir so tests don't write to the host filesystem."""
    for k in (
        "LOCAL_MODE", "DRY_RUN",
        "FIREBASE_CREDENTIALS_PATH", "FIREBASE_ADMIN_SECRET_ID",
        "AWS_REGION",
    ):
        monkeypatch.delenv(k, raising=False)
    # Redirect the dest path to a temp dir.
    monkeypatch.setattr(ep, "_FIREBASE_CRED_LOCAL_PATH",
                        str(tmp_path / "firebase-admin.json"))


@pytest.fixture
def stub_boto3(monkeypatch):
    """Replace boto3.client('secretsmanager') with a controllable mock."""
    captured = {"calls": []}
    mock_client = MagicMock()

    def _get_secret_value(SecretId, **_):
        captured["calls"].append(SecretId)
        return captured["next_response"]

    mock_client.get_secret_value.side_effect = _get_secret_value

    class _Boto3Module:
        @staticmethod
        def client(svc, region_name=None):
            assert svc == "secretsmanager"
            return mock_client

    monkeypatch.setitem(sys.modules, "boto3", _Boto3Module)
    return captured


# ---------------------------------------------------------------- happy path
def test_happy_path_writes_creds_and_sets_env(stub_boto3, monkeypatch):
    stub_boto3["next_response"] = {"SecretString": _valid_sa_json()}

    ep._hydrate_firebase_creds_from_secrets_manager()

    # Default secret name was used.
    assert stub_boto3["calls"] == ["uball/firebase-admin-cv-merge"]

    # Env points at our tmp file.
    path = os.environ.get("FIREBASE_CREDENTIALS_PATH")
    assert path is not None
    assert Path(path).exists()

    # File contains the JSON we returned.
    assert json.loads(Path(path).read_text())["type"] == "service_account"

    # File perms restrict to user.
    mode = Path(path).stat().st_mode & 0o777
    assert mode == 0o600


def test_respects_FIREBASE_ADMIN_SECRET_ID_override(stub_boto3, monkeypatch):
    monkeypatch.setenv("FIREBASE_ADMIN_SECRET_ID", "uball/firebase-admin-test")
    stub_boto3["next_response"] = {"SecretString": _valid_sa_json()}

    ep._hydrate_firebase_creds_from_secrets_manager()

    assert stub_boto3["calls"] == ["uball/firebase-admin-test"]


# ---------------------------------------------------------------- rejections
def test_placeholder_marker_raises_with_actionable_message(stub_boto3):
    stub_boto3["next_response"] = {"SecretString": _placeholder_json()}

    with pytest.raises(RuntimeError) as exc:
        ep._hydrate_firebase_creds_from_secrets_manager()

    msg = str(exc.value)
    assert "placeholder" in msg.lower()
    assert "put-secret-value" in msg
    assert "uball/firebase-admin-cv-merge" in msg


def test_invalid_json_raises(stub_boto3):
    stub_boto3["next_response"] = {"SecretString": "not json {{{ }"}

    with pytest.raises(RuntimeError) as exc:
        ep._hydrate_firebase_creds_from_secrets_manager()

    assert "not valid JSON" in str(exc.value)


def test_non_service_account_raises(stub_boto3):
    stub_boto3["next_response"] = {"SecretString": json.dumps({"type": "user_account"})}

    with pytest.raises(RuntimeError) as exc:
        ep._hydrate_firebase_creds_from_secrets_manager()

    assert "service" in str(exc.value).lower()


def test_empty_secret_string_raises(stub_boto3):
    stub_boto3["next_response"] = {"SecretString": ""}

    with pytest.raises(RuntimeError) as exc:
        ep._hydrate_firebase_creds_from_secrets_manager()

    assert "placeholder" in str(exc.value).lower() or "valid" in str(exc.value).lower()


# ---------------------------------------------------------------- LOCAL_MODE
def test_local_mode_with_existing_file_skips_fetch(monkeypatch, stub_boto3, tmp_path):
    local_creds = tmp_path / "local-firebase.json"
    local_creds.write_text(_valid_sa_json())
    monkeypatch.setenv("LOCAL_MODE", "true")
    monkeypatch.setenv("FIREBASE_CREDENTIALS_PATH", str(local_creds))

    ep._hydrate_firebase_creds_from_secrets_manager()

    # No Secrets Manager call should have been made.
    assert stub_boto3["calls"] == []
    # FIREBASE_CREDENTIALS_PATH still points at the local file.
    assert os.environ["FIREBASE_CREDENTIALS_PATH"] == str(local_creds)


def test_local_mode_without_existing_file_still_fetches(monkeypatch, stub_boto3):
    monkeypatch.setenv("LOCAL_MODE", "true")
    # No FIREBASE_CREDENTIALS_PATH set — fetch should still happen because
    # there's no usable local fallback.
    stub_boto3["next_response"] = {"SecretString": _valid_sa_json()}

    ep._hydrate_firebase_creds_from_secrets_manager()

    assert stub_boto3["calls"] == ["uball/firebase-admin-cv-merge"]


def test_dry_run_with_existing_file_skips_fetch(monkeypatch, stub_boto3, tmp_path):
    local_creds = tmp_path / "dry-firebase.json"
    local_creds.write_text(_valid_sa_json())
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("FIREBASE_CREDENTIALS_PATH", str(local_creds))

    ep._hydrate_firebase_creds_from_secrets_manager()

    assert stub_boto3["calls"] == []
