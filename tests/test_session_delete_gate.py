"""The masters-delete gate. This is the one irreversible step in ingestion.

The case that matters is the first one: a run where every transcode failed
uploads nothing, and the old guard -- `all(safe(r) for r in tr.values())` --
returned True for that empty set and deleted the footage. Moonlight vs
Locksmith (5 angles, ~40 GB) was destroyed that way on 2026-08-29.
"""
import pytest

from agx_pipeline.ingest import session_delete_ok


def test_empty_ingest_never_deletes():
    """Nothing uploaded is NOT success, however the rest of the run reported.

    This is the regression. `all()` over an empty sequence is True, so the old
    guard passed here and removed the only copy of the game.
    """
    assert session_delete_ok(0, 0, registered=True) is False


def test_all_uploaded_and_registered_deletes():
    assert session_delete_ok(3, 3, registered=True) is True


def test_partial_upload_keeps_masters():
    """One angle missing from S3 means the game is not safely stored."""
    assert session_delete_ok(3, 2, registered=True) is False


def test_uploaded_but_unregistered_keeps_masters():
    """Footage in S3 that no annotation game points at is unusable footage.

    This is the Win-or-Booze-vs-Anti-Sandbaggers shape: everything uploaded,
    the annotation game later absent, nobody able to see it.
    """
    assert session_delete_ok(3, 3, registered=False) is False


@pytest.mark.parametrize("n_files,n_uploaded", [(1, 0), (5, 4), (2, 1)])
def test_any_shortfall_keeps_masters(n_files, n_uploaded):
    assert session_delete_ok(n_files, n_uploaded, registered=True) is False
