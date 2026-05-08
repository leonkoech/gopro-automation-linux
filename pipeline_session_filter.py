"""Session-list filter for the auto-pipeline starter.

Splits the candidate ``recording-sessions`` list into three buckets so
``_start_auto_pipeline_internal`` can decide which sessions actually go into
the new pipeline.

Why this lives in its own module: the same logic used to live inline inside
``main.py``, where it could not be unit-tested without importing Flask /
firebase / dotenv at module load.  A pure helper keeps the filter testable
and the rules explicit.

Rules (in order):
  1. ``status`` must be ``'stopped'`` — anything else (recording, failed,
     uploaded-and-purged, …) is ignored entirely.
  2. Empty sessions — ``totalChapters == 0`` AND no ``chapterFiles`` AND no
     ``s3Prefix`` — go into ``empty_sessions``.  Including these expands the
     pipeline's recording-time range and pulls unrelated games into the run.
  3. Sessions whose ``startedAt`` is older than ``age_cutoff`` go into
     ``stale_sessions``.  Sessions with a missing or unparseable
     ``startedAt`` are kept (defensive — better than silently dropping data).
  4. Everything else goes into ``sessions`` — these are the ones the pipeline
     will actually process.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Mapping, Tuple

# Default cutoff for "fresh enough to belong to this pipeline run".
DEFAULT_AGE_HOURS = 12


@dataclass(frozen=True)
class FilteredSessions:
    """Result of :func:`filter_pipeline_sessions`."""

    sessions: List[Mapping]       # eligible — feed these to the pipeline
    stale_sessions: List[Mapping]  # too old — skip
    empty_sessions: List[Mapping]  # 0 chapters, no s3Prefix — skip


def _is_empty_session(s: Mapping) -> bool:
    """A session is "empty" if it has nothing useful for the pipeline.

    Both upload paths require either chapter files (to upload) or a
    pre-existing ``s3Prefix`` (already uploaded).  Without either, the
    session is just a placeholder — typically a failed recording where the
    GoPro never produced files — and including it only widens the pipeline's
    time range.
    """
    total_chapters = s.get('totalChapters', 0) or 0
    chapter_files = len(s.get('chapterFiles') or [])
    has_s3_prefix = bool(s.get('s3Prefix'))
    return total_chapters == 0 and chapter_files == 0 and not has_s3_prefix


def _parse_started_at(started_at) -> datetime | None:
    """Coerce ``started_at`` to a tz-aware UTC datetime, or None on failure.

    Accepts:
      * ISO-8601 string (with or without trailing ``Z``).
      * Anything with a ``timestamp()`` method (Firestore DatetimeWithNanos,
        plain datetime, …).
    Naive datetimes are assumed to be UTC.
    """
    if started_at is None:
        return None
    try:
        if isinstance(started_at, str):
            return datetime.fromisoformat(started_at.replace('Z', '+00:00'))
        if hasattr(started_at, 'timestamp'):
            return started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return None


def filter_pipeline_sessions(
    all_sessions: Iterable[Mapping],
    age_cutoff: datetime,
) -> FilteredSessions:
    """Split ``all_sessions`` into (sessions, stale_sessions, empty_sessions).

    :param all_sessions: raw session docs from Firebase (any status).
    :param age_cutoff: tz-aware UTC datetime; sessions started before this
        go into ``stale_sessions``.
    """
    sessions: List[Mapping] = []
    stale_sessions: List[Mapping] = []
    empty_sessions: List[Mapping] = []

    for s in all_sessions:
        if s.get('status') != 'stopped':
            continue

        if _is_empty_session(s):
            empty_sessions.append(s)
            continue

        started = _parse_started_at(s.get('startedAt'))
        if started is None:
            # No usable startedAt → keep it; the pipeline may still recover
            # something useful, and silently dropping is worse than logging.
            sessions.append(s)
            continue

        if started >= age_cutoff:
            sessions.append(s)
        else:
            stale_sessions.append(s)

    return FilteredSessions(
        sessions=sessions,
        stale_sessions=stale_sessions,
        empty_sessions=empty_sessions,
    )


def session_label(s: Mapping) -> str:
    """Short human-readable label used in log lines."""
    angle = s.get('angleCode', '?')
    sid = (s.get('id', '?') or '?')[:8]
    return f"{angle}({sid})"


def format_skip_log(label: str, skipped: List[Mapping]) -> str:
    """Build the log message for a skipped-session bucket."""
    items = [session_label(s) for s in skipped]
    return f"[Pipeline] Skipping {len(skipped)} {label} sessions: {items}"
