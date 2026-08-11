"""Mark stale ``status='stopped'`` recording-sessions with no ``s3Prefix``
as ``status='failed'`` so the pipeline-start endpoint stops considering them.

Why these exist
---------------
The pipeline-start endpoint at ``main.py`` collects sessions with
``status='stopped' AND s3Prefix=None`` as upload candidates. When the camera
that owns one of those sessions is no longer reachable (powered off, on a
different network, swapped out), the upload silently fails and the session
stays in that state forever. Each subsequent pipeline run still picks them up
in the candidate list and walks past them — they don't actually get uploaded
because the GoPro isn't there, but they pollute logs and slow down the
upload-phase decision.

The pipeline-start endpoint already filters by a ``startedAt < now-96h``
age cutoff, so these old sessions are *skipped* but not *cleaned*. This
script flips them from ``stopped`` to ``failed`` so they fall out of the
candidate set entirely. Idempotent — re-running is a no-op once flipped.

Usage
-----
    # Dry run (default): just lists what would change.
    python3 scripts/mark_orphan_sessions_failed.py

    # Apply the change.
    python3 scripts/mark_orphan_sessions_failed.py --apply

    # Tune the age cutoff (default: 96 hours).
    python3 scripts/mark_orphan_sessions_failed.py --apply --min-age-hours 168

This script is safe to add to a periodic cron / weekly cleanup task.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import firebase_admin
from firebase_admin import credentials, firestore


def _resolve_credentials_path() -> str:
    explicit = os.environ.get("FIREBASE_CREDENTIALS_PATH")
    if explicit and Path(explicit).is_file():
        return explicit
    here = Path(__file__).resolve().parent.parent
    fallback = here / "uball-gopro-fleet-firebase-adminsdk.json"
    if fallback.is_file():
        return str(fallback)
    raise FileNotFoundError(
        "Firebase admin credentials not found. Set FIREBASE_CREDENTIALS_PATH "
        f"or place the JSON at {fallback}."
    )


def _parse_started_at(raw: Any) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def find_orphan_sessions(
    db: firestore.Client, min_age_hours: int
) -> List[Tuple[str, Dict[str, Any]]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    query = db.collection("recording-sessions").where("status", "==", "stopped")
    orphans: List[Tuple[str, Dict[str, Any]]] = []
    for doc in query.stream():
        data = doc.to_dict() or {}
        if data.get("s3Prefix"):
            continue  # already uploaded
        started_at = _parse_started_at(data.get("startedAt"))
        if started_at is None or started_at >= cutoff:
            continue  # too recent — probably still in flight
        orphans.append((doc.id, data))
    orphans.sort(key=lambda x: x[1].get("startedAt") or "")
    return orphans


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write the status change (default: dry-run).",
    )
    parser.add_argument(
        "--min-age-hours",
        type=int,
        default=96,
        help="Only flip sessions whose startedAt is older than this many hours (default: 96).",
    )
    args = parser.parse_args()

    cred = credentials.Certificate(_resolve_credentials_path())
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    orphans = find_orphan_sessions(db, args.min_age_hours)
    if not orphans:
        print("No orphan sessions found (status=stopped, no s3Prefix, age>cutoff).")
        return 0

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] Found {len(orphans)} orphan session(s) older than "
        f"{args.min_age_hours}h:"
    )
    for sid, data in orphans:
        print(
            f"  {data.get('jetsonId','?'):9s} {data.get('angleCode','?'):3s} "
            f"id={sid:24s} startedAt={data.get('startedAt')} "
            f"chapters={len(data.get('chapterFiles') or [])}"
        )

    if not args.apply:
        print("\n(dry-run; pass --apply to write status='failed')")
        return 0

    for sid, _ in orphans:
        db.collection("recording-sessions").document(sid).update(
            {
                "status": "failed",
                "failureReason": (
                    "Orphan session: status='stopped' with no s3Prefix and "
                    "GoPro never came back online. Auto-marked failed by "
                    "scripts/mark_orphan_sessions_failed.py to remove from "
                    "pipeline upload-candidate list."
                ),
                "failedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
    print(f"\nMarked {len(orphans)} session(s) as status='failed'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
