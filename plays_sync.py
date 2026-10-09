"""Create annotation-tool plays ("cards") from Firebase game logs.

This module is shared between the manual sync endpoint in `main.py` and the
automatic sync inside `video_processing.py`. It lives in its own module so both
callers can import it without introducing a circular dependency.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from logging_service import get_logger

logger = get_logger("gopro.plays_sync")


# Points → canonical annotation-tool classification. These MUST match the
# vocabulary the box score / UBall sync / chatbot expect: a 2-point make is
# FG_MAKE and a free throw is FREE_THROW_MAKE. (The old 2PT_MAKE/FT_MAKE were
# wrong and polluted the plays table; 4-pointers were dropped entirely.)
_MAKE_BY_POINTS: Dict[int, str] = {1: "FREE_THROW_MAKE", 2: "FG_MAKE", 3: "3PT_MAKE", 4: "4PT_MAKE"}

SHOT_LABELS: Dict[str, str] = {
    "FG_MAKE": "Field Goal Made", "FG_MISS": "Field Goal Missed",
    "3PT_MAKE": "3-Pointer Made", "3PT_MISS": "3-Pointer Missed",
    "4PT_MAKE": "4-Pointer Made", "4PT_MISS": "4-Pointer Missed",
    "FREE_THROW_MAKE": "Free Throw Made", "FREE_THROW_MISS": "Free Throw Missed",
    "FOUL": "Foul", "TIPOFF": "Tipoff",
}

# Resolve the play's camera side (LEFT/RIGHT) from scoring team + period +
# startingSideTeam1 — the same halftime-aware math the highlight clips use.
# Imported defensively so this module still loads in the legacy (non-AGX) env.
try:
    from agx_pipeline.side_attribution import scoring_hoop_side as _scoring_hoop_side
except Exception:  # pragma: no cover - legacy env without agx_pipeline
    _scoring_hoop_side = None


def create_plays_from_firebase_logs(
    client: Any,
    uball_game_id: str,
    firebase_game: Dict[str, Any],
    summary: Optional[Dict[str, Any]] = None,
) -> int:
    """Create plays in the annotation tool from a Firebase game's logs.

    Idempotent: if the annotation-tool game already has any plays, this is a
    no-op (returns 0) so re-runs of the pipeline do not duplicate cards.

    Args:
        client: A `UballClient` instance with `list_plays()` and `create_play()`.
        uball_game_id: The annotation-tool game UUID.
        firebase_game: The Firebase `basketball-games/{id}` document.

    Returns:
        Number of plays newly created (0 if skipped or none produced).
    """
    if not uball_game_id:
        return 0

    logs = firebase_game.get("logs", []) or []
    if not logs:
        return 0

    try:
        existing = client.list_plays(uball_game_id)
        if existing:
            logger.info(
                f"[PlaysSync] Game {uball_game_id} already has {len(existing)} plays — skipping."
            )
            if summary is not None:
                summary.update({"created": 0, "with_players": 0,
                                "by_label": {}, "skipped_existing": True})
            return 0
    except Exception as exc:
        logger.warning(
            f"[PlaysSync] list_plays check failed for {uball_game_id}: {exc} — proceeding anyway."
        )

    created_at_raw = firebase_game.get("createdAt")
    if not created_at_raw:
        logger.warning(f"[PlaysSync] Firebase game missing createdAt — cannot compute play timestamps")
        return 0
    game_start = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))

    left_name = firebase_game.get("leftTeam", {}).get("name", "Team 1")
    right_name = firebase_game.get("rightTeam", {}).get("name", "Team 2")

    created = 0
    with_players = 0
    by_label: Dict[str, int] = {}
    for log in logs:
        action = log.get("actionType", "")
        payload = log.get("payload", {}) or {}
        team_side = log.get("team")

        # Player pre-fill: only a player tap (player_score_added) carries a
        # player; a team-button score (score_added) does not, and the annotator
        # fills it in later — but the card + clip are already placed for them.
        player_id = None
        player_name = None
        if action in ("score_added", "player_score_added"):
            classification = _MAKE_BY_POINTS.get(payload.get("points", 0))
            if not classification:
                continue  # 0-point / malformed score log
            if action == "player_score_added":
                player_id = payload.get("playerId")
                player_name = payload.get("playerName")
        elif action == "foul_added":
            classification = "FOUL"
        elif action == "game_started":
            classification = "TIPOFF"
        else:
            continue

        if team_side == "left":
            team = "team1"
            team_name = left_name
        elif team_side == "right":
            team = "team2"
            team_name = right_name
        else:
            team = None
            team_name = "Game"

        log_ts_raw = log.get("timestamp")
        if not log_ts_raw:
            continue
        log_time = datetime.fromisoformat(log_ts_raw.replace("Z", "+00:00"))
        ts = (log_time - game_start).total_seconds()
        start_ts = max(0.0, ts - 5.0)
        end_ts = ts + 3.0

        label = SHOT_LABELS.get(classification, classification)

        # Camera side (LEFT/RIGHT) the play happened at — flips at halftime.
        # Same math as the highlight clips; None if the resolver isn't available
        # (legacy env) or the event has no team (e.g. tipoff).
        angle = None
        if _scoring_hoop_side and team_side in ("left", "right"):
            hoop = _scoring_hoop_side(team_side, log.get("period"),
                                      firebase_game.get("startingSideTeam1"))
            angle = hoop.upper() if hoop else None

        confidence = 1.0  # operator-entered scoreboard truth
        note = f"{player_name or team_name} — {label}"

        play_data: Dict[str, Any] = {
            "game_id": uball_game_id,
            "classification": classification,
            "note": note,
            "timestamp_seconds": ts,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            # source tags the card's origin so annotators can tell auto-seeded
            # scoreboard plays ("firebase") from their own ("manual").
            "source": "firebase",
            "events": [{
                "label": classification,
                "playerA": player_name, "playerAId": player_id,
                "playerB": None, "playerBId": None,
                "confidence": confidence,
            }],
        }
        if team:
            play_data["team"] = team
        if angle:
            play_data["angle"] = angle
        if player_id or player_name:
            play_data["player_a"] = player_name
            play_data["player_a_id"] = player_id
        if confidence is not None:
            play_data["confidence"] = confidence

        try:
            client.create_play(play_data)
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
            if player_id:
                with_players += 1
        except Exception as exc:
            logger.warning(
                f"[PlaysSync] Failed to create play ({classification} at {ts:.1f}s) "
                f"for game {uball_game_id}: {exc}"
            )

    logger.info(f"[PlaysSync] Created {created}/{len(logs)} plays for game {uball_game_id}")
    if summary is not None:
        summary.update({"created": created, "with_players": with_players,
                        "by_label": by_label, "skipped_existing": False})
    return created


_HOOP_ANGLE = {"left": "LEFT", "right": "RIGHT"}


# Written on every CV card. It is a PLACEHOLDER, not a measurement: the value is
# identical for a textbook shot and a marginal one, so nothing downstream can
# rank or threshold on it. The annotation editor shows a green/red flag off this
# field, so while it stays constant every CV card flags the same way. Giving it
# real signal means joining the typing verdict (cv_points.{logId}, which carries
# trust/STRICT-UNKNOWN) onto the card at ingest.
CV_PLACEHOLDER_CONFIDENCE = 0.5

# The typing stage's zone -> the annotation vocabulary, for a made and a missed
# shot. Without this join a CV card says only FG_MAKE/FG_MISS, even though the
# chain already worked out 2/3/4PT and free throws at 96% accuracy (measured on
# cb9e1294, 2026-09-22) and wrote it to cv_points.{logId} on the game doc.
_ZONE_CLASS = {
    ("2PT", True): "FG_MAKE",              ("2PT", False): "FG_MISS",
    ("3PT", True): "3PT_MAKE",             ("3PT", False): "3PT_MISS",
    ("4PT", True): "4PT_MAKE",             ("4PT", False): "4PT_MISS",
    ("FREE_THROW", True): "FREE_THROW_MAKE",
    ("FREE_THROW", False): "FREE_THROW_MISS",
}


def _typing_verdict(cv_points: Dict[str, Any], shot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The typing verdict for this shot, or None.

    Keyed the way shot_detect/live.py builds it: cv_{int(wallclock epoch)}_{side}.
    Only MAKES have one — a highlight clip, and therefore a typing pass, is only
    cut for a made shot — so misses keep the generic label.
    """
    side, wc = shot.get("side"), shot.get("wallclock")
    if not (cv_points and side and wc):
        return None
    try:
        epoch = int(datetime.fromisoformat(wc).timestamp())
    except Exception:  # noqa: BLE001
        return None
    v = cv_points.get(f"cv_{epoch}_{side}")
    return v if isinstance(v, dict) else None


# Same vote thresholds as the tracker's own (possession_tracker/who_eval.speak).
_WHO_V_MIN = 1.5
_WHO_V_RATIO = 1.5


def _who_suggestion(v: Optional[Dict[str, Any]], team: Optional[str],
                    rosters: Dict[str, Dict[str, str]]) -> Optional[tuple]:
    """(number, player name or None) to suggest on a CV card, or None.

    With the shooting team known and its roster available, the tracker's stored jersey votes are
    re-voted over that roster's numbers only (a read of an opponent's number is dropped) and the
    player's name comes with it. Without them, the tracker's own number is kept as is."""
    if not v:
        return None
    roster = rosters.get(team) if team in ("left", "right") else None
    votes = v.get("who_votes")
    if roster and isinstance(votes, dict) and votes:
        ok = sorted(((str(n), float(w)) for n, w in votes.items() if str(n) in roster), key=lambda x: -x[1])
        if not ok or ok[0][1] < _WHO_V_MIN or (len(ok) > 1 and ok[0][1] < _WHO_V_RATIO * ok[1][1]):
            return None
        return ok[0][0], roster[ok[0][0]]
    return (str(v["who"]), None) if v.get("who") else None


def rosters_from_annotation_game(game: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """{"left": {jersey: name}, "right": {...}} from an annotation game (team1 = left)."""
    out: Dict[str, Dict[str, str]] = {}
    for side, key in (("left", "roster_team1"), ("right", "roster_team2")):
        r = {}
        for p in (game or {}).get(key) or []:
            if p.get("jersey_number") is not None and p.get("name"):
                r[str(p["jersey_number"])] = str(p["name"])
        if r:
            out[side] = r
    return out


def _shot_key(s: Dict[str, Any]) -> Optional[str]:
    """cv_<epoch>_<side> for a shot_live shot (the id highlights / cv_points use)."""
    try:
        return f"cv_{int(datetime.fromisoformat(s['wallclock']).timestamp())}_{s['side']}"
    except Exception:  # noqa: BLE001
        return None


def _game_context(firebase_game: Dict[str, Any]) -> Dict[str, Any]:
    """Whole-game facts each card's flag needs: shots inside a shooting burst, paused clock."""
    from agx_pipeline.card_flag import burst_shots, paused_intervals
    shots = (firebase_game.get("shot_live") or {}).get("shots") or []
    keys = [k for k in (_shot_key(s) for s in shots) if k]
    return {"burst": burst_shots(keys), "paused": paused_intervals(firebase_game.get("logs") or [])}


def _cv_card(s: Dict[str, Any], firebase_game: Dict[str, Any], cv_points: Dict[str, Any],
             game_start: Optional[datetime], uball_game_id: str,
             rosters: Optional[Dict[str, Dict[str, str]]],
             ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The annotation card for one detected shot (shared by card creation and the later refresh):
    type from the tracker, team, roster-filtered jersey suggestion, and the green/yellow/red flag
    with its reason on the first event, plus a fingerprint of what was written (`cv_written`) so
    a refresh can tell a card an annotator has since changed."""
    typed = False
    classification = "FG_MAKE" if s.get("made") else "FG_MISS"
    confidence = CV_PLACEHOLDER_CONFIDENCE
    # Upgrade to the real shot type when typing reached a verdict, and carry
    # ITS confidence rather than the flat placeholder — that number is what
    # the editor's green/red flag reads.
    _v = _typing_verdict(cv_points, s)
    if _v:
        _typed = _ZONE_CLASS.get((_v.get("zone"), bool(s.get("made"))))
        if _typed:
            classification = _typed
            typed = True
            try:
                confidence = float(_v.get("confidence", CV_PLACEHOLDER_CONFIDENCE))
            except (TypeError, ValueError):
                pass
    # Video-timeline seconds: prefer wallclock - game_start; fall back to the
    # segment offset. Approximate (SL/SR vs tracking-cam sync) — the annotator
    # nudges it; the card + rough position is what saves them the work.
    ts = None
    # `video_ts` is the rebuilt time: the rim crossing found on the SL/SR
    # master, in video seconds, cut straight from cross_frame/measured_fps.
    # Prefer it over everything else — the two fallbacks below are the same
    # arithmetic that put clips minutes from their shot, so a card built on
    # them lands just as wrong. Scored against a hand-annotated game, the
    # rebuilt times sit within 0.78s of what a human marked.
    if s.get("video_ts") is not None:
        try:
            ts = float(s["video_ts"])
        except (TypeError, ValueError):
            ts = None
    wc = s.get("wallclock")
    if ts is None and wc and game_start:
        try:
            ts = (datetime.fromisoformat(wc) - game_start).total_seconds()
        except Exception:  # noqa: BLE001
            ts = None
    if ts is None:
        ts = float(s.get("seg", 0)) * 4.0 + float(s.get("t_shot", 0.0))
    ts = max(0.0, ts)
    angle = _HOOP_ANGLE.get(s.get("side"))
    label = SHOT_LABELS.get(classification, classification)
    note = f"CV: {label}" + (f" ({s.get('cam')} · {s.get('side')} rim)" if s.get("cam") else "")
    # Jersey SUGGESTION from the possession tracker (cv_points.{id}.who): shown in the note
    # only — the player field stays the annotator's call. Validated at 83% right when it
    # speaks on 395 annotated shots, so it is a hint, never a fill.
    # Team from the tracker's half-time switch (basket side + time), makes AND misses — the
    # score goes to the team even when no player is named.
    _team = None
    try:
        from agx_pipeline.team_assign import team_for_shot
        _ep = datetime.fromisoformat(s["wallclock"]).timestamp() if s.get("wallclock") else None
        _team = team_for_shot(firebase_game.get("tracker_teams"), s.get("side"), _ep)
    except Exception:  # noqa: BLE001
        _team = None
    # Only numbers on the SHOOTING team's roster are suggested, with the player's name
    # (unseen games: right 81% when it speaks vs 78% on any number).
    _who = _who_suggestion(_v, _team, rosters or {})
    if _who:
        note += f" · Tracker suggests #{_who[0]}" + (f" {_who[1]}" if _who[1] else "")

    play_data: Dict[str, Any] = {
        "game_id": uball_game_id,
        "classification": classification,
        "note": note,
        "timestamp_seconds": ts,
        "start_timestamp": max(0.0, ts - 5.0),
        "end_timestamp": ts + 3.0,
        "source": "cv",
        "confidence": confidence,
        "events": [{
            "label": classification,
            "playerA": None, "playerAId": None,
            "playerB": None, "playerBId": None,
            "confidence": confidence,
        }],
    }
    if angle:
        play_data["angle"] = angle
    # No `team` on a CV card: the annotation backend uses a play's team ONLY to add a make's points
    # to the game's official score (it is not stored on the play), and CV predictions -- warm-ups
    # included -- must never change that score. The team still picks the roster above.


    from agx_pipeline.card_flag import CONFIDENCE, card_flag, is_paused
    ctx = ctx or {}
    _key = _shot_key(s)
    try:
        _ep = datetime.fromisoformat(s["wallclock"]).timestamp() if s.get("wallclock") else None
    except Exception:  # noqa: BLE001
        _ep = None
    flag, why = card_flag(_v, _who, paused=is_paused(ctx.get("paused"), _ep),
                          burst=bool(_key) and _key in (ctx.get("burst") or set()))
    play_data["confidence"] = CONFIDENCE[flag]
    play_data["events"][0].update({"confidence": CONFIDENCE[flag], "cv_flag": flag, "cv_flag_reason": why})
    play_data["note"] += f" · {flag.upper()}: {why}"
    play_data["events"][0]["cv_written"] = _card_fingerprint(play_data)
    play_data["_typed"] = typed
    return play_data


def _card_fingerprint(card: Dict[str, Any]) -> str:
    """What an annotator would change: type, note, player, time. Equal = untouched since written."""
    import hashlib
    key = "|".join(str(card.get(k) or "") for k in ("classification", "note", "player_a_id"))
    key += "|%.1f" % float(card.get("timestamp_seconds") or 0)
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def refresh_cv_cards(client: Any, uball_game_id: str, firebase_game: Dict[str, Any],
                     rosters: Optional[Dict[str, Dict[str, str]]] = None,
                     dry_run: bool = False) -> Dict[str, int]:
    """Bring a game's CV cards up to date with the tracker's later results (type, jersey
    suggestion, green/yellow/red flag). Cards are created at ingest, often before the tracker has
    finished that game; this rewrites each card ONLY while it is exactly as the pipeline left it
    (its `cv_written` fingerprint still matches), so nothing an annotator changed is touched.
    Cards made before flags existed carry no fingerprint and are left alone too."""
    stats = {"cards": 0, "updated": 0, "unchanged": 0, "edited_by_annotator": 0, "no_fingerprint": 0, "no_shot": 0}
    if not uball_game_id:
        return stats
    shots = (firebase_game.get("shot_live") or {}).get("shots") or []
    created_at_raw = firebase_game.get("createdAt")
    game_start = (datetime.fromisoformat(created_at_raw.replace("Z", "+00:00")) if created_at_raw else None)
    cv_points = firebase_game.get("cv_points") or {}
    fresh = {}
    ctx = _game_context(firebase_game)
    for sh in shots:
        c = _cv_card(sh, firebase_game, cv_points, game_start, uball_game_id, rosters, ctx)
        c.pop("_typed", None)
        fresh[(c.get("angle"), round(float(c["timestamp_seconds"]), 1))] = c
    for p in client.list_plays(uball_game_id):
        if (p.get("source") or "") != "cv":
            continue
        stats["cards"] += 1
        ev = (p.get("events") or [{}])[0] or {}
        new = fresh.get((p.get("angle"), round(float(p.get("timestamp_seconds") or 0), 1)))
        if new is None:
            stats["no_shot"] += 1
            continue
        if not ev.get("cv_written"):
            stats["no_fingerprint"] += 1
            continue
        if ev["cv_written"] != _card_fingerprint(p):
            stats["edited_by_annotator"] += 1
            continue
        fields = {k: new[k] for k in ("classification", "note", "confidence", "events")}
        if all(p.get(k) == v for k, v in fields.items()):
            stats["unchanged"] += 1
            continue
        if not dry_run:
            try:
                client.update_play(p["id"], fields)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[PlaysSync/CV] refresh of card {p.get('id')} failed: {exc}")
                continue
        stats["updated"] += 1
    logger.info(f"[PlaysSync/CV] refresh {uball_game_id}: {stats}")
    return stats


def create_plays_from_shot_live(
    client: Any,
    uball_game_id: str,
    firebase_game: Dict[str, Any],
    dry_run: bool = False,
    summary: Optional[Dict[str, Any]] = None,
    rosters: Optional[Dict[str, Dict[str, str]]] = None,
) -> int:
    """Create annotation cards from the CV's `shot_live` shadow — every make/miss
    the high-fps SL/SR detector saw, independent of the scorekeeper.

    Tagged `source="cv"` so annotators can tell these from scoreboard cards AND so
    this is idempotent on CV cards (re-runs won't duplicate). The CV knows the hoop
    (angle LEFT/RIGHT) and make/miss but NOT the point value (→ FG_MAKE/FG_MISS,
    2pt assumed) or the team (left for the annotator).

    Detection — a card at the right moment with the rim and make/miss — measures
    ~99%. The earlier "~65% recall / ~80% precision" noted here was a stale
    shadow-mode figure and is not what this stage does.

    Every card is written with CV_PLACEHOLDER_CONFIDENCE: see the constant. The
    point VALUE (2/3/4) is produced by the typing stage and lands separately on
    the Firebase game doc at `cv_points.{logId}` — it is not joined onto these
    cards yet, which is why the classification below is only make/miss.

    `dry_run` counts without writing. Returns the number created (or would create).
    """
    if not uball_game_id:
        return 0
    live = firebase_game.get("shot_live") or {}
    shots = live.get("shots") or []
    if not shots:
        return 0

    # Idempotency: skip if this game already has CV cards.
    try:
        existing = client.list_plays(uball_game_id)
        if any((p.get("source") == "cv") for p in existing):
            logger.info(f"[PlaysSync/CV] Game {uball_game_id} already has CV cards — skipping.")
            if summary is not None:
                summary.update({"created": 0, "by_label": {}, "skipped_existing": True})
            return 0
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[PlaysSync/CV] list_plays check failed for {uball_game_id}: {exc}")

    created_at_raw = firebase_game.get("createdAt")
    game_start = (datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                  if created_at_raw else None)

    created = 0
    by_label: Dict[str, int] = {}
    cv_points = firebase_game.get("cv_points") or {}
    n_typed = 0
    ctx = _game_context(firebase_game)
    for s in shots:
        play_data = _cv_card(s, firebase_game, cv_points, game_start, uball_game_id, rosters, ctx)
        classification = play_data["classification"]
        n_typed += bool(play_data.pop("_typed", False))
        ts = play_data["timestamp_seconds"]
        if dry_run:
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
            continue
        try:
            client.create_play(play_data)
            created += 1
            by_label[classification] = by_label.get(classification, 0) + 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[PlaysSync/CV] create_play failed ({classification} @ {ts:.1f}s): {exc}")

    logger.info(f"[PlaysSync/CV] {'(dry-run) ' if dry_run else ''}created {created}/{len(shots)} "
                f"CV cards for game {uball_game_id}")
    if summary is not None:
        summary.update({"created": created, "by_label": by_label,
                        "typed_from_cv_points": n_typed, "skipped_existing": False})
    return created
