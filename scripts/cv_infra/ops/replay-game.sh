#!/usr/bin/env bash
#
# Phase 5.1 / [UBA-220](https://linear.app/uball/issue/UBA-220) — replay CV against a past game's already-transcoded
# 1080p angle files. Writes CV detections to `basketball-games.{id}.
# cv_logs_staging[]` (shadow path) so production plays are not touched.
#
# Wraps the dispatcher's per-game POST with the right `emit_target` so
# operators don't have to remember the body shape.
#
# Why not `reprocess-game.sh`?
#   * reprocess-game.sh is for fixing a broken production run — it
#     prints SQL to delete plays where source='cv' + clears the
#     `cv_dispatched_at` markers. Replays don't want any of that —
#     production data must not be touched.
#   * The Flask dispatcher's per-game request bypasses the
#     `cv_dispatched_at` skip when `firebase_game_id` is set in the
#     body, so replays work even on games that already have a
#     production CV pass.
#
# Usage:
#   ./replay-game.sh <firebase_game_id>             # dry-run
#   ./replay-game.sh <firebase_game_id> --apply
#   ./replay-game.sh g1 g2 g3 --apply               # batch (one per line)
#
# Required env (defaults shown):
#   DISPATCH_URL    http://localhost:5000
#   EMIT_TARGET     cv_logs_staging   # never override unless you know
#                                     # why — switching to "logs" makes
#                                     # this a production run, not a replay
#
# Exit codes:
#   0 success / dry-run
#   2 pre-flight failed
#   5 dispatch trigger failed for any game

set -euo pipefail

DISPATCH_URL="${DISPATCH_URL:-http://localhost:5000}"
EMIT_TARGET="${EMIT_TARGET:-cv_logs_staging}"
APPLY=0
GAME_IDS=()

log()  { printf '\n\033[1;34m[replay-game]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[replay-game]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[replay-game]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

for arg in "$@"; do
  case "$arg" in
    --apply)   APPLY=1 ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *)         GAME_IDS+=("$arg") ;;
  esac
done

(( ${#GAME_IDS[@]} > 0 )) || fail "missing FIREBASE_GAME_ID (one or more positional args)" 2
command -v curl >/dev/null 2>&1 || fail "curl required" 2
command -v jq   >/dev/null 2>&1 || fail "jq required" 2

[[ "$EMIT_TARGET" == "cv_logs_staging" || "$EMIT_TARGET" == "logs" ]] \
  || fail "EMIT_TARGET must be 'cv_logs_staging' or 'logs', got: $EMIT_TARGET" 2

if [[ "$EMIT_TARGET" == "logs" ]]; then
  warn "EMIT_TARGET=logs — this is a PRODUCTION run, not a replay!"
  warn "Replays should always use cv_logs_staging. Aborting unless you confirm."
  if (( APPLY )); then
    read -p "Type 'CONFIRM-PRODUCTION' to continue: " confirm
    [[ "$confirm" == "CONFIRM-PRODUCTION" ]] || fail "aborted" 2
  fi
fi

log "dispatch_url: $DISPATCH_URL"
log "emit_target:  $EMIT_TARGET"
log "games:        ${#GAME_IDS[@]}  ($(printf '%s ' "${GAME_IDS[@]}"))"

if (( !APPLY )); then
  log "DRY-RUN — pass --apply to actually trigger dispatches."
fi

failed=0

for gid in "${GAME_IDS[@]}"; do
  if [[ ! "$gid" =~ ^[A-Za-z0-9_-]{8,40}$ ]]; then
    warn "skipping malformed game_id: $gid"
    failed=$((failed + 1))
    continue
  fi
  body="$(jq -nc --arg id "$gid" --arg target "$EMIT_TARGET" \
    '{firebase_game_id: $id, emit_target: $target, dry_run: false, limit: 1}')"
  log ""
  log "→ $gid"
  log "  POST $DISPATCH_URL/api/cv/dispatch-pending"
  log "  body: $body"

  if (( !APPLY )); then
    continue
  fi

  resp="$(curl -sS -X POST "$DISPATCH_URL/api/cv/dispatch-pending" \
    -H "Content-Type: application/json" -d "$body" || true)"
  if echo "$resp" | jq . >/dev/null 2>&1; then
    echo "$resp" | jq .
    dispatched="$(echo "$resp" | jq -r '.dispatched_count // 0')"
    if [[ "$dispatched" == "0" ]]; then
      reason="$(echo "$resp" | jq -r '.errors[0].error // "unknown"' 2>/dev/null || true)"
      warn "$gid: dispatched_count=0; reason: $reason"
      failed=$((failed + 1))
    fi
  else
    warn "$gid: non-JSON response from dispatcher: $resp"
    failed=$((failed + 1))
  fi
done

if (( APPLY && failed > 0 )); then
  fail "$failed game(s) failed to dispatch" 5
fi

log ""
log "Next steps:"
log "  1. Wait ~15-25 min for the fusion + merge Batch jobs to complete."
log "     Watch: aws batch list-jobs --job-queue cv-shot-detection-queue \\"
log "                --filters name=jobStatus,values=RUNNING --region us-east-1"
log "  2. Confirm cv_logs_staging[] populated on each Firebase game."
log "  3. Run the accuracy report against the human ground truth:"
log "       python3 scripts/cv_infra/ops/accuracy_report.py \\"
log "         --game-id <firebase-id> --supabase-game-id <uuid>"
log "  4. Aggregate per-game reports into a single client-facing summary "
log "     (UBA-223, follow-up)."
log ""
log "done"
