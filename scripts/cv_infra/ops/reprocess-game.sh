#!/usr/bin/env bash
#
# Reprocess a CV pipeline run for a single game. Wraps the manual 3-step
# recipe from `docs/CV_PIPELINE_RUNBOOK.md` section 1:
#
#   1. DELETE plays from Supabase where game_id matches AND source='cv'
#   2. Clear CV-dispatch markers on the Firebase basketball-games doc
#      (cv_dispatched_at, cv_emitted_at, cv_*_job_id, cv_emit_target)
#   3. Trigger a single-game dispatch via the Flask /api/cv/dispatch-pending
#      endpoint
#
# This is a dry-run by default — pass --apply to actually mutate state.
#
# Required env (or pass as flags):
#   FIREBASE_GAME_ID    Firebase basketball-games doc ID for the failed game
#                       (or pass as the first positional arg)
#
#   DISPATCH_URL        Default: http://localhost:5000
#                       Override to the production gopro-automation-linux
#                       host when re-running remotely.
#
#   SUPABASE_URL        Default: read from `.env`
#   SUPABASE_KEY        Default: read from `.env`
#                       (used by the `psql` call to delete CV plays)
#
# Usage:
#   ./reprocess-game.sh <firebase_game_id>           # dry-run
#   ./reprocess-game.sh <firebase_game_id> --apply   # mutate state
#   ./reprocess-game.sh --help
#
# Exit codes:
#   0  success / dry-run completed
#   2  pre-flight (missing tool / arg / env)
#   3  Supabase delete failed
#   4  Firebase clear failed
#   5  dispatch trigger failed

set -euo pipefail

GAME_ID=""
APPLY=0
DISPATCH_URL="${DISPATCH_URL:-http://localhost:5000}"

log()  { printf '\n\033[1;34m[reprocess-game]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[reprocess-game]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[reprocess-game]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

for arg in "$@"; do
  case "$arg" in
    --apply)   APPLY=1 ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *)         GAME_ID="$arg" ;;
  esac
done

[[ -n "$GAME_ID" ]] || fail "missing FIREBASE_GAME_ID (positional arg)" 2
command -v curl >/dev/null 2>&1 || fail "curl required" 2
command -v jq   >/dev/null 2>&1 || fail "jq required" 2

# Validate the game_id shape (28 chars per Firebase convention, alnum).
# Loose check — Firebase IDs are typically 20 chars but we accept 8-32 here.
if [[ ! "$GAME_ID" =~ ^[A-Za-z0-9_-]{8,40}$ ]]; then
  fail "FIREBASE_GAME_ID looks malformed: $GAME_ID" 2
fi

if (( APPLY )); then
  log "APPLY mode — will mutate Supabase + Firebase + trigger dispatch"
else
  log "DRY-RUN — pass --apply to actually run. Use Ctrl-C if anything below looks wrong."
fi
log "game_id: $GAME_ID"
log "dispatch_url: $DISPATCH_URL"

# ---------------------------------------------------------------------------
# Step 1 — DELETE CV plays from Supabase (manual via psql / SQL editor —
# we just print the SQL since reaching Supabase from a shell needs
# credentials we can't safely inline)
# ---------------------------------------------------------------------------
log ""
log "STEP 1 — delete CV plays from Supabase (manual; copy-paste below)"
cat <<SQL
  -- Run in the Supabase SQL editor for the Uball AI project (mhbrsftxvxxtfgbajrlc):
  DELETE FROM plays
   WHERE game_id = (SELECT id FROM games WHERE firebase_game_id = '$GAME_ID')
     AND source  = 'cv';

  -- Sanity check what got deleted:
  SELECT count(*) FROM plays
   WHERE game_id = (SELECT id FROM games WHERE firebase_game_id = '$GAME_ID')
     AND source  = 'cv';
  -- expected: 0
SQL

# ---------------------------------------------------------------------------
# Step 2 — clear Firebase CV markers
# ---------------------------------------------------------------------------
log ""
log "STEP 2 — clear Firebase CV markers on basketball-games/$GAME_ID"
log "Fields to delete:"
log "    cv_dispatched_at, cv_emitted_at, cv_emit_target"
log "    cv_fusion_a_job_id, cv_fusion_b_job_id, cv_merge_job_id"
log ""
log "Fastest path: open Firebase console → basketball-games/$GAME_ID → delete the 6 fields."
log "Or via gcloud firestore CLI / Admin SDK (requires Firebase admin creds)."

# ---------------------------------------------------------------------------
# Step 3 — trigger dispatch
# ---------------------------------------------------------------------------
log ""
log "STEP 3 — trigger dispatch via Flask endpoint"
body="$(jq -nc --arg id "$GAME_ID" '{firebase_game_id:$id, dry_run:false, limit:1}')"
log "POST $DISPATCH_URL/api/cv/dispatch-pending"
log "body: $body"

if (( APPLY )); then
  log ""
  resp="$(curl -sS -X POST "$DISPATCH_URL/api/cv/dispatch-pending" \
    -H "Content-Type: application/json" \
    -d "$body")"
  echo "$resp" | jq . || echo "$resp"
  dispatched="$(echo "$resp" | jq -r '.dispatched_count // 0')"
  if [[ "$dispatched" == "0" ]]; then
    warn "dispatched_count=0 — check 'errors', 'skipped_*', or 'waiting_on_angles' in the response"
    exit 5
  fi
  log ""
  log "DONE — track the Batch jobs:"
  log "  aws batch list-jobs --job-queue cv-shot-detection-queue --filters name=jobName,values=cv-*$GAME_ID* --region us-east-1"
else
  log ""
  log "DRY-RUN — re-run with --apply to actually POST to the dispatch endpoint."
fi
