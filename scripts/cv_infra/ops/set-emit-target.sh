#!/usr/bin/env bash
#
# Flip the cv-dispatch Lambda's CV_EMIT_TARGET env var between shadow
# (`cv_logs_staging`) and production (`logs`). Used during the Phase 7
# shadow rollout → cutover dance.
#
# Reads + merges the existing env so other variables (CV_FUSION_JOB_QUEUE,
# CV_FUSION_JOB_DEFINITION, etc.) are preserved — Lambda's
# update-function-configuration --environment overwrites the whole
# variable map, so we have to round-trip it.
#
# Required env / args:
#   New target (positional):  `cv_logs_staging` (shadow) | `logs` (production)
#   AWS_REGION                default: us-east-1
#   FUNCTION_NAME             default: uball-cv-dispatch
#
# Usage:
#   ./set-emit-target.sh cv_logs_staging           # dry-run
#   ./set-emit-target.sh cv_logs_staging --apply
#   ./set-emit-target.sh logs --apply              # flip to production
#   ./set-emit-target.sh --status                  # print the current value
#
# Exit codes:
#   0 success / dry-run
#   2 pre-flight (wrong account / bad arg / missing CLI / function not found)
#   3 AWS API error

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"
FUNCTION_NAME="${FUNCTION_NAME:-uball-cv-dispatch}"

TARGET=""
APPLY=0
SHOW_STATUS=0

log()  { printf '\n\033[1;34m[set-emit-target]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[set-emit-target]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[set-emit-target]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

for arg in "$@"; do
  case "$arg" in
    --apply)   APPLY=1 ;;
    --status)  SHOW_STATUS=1 ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    cv_logs_staging|logs) TARGET="$arg" ;;
    *) fail "unknown arg: $arg (valid targets: cv_logs_staging, logs)" 2 ;;
  esac
done

command -v aws >/dev/null 2>&1 || fail "aws CLI required" 2
command -v jq  >/dev/null 2>&1 || fail "jq required" 2

acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
  || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2

# Read current env.
current_env_json="$(aws lambda get-function-configuration \
  --function-name "$FUNCTION_NAME" --region "$REGION" \
  --query 'Environment.Variables' 2>/dev/null || true)"

if [[ -z "$current_env_json" || "$current_env_json" == "null" ]]; then
  fail "Lambda $FUNCTION_NAME not found in $REGION (or has no env). Has it been deployed yet? See PR #37." 2
fi

current_target="$(echo "$current_env_json" | jq -r '.CV_EMIT_TARGET // "<unset>"')"

if (( SHOW_STATUS )); then
  log "current CV_EMIT_TARGET = $current_target"
  log "(full env follows)"
  echo "$current_env_json" | jq .
  exit 0
fi

[[ -n "$TARGET" ]] \
  || fail "missing target (cv_logs_staging | logs) — see --help" 2

log "function: $FUNCTION_NAME  region: $REGION"
log "current CV_EMIT_TARGET = $current_target"
log "new     CV_EMIT_TARGET = $TARGET"

if [[ "$current_target" == "$TARGET" ]]; then
  log "already at target — nothing to do"
  exit 0
fi

# Build the new env by merging.
new_env_json="$(echo "$current_env_json" | jq --arg t "$TARGET" '.CV_EMIT_TARGET = $t')"
new_env_cli="$(jq -n --argjson v "$new_env_json" '{Variables:$v}')"

if (( !APPLY )); then
  log "DRY-RUN — pass --apply to actually update the Lambda config."
  log "new Environment.Variables would be:"
  echo "$new_env_json" | jq .
  exit 0
fi

log "applying"
aws lambda update-function-configuration \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --environment "$new_env_cli" \
  > /dev/null \
  || fail "update-function-configuration failed" 3

log "done — flipped CV_EMIT_TARGET → $TARGET"

if [[ "$TARGET" == "logs" ]]; then
  log ""
  warn "PRODUCTION CUTOVER — new CV runs from now on emit to basketball-games.logs[]"
  warn "  → plays_sync picks them up → annotation cards show CV plays"
  warn "Confirm at least one shadow-mode run looked right before staying here."
  warn "To roll back: ./scripts/cv_infra/ops/set-emit-target.sh cv_logs_staging --apply"
fi
