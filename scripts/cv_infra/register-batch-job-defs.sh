#!/usr/bin/env bash
#
# Phase 2.1 (UBA-207) — register the two Batch job definitions used by the V1
# CV shot-detection pipeline.
#
# Substitutes the PLACEHOLDER_* tokens in the JSONs under
# `deploy/batch-job-defs/` with concrete ARNs (defaults reflect the
# 840102831548 account; override via env if you deploy this elsewhere) and
# calls `aws batch register-job-definition` for each.
#
# Re-running is safe — Batch creates a new immutable revision each time.
# Old revisions stay registered but are not the default for submit-job that
# names the job-def without `:revision`.
#
# Required env / args:
#   AWS_REGION                 default: us-east-1
#   ACCOUNT_ID                 default: 840102831548
#   FUSION_IMAGE_URI           default: ${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/uball-cv-fusion:v1
#   MERGE_IMAGE_URI            default: ${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/uball-cv-merge:v1
#   FUSION_JOB_ROLE_ARN        default: arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-batch-execution
#   FUSION_EXECUTION_ROLE_ARN  default: arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-batch-execution
#   MERGE_JOB_ROLE_ARN         default: arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-merge-execution
#   MERGE_EXECUTION_ROLE_ARN   default: arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-merge-execution
#   UBALL_BACKEND_URL_SECRET_ARN     required for merge (Secrets Manager)
#   UBALL_AUTH_EMAIL_SECRET_ARN      required for merge
#   UBALL_AUTH_PASSWORD_SECRET_ARN   required for merge
#
# Flags:
#   --dry-run     print the substituted JSONs to stdout; don't call AWS
#   --fusion-only / --merge-only   register one of the two job defs only
#
# Usage:
#   ./scripts/cv_infra/register-batch-job-defs.sh --dry-run
#   ./scripts/cv_infra/register-batch-job-defs.sh
#   ./scripts/cv_infra/register-batch-job-defs.sh --fusion-only
#
# Exit codes:
#   0  both job defs registered (or dry-run completed)
#   2  pre-flight check failed (missing required env / missing AWS resource)
#   3  AWS API error during registration

set -euo pipefail

# -------------------------------------------------------------------- helpers
log()  { printf '\n\033[1;34m[register-batch-job-defs]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[register-batch-job-defs]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[register-batch-job-defs]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JD_DIR="$REPO_ROOT/deploy/batch-job-defs"

# ----------------------------------------------------------------- arg parse
DRY_RUN=0
WHICH=both
for arg in "$@"; do
  case "$arg" in
    --dry-run)     DRY_RUN=1 ;;
    --fusion-only) WHICH=fusion ;;
    --merge-only)  WHICH=merge ;;
    -h|--help)
      sed -n '2,/^set -e/p' "$0" | sed -e 's/^# \{0,1\}//; /^set -e/d'
      exit 0
      ;;
    *) fail "unknown arg: $arg" 2 ;;
  esac
done

# ---------------------------------------------------------------- defaults
: "${AWS_REGION:=us-east-1}"
: "${ACCOUNT_ID:=840102831548}"
: "${FUSION_IMAGE_URI:=${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/uball-cv-fusion:v1}"
: "${MERGE_IMAGE_URI:=${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/uball-cv-merge:v1}"
: "${FUSION_JOB_ROLE_ARN:=arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-batch-execution}"
: "${FUSION_EXECUTION_ROLE_ARN:=arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-batch-execution}"
: "${MERGE_JOB_ROLE_ARN:=arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-merge-execution}"
: "${MERGE_EXECUTION_ROLE_ARN:=arn:aws:iam::${ACCOUNT_ID}:role/uball-cv-merge-execution}"

# Secrets — only required when registering the merge job def.
: "${UBALL_BACKEND_URL_SECRET_ARN:=}"
: "${UBALL_AUTH_EMAIL_SECRET_ARN:=}"
: "${UBALL_AUTH_PASSWORD_SECRET_ARN:=}"

# ----------------------------------------------------------- pre-flight
require_tool() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1" 2
}
require_tool aws
require_tool sed

if (( !DRY_RUN )); then
  log "checking caller identity in account $ACCOUNT_ID, region $AWS_REGION..."
  caller_acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [[ "$caller_acct" == "$ACCOUNT_ID" ]] \
    || fail "caller account ($caller_acct) does not match expected $ACCOUNT_ID" 2
fi

check_role() {
  local role_arn="$1" role_name
  role_name="${role_arn##*/}"
  aws iam get-role --role-name "$role_name" >/dev/null 2>&1 \
    || fail "IAM role missing: $role_name (expected $role_arn)" 2
}

require_secret_env() {
  # Always-required env-var presence check (runs in dry-run too).
  local arn="$1" label="$2"
  [[ -n "$arn" ]] || fail "$label is required for merge registration (see --help)" 2
}

check_secret_in_aws() {
  # Only-when-not-dry-run check that the Secrets Manager entry actually exists.
  local arn="$1" label="$2"
  aws secretsmanager describe-secret --secret-id "$arn" --region "$AWS_REGION" >/dev/null 2>&1 \
    || fail "Secrets Manager entry missing: $arn ($label)" 2
}

# ----------------------------------------------------------- substitution
substitute_into() {
  local src="$1" dst="$2"
  # sed handles ARNs and image URIs cleanly because we use '#' as the delimiter.
  sed \
    -e "s#PLACEHOLDER_ECR_IMAGE_URI#${3}#g" \
    -e "s#PLACEHOLDER_CV_BATCH_JOB_ROLE_ARN#${4}#g" \
    -e "s#PLACEHOLDER_CV_BATCH_EXECUTION_ROLE_ARN#${5}#g" \
    -e "s#PLACEHOLDER_UBALL_BACKEND_URL_SECRET_ARN#${UBALL_BACKEND_URL_SECRET_ARN}#g" \
    -e "s#PLACEHOLDER_UBALL_AUTH_EMAIL_SECRET_ARN#${UBALL_AUTH_EMAIL_SECRET_ARN}#g" \
    -e "s#PLACEHOLDER_UBALL_AUTH_PASSWORD_SECRET_ARN#${UBALL_AUTH_PASSWORD_SECRET_ARN}#g" \
    "$src" > "$dst"
  # Guard: confirm no PLACEHOLDER_ tokens remain (so a missing env var
  # doesn't silently leave a placeholder in the registered JD).
  if grep -q 'PLACEHOLDER_' "$dst"; then
    grep 'PLACEHOLDER_' "$dst" >&2
    fail "unresolved placeholders in $dst — set the matching env vars" 2
  fi
}

# ----------------------------------------------------------- register one
register_one() {
  local label="$1" src_json="$2" job_role="$3" exec_role="$4" image_uri="$5"
  local tmp; tmp="$(mktemp /tmp/cv-batch-jd-XXXXXX.json)"
  trap 'rm -f "$tmp"' RETURN

  substitute_into "$src_json" "$tmp" "$image_uri" "$job_role" "$exec_role"

  if (( DRY_RUN )); then
    log "[$label] dry-run — substituted JSON:"
    cat "$tmp"
    log "[$label] dry-run complete (no AWS call made)"
    return 0
  fi

  log "[$label] registering job definition..."
  local out
  out="$(aws batch register-job-definition \
    --region "$AWS_REGION" \
    --cli-input-json "file://$tmp" 2>&1)" || fail "[$label] register failed:\n$out" 3
  local revision
  revision="$(echo "$out" | sed -n 's/.*"revision": \([0-9]*\).*/\1/p' | head -1)"
  log "[$label] registered revision $revision"
}

# ----------------------------------------------------------- main
log "scope: $WHICH; dry_run=$DRY_RUN; region=$AWS_REGION"

if [[ "$WHICH" == "fusion" || "$WHICH" == "both" ]]; then
  if (( !DRY_RUN )); then
    check_role "$FUSION_JOB_ROLE_ARN"
    check_role "$FUSION_EXECUTION_ROLE_ARN"
  fi
  register_one fusion "$JD_DIR/cv-fusion-job-def.json" \
    "$FUSION_JOB_ROLE_ARN" "$FUSION_EXECUTION_ROLE_ARN" "$FUSION_IMAGE_URI"
fi

if [[ "$WHICH" == "merge" || "$WHICH" == "both" ]]; then
  # Env-var presence check — required even in dry-run so we don't silently
  # write `valueFrom: ""` into the substituted JSON.
  require_secret_env "$UBALL_BACKEND_URL_SECRET_ARN"   UBALL_BACKEND_URL_SECRET_ARN
  require_secret_env "$UBALL_AUTH_EMAIL_SECRET_ARN"    UBALL_AUTH_EMAIL_SECRET_ARN
  require_secret_env "$UBALL_AUTH_PASSWORD_SECRET_ARN" UBALL_AUTH_PASSWORD_SECRET_ARN

  if (( !DRY_RUN )); then
    check_role "$MERGE_JOB_ROLE_ARN"
    check_role "$MERGE_EXECUTION_ROLE_ARN"
    check_secret_in_aws "$UBALL_BACKEND_URL_SECRET_ARN"   UBALL_BACKEND_URL_SECRET_ARN
    check_secret_in_aws "$UBALL_AUTH_EMAIL_SECRET_ARN"    UBALL_AUTH_EMAIL_SECRET_ARN
    check_secret_in_aws "$UBALL_AUTH_PASSWORD_SECRET_ARN" UBALL_AUTH_PASSWORD_SECRET_ARN
  fi
  register_one merge "$JD_DIR/cv-merge-job-def.json" \
    "$MERGE_JOB_ROLE_ARN" "$MERGE_EXECUTION_ROLE_ARN" "$MERGE_IMAGE_URI"
fi

log "done"
