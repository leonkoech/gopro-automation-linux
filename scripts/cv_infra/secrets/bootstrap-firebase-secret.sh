#!/usr/bin/env bash
#
# Phase 4 / [UBA-219](https://linear.app/uball/issue/UBA-219) — provision the Firebase admin SDK secret in AWS
# Secrets Manager for the uball-cv-merge container.
#
# Behaviour:
#   * If the secret doesn't exist → create it with a placeholder value
#     ({"PLACEHOLDER": "fill via aws secretsmanager put-secret-value or
#     the AWS console"}). The placeholder is recognisably invalid, so a
#     merge container that tries to use it fails fast with a clear error
#     instead of silently mis-authenticating.
#   * If the secret already exists → leave it alone. This script never
#     overwrites a real value, so re-running after the value has been
#     populated is safe.
#
# Why a placeholder instead of the real JSON?
#   * The Firebase admin JSON is sensitive enough that we don't want a
#     copy in the repo, an AWS CloudTrail log, a CI artefact, or a
#     terminal scrollback. The operator populates the real value in a
#     channel of their choice (AWS console, paste via SSM Session
#     Manager, vault import) — this script just guarantees the secret
#     entry exists with the right name + IAM policy match.
#
# Usage:
#   ./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh             # apply
#   ./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh --dry-run   # print only
#   ./scripts/cv_infra/secrets/bootstrap-firebase-secret.sh --check     # exit non-zero if missing or still placeholder
#
# Exit codes:
#   0 success / placeholder-still-set (in apply or dry mode)
#   2 pre-flight (wrong account / missing aws CLI)
#   3 AWS API error
#   4 (in --check) secret is missing or still has placeholder value

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"
SECRET_NAME="${FIREBASE_ADMIN_SECRET_NAME:-uball/firebase-admin-cv-merge}"
DESCRIPTION="Firebase Admin SDK credentials JSON for the uball-cv-merge container (Phase 4 / UBA-219)"
PLACEHOLDER_VALUE='{"PLACEHOLDER":"fill via aws secretsmanager put-secret-value --secret-id '"$SECRET_NAME"' --secret-string file://path/to/firebase-admin.json"}'

log()  { printf '\n\033[1;34m[bootstrap-firebase-secret]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[bootstrap-firebase-secret]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[bootstrap-firebase-secret]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

MODE=apply
for arg in "$@"; do
  case "$arg" in
    --apply)   MODE=apply ;;
    --dry-run) MODE=dry   ;;
    --check)   MODE=check ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *) fail "unknown arg: $arg" 2 ;;
  esac
done

command -v aws >/dev/null 2>&1 || fail "aws CLI required" 2

acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
  || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2
log "caller account: $acct  region: $REGION  secret: $SECRET_NAME"

# -----------------------------------------------------------------
# Does the secret exist?
# -----------------------------------------------------------------
if existing="$(aws secretsmanager describe-secret \
  --secret-id "$SECRET_NAME" --region "$REGION" \
  --query '{arn:ARN,name:Name}' --output json 2>/dev/null)"; then
  arn="$(echo "$existing" | sed -n 's/.*"arn": *"\([^"]*\)".*/\1/p')"
  log "secret exists: $arn"

  # Peek at the current value to decide whether it's still the placeholder.
  current_value=""
  if [[ "$MODE" == "check" ]]; then
    current_value="$(aws secretsmanager get-secret-value \
      --secret-id "$SECRET_NAME" --region "$REGION" \
      --query 'SecretString' --output text 2>/dev/null || true)"
    if [[ "$current_value" == *"PLACEHOLDER"* ]]; then
      warn "secret still has placeholder value — operator needs to fill it"
      exit 4
    fi
    log "secret has non-placeholder value ✓"
  fi
  exit 0
fi

# -----------------------------------------------------------------
# Create.
# -----------------------------------------------------------------
case "$MODE" in
  check)
    warn "secret $SECRET_NAME not found"
    exit 4
    ;;
  dry)
    log "DRY: would create secret $SECRET_NAME with placeholder value"
    exit 0
    ;;
  apply)
    log "creating secret $SECRET_NAME"
    aws secretsmanager create-secret \
      --name "$SECRET_NAME" \
      --description "$DESCRIPTION" \
      --secret-string "$PLACEHOLDER_VALUE" \
      --region "$REGION" \
      --tags '[{"Key":"service","Value":"cv-shot-detection"},{"Key":"pipeline","Value":"v1"}]' \
      > /dev/null \
      || fail "create-secret failed" 3

    # Re-fetch to confirm + log the ARN (it has a random 6-char suffix
    # appended by AWS — the IAM policy on uball-cv-merge-execution uses
    # `arn:...:secret:uball/firebase-admin-cv-merge-*` so the suffix
    # matches without policy change).
    arn="$(aws secretsmanager describe-secret \
      --secret-id "$SECRET_NAME" --region "$REGION" \
      --query 'ARN' --output text)"
    log "created: $arn"
    log ""
    log "NEXT — operator fills the real value:"
    log "  aws secretsmanager put-secret-value \\"
    log "    --secret-id $SECRET_NAME \\"
    log "    --secret-string file:///path/to/uball-gopro-fleet-firebase-adminsdk.json \\"
    log "    --region $REGION"
    log ""
    log "Or paste the JSON via the AWS console: Secrets Manager → $SECRET_NAME → Retrieve secret value → Edit."
    ;;
esac

log "done ($MODE)"
