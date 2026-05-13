#!/usr/bin/env bash
#
# Bootstrap the GitHub Actions OIDC provider + the uball-gha-ecr-push role.
#
# This is a one-time setup script. After it runs successfully, GitHub
# Actions workflows in the two repos listed in
# `gha-ecr-push-trust.json` can call
# `aws-actions/configure-aws-credentials` with
#   role-to-assume: arn:aws:iam::840102831548:role/uball-gha-ecr-push
# and push images to the `uball-cv-fusion` + `uball-cv-merge` ECR repos
# without any long-lived AWS keys in GitHub secrets.
#
# Idempotent — re-running detects existing resources and is a no-op
# unless the trust or permission policy drifted from the repo files.
#
# Usage:
#   ./bootstrap-gha-oidc.sh             # apply (creates anything missing)
#   ./bootstrap-gha-oidc.sh --dry-run   # print what would happen
#   ./bootstrap-gha-oidc.sh --check     # diff live vs repo; non-zero on drift
#
# Exit codes:
#   0 success
#   2 pre-flight failed (wrong account, missing CLI, missing policy file)
#   3 AWS API error

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"
ROLE_NAME="uball-gha-ecr-push"
POLICY_NAME="gha-ecr-push-inline"
OIDC_HOST="token.actions.githubusercontent.com"

POLICIES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRUST_FILE="$POLICIES_DIR/gha-ecr-push-trust.json"
INLINE_FILE="$POLICIES_DIR/gha-ecr-push-inline.json"

log()  { printf '\n\033[1;34m[bootstrap-gha-oidc]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[bootstrap-gha-oidc]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[bootstrap-gha-oidc]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

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
[[ -f "$TRUST_FILE" ]]  || fail "missing $TRUST_FILE"  2
[[ -f "$INLINE_FILE" ]] || fail "missing $INLINE_FILE" 2

acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
  || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2
log "caller account: $acct"

# -----------------------------------------------------------------
# 1. OIDC provider
# -----------------------------------------------------------------
oidc_arn="arn:aws:iam::$EXPECTED_ACCOUNT:oidc-provider/$OIDC_HOST"

if aws iam get-open-id-connect-provider --open-id-connect-provider-arn "$oidc_arn" >/dev/null 2>&1; then
  log "OIDC provider already exists: $oidc_arn"
else
  case "$MODE" in
    apply)
      log "creating OIDC provider for $OIDC_HOST"
      # NB: AWS now derives + maintains the thumbprint server-side. We
      # still need to pass at least one thumbprint to satisfy the API
      # contract. Using GitHub's well-known thumbprint as a placeholder;
      # AWS will refresh it. See:
      # https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-idp_oidc-obtain-thumbprint.html
      aws iam create-open-id-connect-provider \
        --url "https://$OIDC_HOST" \
        --client-id-list "sts.amazonaws.com" \
        --thumbprint-list "6938fd4d98bab03faadb97b34396831e3780aea1" \
        || fail "create-open-id-connect-provider failed" 3
      ;;
    dry)
      log "DRY: would create OIDC provider $oidc_arn"
      ;;
    check)
      warn "OIDC provider missing (drift)"
      exit 1
      ;;
  esac
fi

# -----------------------------------------------------------------
# 2. Role
# -----------------------------------------------------------------
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  log "role $ROLE_NAME exists — checking trust policy"
  live="$(mktemp /tmp/gha-trust-XXXXXX.json)"
  aws iam get-role --role-name "$ROLE_NAME" \
    --query 'Role.AssumeRolePolicyDocument' > "$live"
  if diff -u "$live" "$TRUST_FILE" >/dev/null 2>&1; then
    log "trust policy in sync"
  else
    case "$MODE" in
      apply)
        log "updating trust policy"
        aws iam update-assume-role-policy \
          --role-name "$ROLE_NAME" \
          --policy-document "file://$TRUST_FILE" \
          || fail "update-assume-role-policy failed" 3
        ;;
      dry)
        log "DRY: would update trust policy"
        diff -u "$live" "$TRUST_FILE" || true
        ;;
      check)
        warn "trust policy drift"
        diff -u "$live" "$TRUST_FILE" || true
        exit 1
        ;;
    esac
  fi
  rm -f "$live"
else
  case "$MODE" in
    apply)
      log "creating role $ROLE_NAME"
      aws iam create-role \
        --role-name "$ROLE_NAME" \
        --description "GitHub Actions OIDC role for pushing CV pipeline images to ECR" \
        --assume-role-policy-document "file://$TRUST_FILE" \
        --max-session-duration 3600 \
        > /dev/null \
        || fail "create-role failed" 3
      ;;
    dry)
      log "DRY: would create role $ROLE_NAME"
      ;;
    check)
      warn "role $ROLE_NAME missing"
      exit 1
      ;;
  esac
fi

# -----------------------------------------------------------------
# 3. Inline permission policy
# -----------------------------------------------------------------
if aws iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" >/dev/null 2>&1; then
  log "policy $POLICY_NAME exists — checking permissions"
  live="$(mktemp /tmp/gha-inline-XXXXXX.json)"
  aws iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" \
    --query 'PolicyDocument' > "$live"
  if diff -u "$live" "$INLINE_FILE" >/dev/null 2>&1; then
    log "permission policy in sync"
  else
    case "$MODE" in
      apply)
        log "updating permission policy"
        aws iam put-role-policy \
          --role-name "$ROLE_NAME" \
          --policy-name "$POLICY_NAME" \
          --policy-document "file://$INLINE_FILE" \
          || fail "put-role-policy failed" 3
        ;;
      dry)
        log "DRY: would update permission policy"
        diff -u "$live" "$INLINE_FILE" || true
        ;;
      check)
        warn "permission policy drift"
        diff -u "$live" "$INLINE_FILE" || true
        exit 1
        ;;
    esac
  fi
  rm -f "$live"
else
  case "$MODE" in
    apply)
      log "attaching $POLICY_NAME"
      aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "$POLICY_NAME" \
        --policy-document "file://$INLINE_FILE" \
        || fail "put-role-policy failed" 3
      ;;
    dry)
      log "DRY: would attach $POLICY_NAME"
      ;;
    check)
      warn "permission policy missing"
      exit 1
      ;;
  esac
fi

log "done ($MODE) — role ARN: arn:aws:iam::$EXPECTED_ACCOUNT:role/$ROLE_NAME"
