#!/usr/bin/env bash
#
# Provision the three UBall-backend credentials in AWS Secrets Manager
# used by the uball-cv-merge container to authenticate against the
# UBall backend when calling `plays_sync.create_plays_from_firebase_logs`.
#
# Secrets created (under prefix `uball/cv/`):
#   uball/cv/backend-url      — UBall backend base URL (e.g. https://api.uball.example.com)
#   uball/cv/auth-email       — service-account email
#   uball/cv/auth-password    — service-account password
#
# Each is created with a recognisably-invalid placeholder value
# (`<NOT_SET_RUN_PUT_SECRET_VALUE>`) so an accidental merge-job launch
# fails fast at the env-var read rather than silently mis-authenticating.
# The operator then populates the real values via
# `aws secretsmanager put-secret-value` (don't paste through chat).
#
# Why a separate script from `bootstrap-firebase-secret.sh`:
#   * Different IAM resource pattern (`uball/cv/*` vs
#     `uball/firebase-admin-cv-merge-*`) — split lets each have its own
#     narrow IAM statement.
#   * Different consumption pattern in the container: backend creds are
#     injected as env vars via the AWS Batch `secrets` block (ECS fetches
#     the secret at task start). Firebase JSON is fetched in-process by
#     the entrypoint (the Batch `secrets` block can't easily inject a
#     multi-line JSON value into the FIREBASE_CREDENTIALS_PATH env).
#   * Different rotate cadence — backend creds rotate with normal
#     service-account password ops; Firebase admin rotates rarely.
#
# Usage:
#   ./bootstrap-uball-backend-secrets.sh             # apply (idempotent)
#   ./bootstrap-uball-backend-secrets.sh --dry-run   # print only
#   ./bootstrap-uball-backend-secrets.sh --check     # exit 4 if any missing or placeholder
#   ./bootstrap-uball-backend-secrets.sh --print-arns
#                                                   # print the ARNs (for piping into env vars
#                                                   # consumed by register-batch-job-defs.sh)
#
# Exit codes:
#   0 success
#   2 pre-flight failed (wrong account / missing aws CLI)
#   3 AWS API error
#   4 (in --check) any of the 3 secrets is missing or still placeholder

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"

# (env-var-name, secret-name, description) triples.
SECRETS=(
  "UBALL_BACKEND_URL_SECRET_ARN     uball/cv/backend-url       UBall backend base URL for the uball-cv-merge container"
  "UBALL_AUTH_EMAIL_SECRET_ARN      uball/cv/auth-email        UBall service-account email for the uball-cv-merge container"
  "UBALL_AUTH_PASSWORD_SECRET_ARN   uball/cv/auth-password     UBall service-account password for the uball-cv-merge container"
)
PLACEHOLDER='<NOT_SET_RUN_PUT_SECRET_VALUE>'

log()  { printf '\n\033[1;34m[bootstrap-uball-backend]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[bootstrap-uball-backend]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[bootstrap-uball-backend]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

MODE=apply
for arg in "$@"; do
  case "$arg" in
    --apply)      MODE=apply ;;
    --dry-run)    MODE=dry   ;;
    --check)      MODE=check ;;
    --print-arns) MODE=print ;;
    -h|--help)    sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *) fail "unknown arg: $arg" 2 ;;
  esac
done

command -v aws >/dev/null 2>&1 || fail "aws CLI required" 2

acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
  || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2
[[ "$MODE" == "print" ]] || log "caller account: $acct  region: $REGION"

any_missing=0
any_placeholder=0

for entry in "${SECRETS[@]}"; do
  read -r env_name secret_name description <<<"$entry"

  if existing="$(aws secretsmanager describe-secret \
    --secret-id "$secret_name" --region "$REGION" \
    --query 'ARN' --output text 2>/dev/null)"; then
    case "$MODE" in
      apply|dry)
        log "$secret_name exists: $existing"
        ;;
      check)
        # Peek at the current value to decide placeholder vs real.
        val="$(aws secretsmanager get-secret-value \
          --secret-id "$secret_name" --region "$REGION" \
          --query 'SecretString' --output text 2>/dev/null || true)"
        if [[ "$val" == "$PLACEHOLDER" ]]; then
          warn "$secret_name still has placeholder value"
          any_placeholder=1
        else
          log "$secret_name has non-placeholder value ✓"
        fi
        ;;
      print)
        printf '%s=%s\n' "$env_name" "$existing"
        ;;
    esac
    continue
  fi

  # Missing.
  case "$MODE" in
    apply)
      log "creating $secret_name"
      aws secretsmanager create-secret \
        --name "$secret_name" \
        --description "$description" \
        --secret-string "$PLACEHOLDER" \
        --region "$REGION" \
        --tags '[{"Key":"service","Value":"cv-shot-detection"},{"Key":"pipeline","Value":"v1"}]' \
        > /dev/null \
        || fail "create-secret $secret_name failed" 3
      ;;
    dry)
      log "DRY: would create $secret_name"
      ;;
    check|print)
      warn "$secret_name missing"
      any_missing=1
      ;;
  esac
done

case "$MODE" in
  apply)
    log ""
    log "NEXT — operator fills the 3 real values, e.g.:"
    log "  aws secretsmanager put-secret-value --secret-id uball/cv/backend-url    --secret-string 'https://api.uball.example.com' --region $REGION"
    log "  aws secretsmanager put-secret-value --secret-id uball/cv/auth-email     --secret-string '<email>'                       --region $REGION"
    log "  aws secretsmanager put-secret-value --secret-id uball/cv/auth-password  --secret-string '<password>'                    --region $REGION"
    log ""
    log "Then export the ARNs and re-register the cv-merge job definition:"
    log "  eval \"\$(./scripts/cv_infra/secrets/bootstrap-uball-backend-secrets.sh --print-arns)\""
    log "  ./scripts/cv_infra/register-batch-job-defs.sh --merge-only"
    ;;
  check)
    if (( any_missing || any_placeholder )); then
      exit 4
    fi
    ;;
esac

[[ "$MODE" == "print" ]] || log "done ($MODE)"
