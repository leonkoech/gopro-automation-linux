#!/usr/bin/env bash
#
# Apply the canonical inline policies from `scripts/cv_infra/iam-policies/`
# to the three CV pipeline IAM roles. Idempotent — re-running is safe.
#
# This is the source of truth for the CV roles' inline policies going
# forward. Phase 0 ([UBA-201](https://linear.app/uball/issue/UBA-201)) created the roles via the AWS console /
# ad-hoc CLI without a repo source. After the
# `cloudwatch:namespace == UballCV` bug was found during Phase 2.2 work,
# the corrected policies were dumped from live AWS, edited, and committed
# here.
#
# Usage:
#   ./scripts/cv_infra/iam-policies/apply-policies.sh           # apply all 3
#   ./scripts/cv_infra/iam-policies/apply-policies.sh --diff    # diff live vs file
#   ./scripts/cv_infra/iam-policies/apply-policies.sh --dry-run # show file paths
#
# Exit codes:
#   0  success
#   2  pre-flight failed (missing aws cli / wrong account)
#   3  AWS API error during put-role-policy

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"

POLICIES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# (role-name, policy-name, file)
ROLES=(
  "uball-cv-dispatch-lambda  cv-dispatch-inline  cv-dispatch-inline.json"
  "uball-cv-batch-execution  cv-fusion-inline    cv-fusion-inline.json"
  "uball-cv-merge-execution  cv-merge-inline     cv-merge-inline.json"
)

log()  { printf '\n\033[1;34m[apply-policies]\033[0m %s\n' "$*"; }
fail() { printf '\n\033[1;31m[apply-policies]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

MODE=apply
for arg in "$@"; do
  case "$arg" in
    --diff)    MODE=diff ;;
    --dry-run) MODE=dry  ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *) fail "unknown arg: $arg" 2 ;;
  esac
done

command -v aws >/dev/null 2>&1 || fail "aws CLI required" 2

if [[ "$MODE" != "dry" ]]; then
  acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
    || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2
  log "caller account: $acct (region: $REGION)"
fi

for line in "${ROLES[@]}"; do
  read -r role policy file <<<"$line"
  path="$POLICIES_DIR/$file"
  [[ -f "$path" ]] || fail "missing policy file: $path" 2

  case "$MODE" in
    apply)
      log "applying $policy → $role"
      aws iam put-role-policy \
        --role-name "$role" \
        --policy-name "$policy" \
        --policy-document "file://$path" \
        || fail "put-role-policy failed for $role/$policy" 3
      ;;
    diff)
      log "diffing live vs $file"
      live="$(mktemp /tmp/iam-policy-XXXXXX.json)"
      aws iam get-role-policy --role-name "$role" --policy-name "$policy" \
        --query 'PolicyDocument' > "$live" 2>/dev/null || true
      if diff -u "$live" "$path"; then
        printf '  ✓ %s in sync\n' "$role"
      else
        printf '  ✗ %s differs (see diff above)\n' "$role"
      fi
      rm -f "$live"
      ;;
    dry)
      log "DRY: would apply $path → $role/$policy"
      ;;
  esac
done

log "done ($MODE)"
