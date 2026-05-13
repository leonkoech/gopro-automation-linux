#!/usr/bin/env bash
#
# Phase 6 (UBA-224 + UBA-226) — apply the CV pipeline's CloudWatch
# resources from the canonical JSON files under `deploy/cloudwatch/`:
#
#   * dashboard: `UBall-CV-Pipeline` (6 widgets, see dashboard.json)
#   * alarms:    `UBall-CV-JobFailure`,
#                `UBall-CV-DispatchUnhandledError`,
#                `UBall-CV-NeedsReviewStreak`
#                — all action onto the `uball-cv-failures` SNS topic.
#   * log retention: 14 days on /aws/batch/cv-fusion,
#                /aws/batch/cv-merge, and /aws/lambda/uball-cv-dispatch.
#
# Idempotent — re-running updates the resources in place. Pre-creates
# log groups so the retention setting is in effect from the first
# Batch / Lambda run (otherwise CloudWatch creates them on first write
# with the AWS-default retention of "Never expire", which is wrong for
# us).
#
# Required env / defaults:
#   AWS_REGION              default: us-east-1
#   ACCOUNT_ID              default: 840102831548
#   SNS_TOPIC_ARN           default: arn:aws:sns:us-east-1:${ACCOUNT_ID}:uball-cv-failures
#                           (created in Phase 0 / UBA-200)
#   LOG_RETENTION_DAYS      default: 14 (UBA-226)
#
# Usage:
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh           # apply all 3 (dashboard, alarms, retention)
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --dry-run # print only, no AWS calls
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --check   # exit 4 if any resource is missing or drifted
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --dashboard-only
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --alarms-only
#   ./scripts/cv_infra/cloudwatch/apply-cloudwatch.sh --retention-only
#
# Exit codes:
#   0 success
#   2 pre-flight failed
#   3 AWS API error
#   4 (in --check) drift detected

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
EXPECTED_ACCOUNT="${EXPECTED_ACCOUNT:-840102831548}"
SNS_TOPIC_ARN="${SNS_TOPIC_ARN:-arn:aws:sns:us-east-1:${EXPECTED_ACCOUNT}:uball-cv-failures}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-14}"
DASHBOARD_NAME="UBall-CV-Pipeline"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DASHBOARD_FILE="$REPO_ROOT/deploy/cloudwatch/dashboard.json"
ALARMS_FILE="$REPO_ROOT/deploy/cloudwatch/alarms.json"

LOG_GROUPS=(
  "/aws/batch/cv-fusion"
  "/aws/batch/cv-merge"
  "/aws/lambda/uball-cv-dispatch"
)

log()  { printf '\n\033[1;34m[apply-cloudwatch]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[1;33m[apply-cloudwatch]\033[0m %s\n' "$*" >&2; }
fail() { printf '\n\033[1;31m[apply-cloudwatch]\033[0m %s\n' "$*" >&2; exit "${2:-1}"; }

# ----------------------------------------------------------------- args
MODE=apply
SCOPE=all
for arg in "$@"; do
  case "$arg" in
    --apply)           MODE=apply ;;
    --dry-run)         MODE=dry   ;;
    --check)           MODE=check ;;
    --dashboard-only)  SCOPE=dashboard ;;
    --alarms-only)     SCOPE=alarms ;;
    --retention-only)  SCOPE=retention ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//; /^set -e/d'; exit 0 ;;
    *) fail "unknown arg: $arg" 2 ;;
  esac
done

# ----------------------------------------------------------------- pre-flight
command -v aws >/dev/null 2>&1 || fail "aws CLI required" 2
command -v jq  >/dev/null 2>&1 || fail "jq required (used to substitute SNS_TOPIC_ARN into alarms.json)" 2
[[ -f "$DASHBOARD_FILE" ]] || fail "missing $DASHBOARD_FILE" 2
[[ -f "$ALARMS_FILE" ]]    || fail "missing $ALARMS_FILE" 2

acct="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ "$acct" == "$EXPECTED_ACCOUNT" ]] \
  || fail "caller account ($acct) does not match expected $EXPECTED_ACCOUNT" 2
log "caller account: $acct  region: $REGION  scope: $SCOPE  mode: $MODE"

# Confirm the SNS topic exists before we paste its ARN into alarms — a
# typo here means alarms fire silently into a missing topic.
if [[ "$MODE" != "dry" && "$SCOPE" != "dashboard" && "$SCOPE" != "retention" ]]; then
  aws sns get-topic-attributes --topic-arn "$SNS_TOPIC_ARN" --region "$REGION" >/dev/null 2>&1 \
    || fail "SNS topic missing: $SNS_TOPIC_ARN (Phase 0 / UBA-200 should have created it)" 2
fi

# ----------------------------------------------------------------- DASHBOARD
apply_dashboard() {
  case "$MODE" in
    apply)
      log "putting dashboard $DASHBOARD_NAME"
      aws cloudwatch put-dashboard \
        --dashboard-name "$DASHBOARD_NAME" \
        --dashboard-body "file://$DASHBOARD_FILE" \
        --region "$REGION" \
        > /dev/null \
        || fail "put-dashboard failed" 3
      log "dashboard URL: https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${DASHBOARD_NAME}"
      ;;
    dry)
      log "DRY: would put-dashboard $DASHBOARD_NAME from $DASHBOARD_FILE"
      ;;
    check)
      if aws cloudwatch list-dashboards --region "$REGION" \
        --query "DashboardEntries[?DashboardName=='$DASHBOARD_NAME'].DashboardName" \
        --output text 2>/dev/null | grep -q "$DASHBOARD_NAME"; then
        log "dashboard $DASHBOARD_NAME exists ✓"
      else
        warn "dashboard $DASHBOARD_NAME missing"
        return 4
      fi
      ;;
  esac
}

# ----------------------------------------------------------------- ALARMS
apply_alarms() {
  # alarms.json is an array of put-metric-alarm payloads with
  # PLACEHOLDER_SNS_TOPIC_ARN — substitute, then iterate.
  local count
  count="$(jq 'length' "$ALARMS_FILE")"
  log "$count alarm(s) in $ALARMS_FILE"

  for i in $(seq 0 $((count - 1))); do
    local payload alarm_name
    payload="$(jq --arg arn "$SNS_TOPIC_ARN" '
      .['"$i"']
      | walk(if type == "string" and . == "PLACEHOLDER_SNS_TOPIC_ARN" then $arn else . end)
    ' "$ALARMS_FILE")"
    alarm_name="$(echo "$payload" | jq -r '.AlarmName')"

    case "$MODE" in
      apply)
        log "putting alarm $alarm_name"
        local tmp
        tmp="$(mktemp /tmp/cv-alarm-XXXXXX.json)"
        echo "$payload" > "$tmp"
        aws cloudwatch put-metric-alarm \
          --cli-input-json "file://$tmp" \
          --region "$REGION" \
          || { rm -f "$tmp"; fail "put-metric-alarm $alarm_name failed" 3; }
        rm -f "$tmp"
        ;;
      dry)
        log "DRY: would put alarm $alarm_name (with SNS ARN substituted)"
        ;;
      check)
        if aws cloudwatch describe-alarms --alarm-names "$alarm_name" --region "$REGION" \
          --query 'MetricAlarms[0].AlarmName' --output text 2>/dev/null | grep -q "$alarm_name"; then
          log "alarm $alarm_name exists ✓"
        else
          warn "alarm $alarm_name missing"
          return 4
        fi
        ;;
    esac
  done
}

# ----------------------------------------------------------------- LOG RETENTION
apply_log_retention() {
  for lg in "${LOG_GROUPS[@]}"; do
    case "$MODE" in
      apply)
        # Create if missing — set retention as part of the create. If it
        # already exists, fall back to put-retention-policy. ResourceAlreadyExistsException
        # is the AWS error code we tolerate.
        if aws logs create-log-group --log-group-name "$lg" --region "$REGION" 2>&1 \
          | grep -qE 'ResourceAlreadyExistsException|already exists'; then
          log "$lg already exists"
        elif aws logs describe-log-groups --log-group-name-prefix "$lg" --region "$REGION" \
          --query 'logGroups[?logGroupName==`'"$lg"'`].logGroupName' --output text 2>/dev/null \
          | grep -q "$lg"; then
          log "$lg created or pre-existed"
        else
          # create-log-group may have failed for an unrelated reason — retry.
          aws logs create-log-group --log-group-name "$lg" --region "$REGION" \
            || fail "create-log-group $lg failed" 3
        fi
        log "setting retention=$LOG_RETENTION_DAYS days on $lg"
        aws logs put-retention-policy \
          --log-group-name "$lg" \
          --retention-in-days "$LOG_RETENTION_DAYS" \
          --region "$REGION" \
          || fail "put-retention-policy $lg failed" 3
        ;;
      dry)
        log "DRY: would create-if-missing + set retention=$LOG_RETENTION_DAYS days on $lg"
        ;;
      check)
        actual="$(aws logs describe-log-groups --log-group-name-prefix "$lg" --region "$REGION" \
          --query 'logGroups[?logGroupName==`'"$lg"'`].retentionInDays' --output text 2>/dev/null)"
        if [[ -z "$actual" || "$actual" == "None" ]]; then
          warn "$lg missing or has no retention (expected $LOG_RETENTION_DAYS)"
          return 4
        fi
        if [[ "$actual" != "$LOG_RETENTION_DAYS" ]]; then
          warn "$lg retention=$actual but expected $LOG_RETENTION_DAYS"
          return 4
        fi
        log "$lg retention=$LOG_RETENTION_DAYS ✓"
        ;;
    esac
  done
}

# ----------------------------------------------------------------- main
drift=0

case "$SCOPE" in
  all)         apply_dashboard       || drift=$?
               apply_alarms          || drift=$?
               apply_log_retention   || drift=$? ;;
  dashboard)   apply_dashboard       || drift=$? ;;
  alarms)      apply_alarms          || drift=$? ;;
  retention)   apply_log_retention   || drift=$? ;;
esac

if [[ "$MODE" == "check" ]]; then
  exit "$drift"
fi

log "done ($MODE)"
