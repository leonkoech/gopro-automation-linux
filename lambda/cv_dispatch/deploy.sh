#!/usr/bin/env bash
#
# Deploy the uball-cv-dispatch Lambda.
#
# Prerequisites:
#   * AWS SAM CLI installed (pip install aws-sam-cli)
#   * AWS CLI configured with creds in account 840102831548
#   * IAM role uball-cv-dispatch-lambda exists (Phase 0, UBA-201)
#   * EventBridge notifications enabled on uball-videos-production (one
#     time — this script enables them on first run)
#
# Usage:
#   ./deploy.sh                 # Deploy to us-east-1 with defaults
#   ./deploy.sh us-west-2       # Override region
#   ./deploy.sh us-east-1 1     # Region + skip EventBridge enable (already on)
#
# The script is idempotent — re-running updates the stack and leaves the
# bucket notification config unchanged once EventBridge is enabled.

set -euo pipefail

REGION="${1:-us-east-1}"
SKIP_EVENTBRIDGE_ENABLE="${2:-0}"
STACK_NAME="uball-cv-dispatch"
ARTIFACT_BUCKET="uball-lambda-deployments"
INPUTS_BUCKET="uball-videos-production"

log() { printf '\n\033[1;34m[deploy-cv-dispatch]\033[0m %s\n' "$*"; }
fail() { printf '\n\033[1;31m[deploy-cv-dispatch]\033[0m %s\n' "$*" >&2; exit 1; }

command -v sam >/dev/null 2>&1 || fail "AWS SAM CLI not installed (pip install aws-sam-cli)"
command -v aws >/dev/null 2>&1 || fail "AWS CLI not installed"

log "region=$REGION stack=$STACK_NAME"

# ---------------------------------------------------------------------------
# 1. Ensure the SAM artifact bucket exists
# ---------------------------------------------------------------------------
if ! aws s3 ls "s3://$ARTIFACT_BUCKET" --region "$REGION" >/dev/null 2>&1; then
  log "creating SAM artifact bucket $ARTIFACT_BUCKET"
  aws s3 mb "s3://$ARTIFACT_BUCKET" --region "$REGION"
fi

# ---------------------------------------------------------------------------
# 2. Enable EventBridge notifications on the inputs bucket (one-time)
# ---------------------------------------------------------------------------
if [[ "$SKIP_EVENTBRIDGE_ENABLE" != "1" ]]; then
  log "checking EventBridge notification on $INPUTS_BUCKET"
  current="$(aws s3api get-bucket-notification-configuration \
    --bucket "$INPUTS_BUCKET" --region "$REGION" \
    --query 'EventBridgeConfiguration' --output text 2>/dev/null || echo None)"

  if [[ "$current" == "None" || -z "$current" ]]; then
    log "enabling EventBridge notifications on $INPUTS_BUCKET (one-time)"
    aws s3api put-bucket-notification-configuration \
      --bucket "$INPUTS_BUCKET" --region "$REGION" \
      --notification-configuration '{"EventBridgeConfiguration":{}}'
    log "EventBridge enabled"
  else
    log "EventBridge already enabled"
  fi
fi

# ---------------------------------------------------------------------------
# 3. SAM build (Makefile bundles cv_batch_dispatch.py + cv_metrics.py)
# ---------------------------------------------------------------------------
log "sam build"
sam build --template-file template.yaml

# ---------------------------------------------------------------------------
# 4. SAM deploy
# ---------------------------------------------------------------------------
log "sam deploy"
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --s3-bucket "$ARTIFACT_BUCKET" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset

# ---------------------------------------------------------------------------
# 5. Smoke check
# ---------------------------------------------------------------------------
FUNCTION_NAME="$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionName`].OutputValue' \
  --output text)"

log "deployed function: $FUNCTION_NAME"
aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" \
  --query 'Configuration.{Name:FunctionName,State:State,LastModified:LastModified,Role:Role,Memory:MemorySize,Timeout:Timeout}'

log "done"
