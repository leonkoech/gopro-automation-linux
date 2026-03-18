#!/bin/bash
# Deploy the Video Extractor Lambda function
#
# Prerequisites:
# 1. AWS CLI configured with appropriate credentials
# 2. AWS SAM CLI installed (pip install aws-sam-cli)
# 3. FFmpeg Lambda Layer created (see create-ffmpeg-layer.sh)
#
# Usage:
#   ./deploy.sh                    # Deploy to us-east-1
#   ./deploy.sh us-west-2          # Deploy to specific region

set -e

REGION="${1:-us-east-1}"
STACK_NAME="uball-video-extractor"
S3_BUCKET="uball-lambda-deployments"

echo "=== Deploying Video Extractor Lambda ==="
echo "Region: $REGION"
echo "Stack: $STACK_NAME"
echo ""

# Check if SAM CLI is installed
if ! command -v sam &> /dev/null; then
    echo "ERROR: AWS SAM CLI not found. Install with: pip install aws-sam-cli"
    exit 1
fi

# Check if deployment bucket exists, create if not
if ! aws s3 ls "s3://$S3_BUCKET" --region "$REGION" 2>/dev/null; then
    echo "Creating deployment bucket: $S3_BUCKET"
    aws s3 mb "s3://$S3_BUCKET" --region "$REGION"
fi

# Build the Lambda package
echo ""
echo "Building Lambda package..."
sam build --template-file template.yaml

# Deploy
echo ""
echo "Deploying to AWS..."
sam deploy \
    --template-file .aws-sam/build/template.yaml \
    --stack-name "$STACK_NAME" \
    --s3-bucket "$S3_BUCKET" \
    --region "$REGION" \
    --capabilities CAPABILITY_IAM \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset

echo ""
echo "=== Deployment Complete ==="

# Get the function ARN
FUNCTION_ARN=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='FunctionArn'].OutputValue" \
    --output text)

echo "Function ARN: $FUNCTION_ARN"
echo ""
echo "Test with:"
echo "  aws lambda invoke --function-name uball-video-extractor \\"
echo "    --payload '{\"chapters\":[{\"s3_key\":\"test.mp4\"}],\"bucket\":\"uball-videos-production\",\"offset_seconds\":0,\"duration_seconds\":60,\"output_s3_key\":\"test-output.mp4\"}' \\"
echo "    --region $REGION output.json"
