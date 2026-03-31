#!/bin/bash
# Register a new revision of the ffmpeg-extract-transcode job definition
# with the v2 command that includes duration directives and output-level seeking.
#
# This creates a new revision (e.g., ffmpeg-extract-transcode:2) while keeping
# the old revision active. Update AWS_BATCH_JOB_DEFINITION_EXTRACT env var
# on the Jetson to point to the new revision.
#
# Usage: ./scripts/register_batch_job_v2.sh

set -e

# Read the v2 command script and convert to a single inline command
# (AWS Batch expects the command as ["/bin/bash", "-c", "...inline script..."])
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INLINE_CMD=$(cat "$SCRIPT_DIR/batch_extract_transcode_command.sh" | \
    # Remove shebang and comments at the top
    sed '/^#!/d' | \
    # Remove comment-only lines (but keep inline comments)
    sed '/^[[:space:]]*#/d' | \
    # Remove empty lines
    sed '/^[[:space:]]*$/d' | \
    # Join lines with ' && ' for shell execution
    tr '\n' '\n' | \
    # Convert to single line with && between commands
    paste -sd'\n' -)

echo "=== Registering new ffmpeg-extract-transcode job definition ==="
echo ""

# Get current job definition details
echo "Fetching current job definition..."
CURRENT_JD=$(aws batch describe-job-definitions \
    --job-definition-name ffmpeg-extract-transcode \
    --status ACTIVE \
    --query 'jobDefinitions[-1]' \
    --output json)

# Extract current properties
IMAGE=$(echo "$CURRENT_JD" | jq -r '.containerProperties.image')
VCPUS=$(echo "$CURRENT_JD" | jq -r '.containerProperties.vcpus')
MEMORY=$(echo "$CURRENT_JD" | jq -r '.containerProperties.memory')
JOB_ROLE=$(echo "$CURRENT_JD" | jq -r '.containerProperties.jobRoleArn')
EXEC_ROLE=$(echo "$CURRENT_JD" | jq -r '.containerProperties.executionRoleArn')
LOG_CONFIG=$(echo "$CURRENT_JD" | jq '.containerProperties.logConfiguration')
RESOURCE_REQS=$(echo "$CURRENT_JD" | jq '.containerProperties.resourceRequirements')
CURRENT_REV=$(echo "$CURRENT_JD" | jq -r '.revision')

echo "Current revision: $CURRENT_REV"
echo "Image: $IMAGE"
echo "vCPUs: $VCPUS, Memory: ${MEMORY}MB"
echo ""

# Build the new command as JSON array
# Read the script file and create a proper inline command
SCRIPT_CONTENT=$(cat "$SCRIPT_DIR/batch_extract_transcode_command.sh")

# Register new revision
echo "Registering new revision..."
NEW_JD=$(aws batch register-job-definition \
    --job-definition-name ffmpeg-extract-transcode \
    --type container \
    --container-properties "{
        \"image\": \"$IMAGE\",
        \"vcpus\": $VCPUS,
        \"memory\": $MEMORY,
        \"jobRoleArn\": \"$JOB_ROLE\",
        \"executionRoleArn\": \"$EXEC_ROLE\",
        \"command\": [\"/bin/bash\", \"-c\", $(echo "$SCRIPT_CONTENT" | jq -Rs .)],
        \"resourceRequirements\": $RESOURCE_REQS,
        \"logConfiguration\": $LOG_CONFIG
    }")

NEW_REV=$(echo "$NEW_JD" | jq -r '.revision')
NEW_ARN=$(echo "$NEW_JD" | jq -r '.jobDefinitionArn')

echo ""
echo "=== SUCCESS ==="
echo "New revision: ffmpeg-extract-transcode:$NEW_REV"
echo "ARN: $NEW_ARN"
echo ""
echo "To use the new version, update the Jetson env var:"
echo "  export AWS_BATCH_JOB_DEFINITION_EXTRACT=ffmpeg-extract-transcode:$NEW_REV"
echo ""
echo "Or update in the Jetson's .env file and restart the service."
