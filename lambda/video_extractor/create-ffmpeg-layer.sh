#!/bin/bash
# Create FFmpeg Lambda Layer
#
# This script creates a Lambda layer containing a statically compiled FFmpeg binary.
# The layer provides /opt/bin/ffmpeg and /opt/bin/ffprobe.
#
# Prerequisites:
# - Docker (for building in Amazon Linux environment)
# - AWS CLI configured
#
# Usage:
#   ./create-ffmpeg-layer.sh              # Create layer in us-east-1
#   ./create-ffmpeg-layer.sh us-west-2    # Create layer in specific region

set -e

REGION="${1:-us-east-1}"
LAYER_NAME="ffmpeg"

echo "=== Creating FFmpeg Lambda Layer ==="
echo "Region: $REGION"
echo ""

# Option 1: Use pre-built FFmpeg layer from public source
# This is faster and recommended for most use cases
echo "Option 1: Use public FFmpeg layer (recommended)"
echo ""
echo "Add this ARN to your Lambda function:"
echo "  arn:aws:lambda:us-east-1:678705476278:layer:ffmpeg:1"
echo ""
echo "Or for other regions, search for 'ffmpeg lambda layer' on:"
echo "  https://github.com/serverlessrepo/ffmpeg-lambda-layer"
echo ""

# Option 2: Build your own layer (if you need specific FFmpeg options)
read -p "Do you want to build your own layer instead? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Using public layer. Update template.yaml with the public layer ARN."
    exit 0
fi

echo ""
echo "Building custom FFmpeg layer..."

# Create temp directory
TEMP_DIR=$(mktemp -d)
LAYER_DIR="$TEMP_DIR/layer"
mkdir -p "$LAYER_DIR/bin"

# Download pre-compiled static FFmpeg for Amazon Linux
echo "Downloading static FFmpeg build..."
curl -L -o "$TEMP_DIR/ffmpeg.tar.xz" \
    "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"

# Extract
echo "Extracting..."
tar -xf "$TEMP_DIR/ffmpeg.tar.xz" -C "$TEMP_DIR"
FFMPEG_DIR=$(ls -d "$TEMP_DIR"/ffmpeg-*-amd64-static)

# Copy binaries
cp "$FFMPEG_DIR/ffmpeg" "$LAYER_DIR/bin/"
cp "$FFMPEG_DIR/ffprobe" "$LAYER_DIR/bin/"
chmod +x "$LAYER_DIR/bin/"*

# Create layer zip
echo "Creating layer zip..."
cd "$LAYER_DIR"
zip -r9 "$TEMP_DIR/ffmpeg-layer.zip" .

# Publish layer
echo "Publishing layer to AWS..."
LAYER_VERSION=$(aws lambda publish-layer-version \
    --layer-name "$LAYER_NAME" \
    --description "FFmpeg static binary for video processing" \
    --zip-file "fileb://$TEMP_DIR/ffmpeg-layer.zip" \
    --compatible-runtimes python3.9 python3.10 python3.11 python3.12 \
    --compatible-architectures x86_64 \
    --region "$REGION" \
    --query 'Version' \
    --output text)

LAYER_ARN="arn:aws:lambda:$REGION:$(aws sts get-caller-identity --query Account --output text):layer:$LAYER_NAME:$LAYER_VERSION"

# Cleanup
rm -rf "$TEMP_DIR"

echo ""
echo "=== Layer Created ==="
echo "Layer ARN: $LAYER_ARN"
echo ""
echo "Update template.yaml Layers section with this ARN."
