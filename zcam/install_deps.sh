#!/bin/bash
# Z-CAM Streaming Pipeline — Dependency Installer
# Run on Jetson Nano (JetPack) or Orange Pi 3 LTE (Ubuntu/Armbian)
#
# Usage: sudo bash zcam/install_deps.sh

set -e

echo "=== Z-CAM Dependency Installer ==="

# Detect platform
PLATFORM="unknown"
if [ -e /dev/nvhost-nvenc ] || grep -qi "jetson\|tegra" /proc/device-tree/model 2>/dev/null; then
    PLATFORM="jetson"
    echo "Detected: Jetson Nano (NVENC available)"
else
    PLATFORM="orangepi"
    echo "Detected: Orange Pi / Generic ARM (software encoding)"
fi

echo ""
echo "=== Installing system packages ==="
apt-get update

# GStreamer core + plugins
apt-get install -y \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    python3-gi \
    python3-gst-1.0 \
    python3-pip \
    python3-dev \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev

# Platform-specific
if [ "$PLATFORM" = "jetson" ]; then
    echo ""
    echo "=== Jetson: Verifying NVENC ==="
    if gst-inspect-1.0 nvv4l2h264enc > /dev/null 2>&1; then
        echo "✓ nvv4l2h264enc available"
    else
        echo "⚠ nvv4l2h264enc NOT found — ensure JetPack 4.6+ is installed"
    fi
else
    echo ""
    echo "=== Orange Pi: Installing x264 encoder ==="
    apt-get install -y x264
    if gst-inspect-1.0 x264enc > /dev/null 2>&1; then
        echo "✓ x264enc available"
    else
        echo "⚠ x264enc NOT found — check gstreamer1.0-plugins-ugly installation"
    fi
fi

echo ""
echo "=== Installing Python packages ==="
pip3 install --upgrade pip
pip3 install boto3 flask flask-cors requests opencv-python-headless numpy python-dotenv

echo ""
echo "=== NDI SDK ==="
echo "The NDI GStreamer plugin (gst-plugin-ndi) must be installed separately."
echo "Options:"
echo "  1. Install from Teltek: https://github.com/teltek/gst-plugin-ndi"
echo "  2. Build from NDI SDK: https://ndi.tv/sdk/"
echo ""
echo "After installing, verify with: gst-inspect-1.0 ndisrc"

echo ""
echo "=== Amazon KVS GStreamer Plugin ==="
echo "The kvssink GStreamer element must be compiled from source for ARM."
echo "Follow the official guide:"
echo "  https://docs.aws.amazon.com/kinesisvideostreams/latest/dg/producer-sdk-cpp.html"
echo ""
echo "Build steps (takes 20-40 min on ARM):"
echo "  git clone https://github.com/awslabs/amazon-kinesis-video-streams-producer-sdk-cpp.git"
echo "  cd amazon-kinesis-video-streams-producer-sdk-cpp"
echo "  mkdir build && cd build"
echo "  cmake .. -DBUILD_GSTREAMER_PLUGIN=ON"
echo "  make -j\$(nproc)"
echo ""
echo "After building, add to GST_PLUGIN_PATH:"
echo "  export GST_PLUGIN_PATH=\$GST_PLUGIN_PATH:/path/to/build"
echo "  Verify: gst-inspect-1.0 kvssink"

echo ""
echo "=== Creating staging directory ==="
STAGING_DIR="${ZCAM_DOWNLOAD_DIR:-$HOME/zcam_downloads}"
mkdir -p "$STAGING_DIR"
echo "Staging directory: $STAGING_DIR"

echo ""
echo "=== Done ==="
echo "Platform: $PLATFORM"
echo ""
echo "Next steps:"
echo "  1. Install NDI SDK and gst-plugin-ndi"
echo "  2. Build Amazon KVS GStreamer plugin"
echo "  3. Set ZCAM_CAMERAS in your .env file"
echo "  4. Test with: python3 -c 'from zcam.config import load_zcam_config; print(load_zcam_config())'"
