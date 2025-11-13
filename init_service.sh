#!/bin/bash
# Cloudflare Tunnel Setup Script for Jetson Nano
# This script installs and configures Cloudflare Tunnel (cloudflared)
# to expose your GoPro API service securely to the internet

set -e

echo "=========================================="
echo "Cloudflare Tunnel Setup for Jetson Nano"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Please run as root (use sudo)"
    exit 1
fi

# Get the actual user
ACTUAL_USER=${SUDO_USER:-$USER}
USER_HOME=$(eval echo ~$ACTUAL_USER)

echo "📋 Configuration:"
echo "   User: $ACTUAL_USER"
echo "   Home: $USER_HOME"
echo ""

# Prompt for Jetson name
read -p "Enter Jetson name (e.g., jetson-1, jetson-2, etc.): " JETSON_NAME
if [ -z "$JETSON_NAME" ]; then
    echo "❌ Jetson name cannot be empty"
    exit 1
fi

echo ""
echo "This will create tunnel: $JETSON_NAME.uai.tech"
echo ""
read -p "Continue? (y/n): " CONTINUE
if [ "$CONTINUE" != "y" ]; then
    echo "Setup cancelled"
    exit 0
fi

# Detect architecture
ARCH=$(uname -m)
echo "📊 Detected architecture: $ARCH"

# Download cloudflared based on architecture
echo "📦 Downloading cloudflared..."
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    # ARM64 for Jetson Nano
    wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64 -O /usr/local/bin/cloudflared
elif [ "$ARCH" = "x86_64" ]; then
    # x86_64 for testing on desktop
    wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared
else
    echo "❌ Unsupported architecture: $ARCH"
    exit 1
fi

chmod +x /usr/local/bin/cloudflared

# Verify installation
if ! command -v cloudflared &> /dev/null; then
    echo "❌ Failed to install cloudflared"
    exit 1
fi

echo "✅ cloudflared installed successfully"
cloudflared --version
echo ""

# Create cloudflared config directory
CLOUDFLARED_DIR="$USER_HOME/.cloudflared"
mkdir -p "$CLOUDFLARED_DIR"
chown -R $ACTUAL_USER:$ACTUAL_USER "$CLOUDFLARED_DIR"

echo "=========================================="
echo "🔐 Cloudflare Authentication Required"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run the following command AS YOUR USER (not root):"
echo ""
echo "   cloudflared tunnel login"
echo ""
echo "2. This will open a browser to authenticate with Cloudflare"
echo "3. Login to your Cloudflare account"
echo "4. Select the domain: uai.tech"
echo "5. Authorize the tunnel"
echo ""
echo "After authentication, run this script again to continue setup"
echo ""
echo "Alternatively, if you already have a tunnel token, continue below..."
echo ""

# Check if already authenticated
if [ -f "$CLOUDFLARED_DIR/cert.pem" ]; then
    echo "✅ Found existing Cloudflare certificate"
    AUTHENTICATED=true
else
    echo "⚠️  No Cloudflare certificate found"
    AUTHENTICATED=false
fi

echo ""
read -p "Do you want to authenticate now? (y/n): " AUTH_NOW

if [ "$AUTH_NOW" = "y" ]; then
    echo ""
    echo "Switching to user $ACTUAL_USER to authenticate..."
    echo "A browser window will open. Please login to Cloudflare."
    echo ""
    sleep 2
    
    # Run as user
    su - $ACTUAL_USER -c "cloudflared tunnel login"
    
    if [ $? -eq 0 ]; then
        echo "✅ Authentication successful"
        AUTHENTICATED=true
    else
        echo "❌ Authentication failed"
        exit 1
    fi
fi

if [ "$AUTHENTICATED" = false ]; then
    echo ""
    echo "⚠️  Setup incomplete. Please authenticate first:"
    echo "   su - $ACTUAL_USER"
    echo "   cloudflared tunnel login"
    echo ""
    echo "Then run this script again."
    exit 0
fi

echo ""
echo "=========================================="
echo "🚇 Creating Cloudflare Tunnel"
echo "=========================================="
echo ""

# Create tunnel (as user)
echo "Creating tunnel: $JETSON_NAME"
su - $ACTUAL_USER -c "cloudflared tunnel create $JETSON_NAME" || {
    echo "⚠️  Tunnel might already exist. Continuing..."
}

# Get tunnel ID
TUNNEL_ID=$(su - $ACTUAL_USER -c "cloudflared tunnel list" | grep "$JETSON_NAME" | awk '{print $1}')

if [ -z "$TUNNEL_ID" ]; then
    echo "❌ Failed to create or find tunnel"
    exit 1
fi

echo "✅ Tunnel created with ID: $TUNNEL_ID"
echo ""

# Create tunnel configuration
echo "📝 Creating tunnel configuration..."

cat > "$CLOUDFLARED_DIR/config.yml" << EOF
tunnel: $TUNNEL_ID
credentials-file: $CLOUDFLARED_DIR/$TUNNEL_ID.json

ingress:
  # Route for this specific Jetson
  - hostname: $JETSON_NAME.uai.tech
    service: http://localhost:5000
    originRequest:
      noTLSVerify: true
  
  # Catch-all rule (required)
  - service: http_status:404
EOF

chown $ACTUAL_USER:$ACTUAL_USER "$CLOUDFLARED_DIR/config.yml"

echo "✅ Configuration created at: $CLOUDFLARED_DIR/config.yml"
echo ""

# Create DNS route
echo "🌐 Creating DNS route..."
su - $ACTUAL_USER -c "cloudflared tunnel route dns $JETSON_NAME $JETSON_NAME.uai.tech" || {
    echo "⚠️  DNS route might already exist. Continuing..."
}
echo ""

# Install as a service
echo "⚙️  Installing cloudflared as a system service..."

cloudflared service install

# Create systemd service file with proper user
cat > /etc/systemd/system/cloudflared.service << EOF
[Unit]
Description=Cloudflare Tunnel
After=network.target

[Service]
Type=simple
User=$ACTUAL_USER
ExecStart=/usr/local/bin/cloudflared tunnel --config $CLOUDFLARED_DIR/config.yml run
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reload

# Enable and start service
echo "🚀 Starting cloudflared service..."
systemctl enable cloudflared
systemctl restart cloudflared

# Wait for service to start
sleep 3

# Check status
echo ""
echo "=========================================="
echo "📊 Service Status"
echo "=========================================="
systemctl status cloudflared --no-pager -l || true
echo ""

# Display tunnel info
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "🎉 Your Jetson Nano is now accessible at:"
echo "   https://$JETSON_NAME.uai.tech"
echo ""
echo "📍 Tunnel Details:"
echo "   Tunnel Name: $JETSON_NAME"
echo "   Tunnel ID: $TUNNEL_ID"
echo "   Local Service: http://localhost:5000"
echo "   Public URL: https://$JETSON_NAME.uai.tech"
echo ""
echo "🔧 Useful Commands:"
echo "   Check status:     sudo systemctl status cloudflared"
echo "   View logs:        sudo journalctl -u cloudflared -f"
echo "   Restart:          sudo systemctl restart cloudflared"
echo "   Stop:             sudo systemctl stop cloudflared"
echo "   Tunnel info:      cloudflared tunnel info $JETSON_NAME"
echo "   List tunnels:     cloudflared tunnel list"
echo ""
echo "🧪 Test your endpoint:"
echo "   curl https://$JETSON_NAME.uai.tech/health"
echo ""
echo "⏭️  Next Steps:"
echo "1. Test the URL in your browser"
echo "2. Update your Firebase web app to use this URL"
echo "3. Repeat this setup on other Jetson Nanos (jetson-2, jetson-3, jetson-4)"
echo ""
echo "📝 GoDaddy DNS Configuration:"
echo "   You don't need to configure anything in GoDaddy!"
echo "   Cloudflare automatically manages the DNS records."
echo "   Just make sure uai.tech nameservers point to Cloudflare."
echo ""