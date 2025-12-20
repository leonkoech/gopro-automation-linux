#!/bin/bash
# Complete Jetson Nano Setup Script
# Sets up existing Flask app as service + Cloudflare Tunnel

set -e

echo "=========================================="
echo "Complete Jetson Nano Setup"
echo "Flask Service + Cloudflare Tunnel"
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

# Prompt for Flask app directory
read -p "Enter path to your Flask app directory (e.g., $USER_HOME/gopro-controller): " APP_DIR
if [ -z "$APP_DIR" ]; then
    echo "❌ Flask app directory cannot be empty"
    exit 1
fi

# Expand home directory if needed
APP_DIR="${APP_DIR/#\~/$USER_HOME}"

# Check if directory exists
if [ ! -d "$APP_DIR" ]; then
    echo "❌ Directory does not exist: $APP_DIR"
    exit 1
fi

# Prompt for Flask app file name
read -p "Enter Flask app filename (default: app.py): " FLASK_FILE
FLASK_FILE=${FLASK_FILE:-app.py}

# Check if Flask app exists
if [ ! -f "$APP_DIR/$FLASK_FILE" ]; then
    echo "❌ Flask app not found: $APP_DIR/$FLASK_FILE"
    exit 1
fi

echo "✅ Found Flask app: $APP_DIR/$FLASK_FILE"

# Check for virtual environment
if [ -d "$APP_DIR/venv" ]; then
    echo "✅ Found virtual environment: $APP_DIR/venv"
    USE_VENV=true
else
    echo "⚠️  No virtual environment found at $APP_DIR/venv"
    USE_VENV=false
fi

# Check for requirements.txt
if [ -f "$APP_DIR/requirements.txt" ]; then
    echo "✅ Found requirements.txt"
else
    echo "⚠️  No requirements.txt found"
fi

echo ""

# Prompt for Jetson name
read -p "Enter Jetson name (e.g., jetson-1): " JETSON_NAME
if [ -z "$JETSON_NAME" ]; then
    echo "❌ Jetson name cannot be empty"
    exit 1
fi

# Prompt for port
read -p "Enter Flask port (default: 5000): " FLASK_PORT
FLASK_PORT=${FLASK_PORT:-5000}

echo ""
echo "Configuration Summary:"
echo "   Flask app:     $APP_DIR/$FLASK_FILE"
echo "   Port:          $FLASK_PORT"
echo "   Jetson name:   $JETSON_NAME"
echo "   Public URL:    https://$JETSON_NAME.uai.tech"
echo ""
read -p "Continue? (y/n): " CONTINUE
if [ "$CONTINUE" != "y" ]; then
    echo "Setup cancelled"
    exit 0
fi

echo ""
echo "=========================================="
echo "Part 1: Setting up Flask Service"
echo "=========================================="
echo ""

# Install dependencies if needed
echo "📦 Installing Python dependencies..."
if [ -d "$VENV_PATH" ]; then
    echo "✅ Using virtual environment: $VENV_PATH"
    
    # Check for requirements.txt
    if [ -f "$APP_DIR/requirements.txt" ]; then
        echo "📄 Found requirements.txt, installing dependencies..."
        $VENV_PATH/bin/pip install -r "$APP_DIR/requirements.txt" || {
            echo "⚠️  Some dependencies failed to install, continuing..."
        }
    else
        echo "⚠️  No requirements.txt found, installing basic dependencies..."
        $VENV_PATH/bin/pip install -q flask flask-cors 2>/dev/null || echo "Dependencies already installed"
    fi
else
    echo "⚠️  No venv found, using system Python"
    
    if [ -f "$APP_DIR/requirements.txt" ]; then
        echo "📄 Found requirements.txt, installing dependencies..."
        pip3 install -r "$APP_DIR/requirements.txt" || {
            echo "⚠️  Some dependencies failed to install, continuing..."
        }
    else
        pip3 install -q flask flask-cors || echo "Dependencies already installed"
    fi
fi

# Create .env file if it doesn't exist
if [ ! -f "$APP_DIR/.env" ]; then
    echo "📄 Creating .env file..."
    cat > "$APP_DIR/.env" << EOF
FLASK_APP=$FLASK_FILE
FLASK_ENV=production
EOF
    chown $ACTUAL_USER:$ACTUAL_USER "$APP_DIR/.env"
fi

# Check for virtual environment
VENV_PATH="$APP_DIR/venv"
if [ -d "$VENV_PATH" ]; then
    echo "✅ Found virtual environment: $VENV_PATH"
    PYTHON_EXEC="$VENV_PATH/bin/python3"
    FLASK_EXEC="$VENV_PATH/bin/flask"
else
    echo "⚠️  No virtual environment found, using system Python"
    PYTHON_EXEC="/usr/bin/python3"
    FLASK_EXEC="python3 -m flask"
fi

# Create or update systemd service
echo "⚙️  Creating systemd service..."
cat > /etc/systemd/system/gopro-controller.service << EOF
[Unit]
Description=GoPro Controller Flask API Service
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$APP_DIR
Environment="FLASK_APP=$FLASK_FILE"
Environment="FLASK_ENV=production"
Environment="PATH=$VENV_PATH/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$FLASK_EXEC run --host=0.0.0.0 --port=$FLASK_PORT
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
echo "🔄 Reloading systemd..."
systemctl daemon-reload

# Enable service
echo "✅ Enabling service to start on boot..."
systemctl enable gopro-controller.service

# Restart service
echo "▶️  Starting/restarting service..."
systemctl restart gopro-controller.service

# Wait for service to start
sleep 3

# Check if service is running
if systemctl is-active --quiet gopro-controller; then
    echo "✅ Flask service is running"
else
    echo "⚠️  Flask service may have issues. Check logs:"
    echo "   sudo journalctl -u gopro-controller -n 50"
    read -p "Continue with Cloudflare setup anyway? (y/n): " CONTINUE_CF
    if [ "$CONTINUE_CF" != "y" ]; then
        exit 1
    fi
fi

echo ""
echo "=========================================="
echo "Part 2: Installing Cloudflare Tunnel"
echo "=========================================="
echo ""

# Detect architecture
ARCH=$(uname -m)
echo "📊 Detected architecture: $ARCH"

# Check if cloudflared is already installed
if command -v cloudflared &> /dev/null; then
    echo "✅ cloudflared already installed"
    cloudflared --version
else
    # Download cloudflared
    echo "📦 Downloading cloudflared..."
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
        wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64 -O /usr/local/bin/cloudflared
    elif [ "$ARCH" = "x86_64" ]; then
        wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared
    else
        echo "❌ Unsupported architecture: $ARCH"
        exit 1
    fi
    
    chmod +x /usr/local/bin/cloudflared
    echo "✅ cloudflared installed"
    cloudflared --version
fi

echo ""

# Create cloudflared config directory
CLOUDFLARED_DIR="$USER_HOME/.cloudflared"
mkdir -p "$CLOUDFLARED_DIR"
chown -R $ACTUAL_USER:$ACTUAL_USER "$CLOUDFLARED_DIR"

# Check authentication
echo "=========================================="
echo "🔐 Cloudflare Authentication"
echo "=========================================="
echo ""

if [ -f "$CLOUDFLARED_DIR/cert.pem" ]; then
    echo "✅ Found existing Cloudflare certificate"
    AUTHENTICATED=true
else
    echo "⚠️  No Cloudflare certificate found"
    echo ""
    echo "You need to authenticate with Cloudflare."
    echo "This will open a browser window where you'll:"
    echo "  1. Login to Cloudflare"
    echo "  2. Select domain: uai.tech"
    echo "  3. Authorize the tunnel"
    echo ""
    read -p "Authenticate now? (y/n): " AUTH_NOW
    
    if [ "$AUTH_NOW" = "y" ]; then
        echo ""
        echo "Switching to user $ACTUAL_USER..."
        echo "A browser window will open..."
        sleep 2
        su - $ACTUAL_USER -c "cloudflared tunnel login"
        
        if [ $? -eq 0 ]; then
            echo "✅ Authentication successful"
            AUTHENTICATED=true
        else
            echo "❌ Authentication failed"
            AUTHENTICATED=false
        fi
    else
        AUTHENTICATED=false
    fi
fi

if [ "$AUTHENTICATED" = false ]; then
    echo ""
    echo "=========================================="
    echo "⚠️  Setup Incomplete"
    echo "=========================================="
    echo ""
    echo "Flask service is running, but Cloudflare Tunnel is not configured."
    echo ""
    echo "To complete setup later:"
    echo "  1. Authenticate:"
    echo "     su - $ACTUAL_USER -c 'cloudflared tunnel login'"
    echo ""
    echo "  2. Run this command to finish setup:"
    echo "     sudo $0"
    echo ""
    echo "Flask service info:"
    echo "   Status: sudo systemctl status gopro-controller"
    echo "   Logs:   sudo journalctl -u gopro-controller -f"
    echo "   Local:  curl http://localhost:$FLASK_PORT/health"
    echo ""
    exit 0
fi

echo ""
echo "🚇 Creating tunnel: $JETSON_NAME"

# Create tunnel
su - $ACTUAL_USER -c "cloudflared tunnel create $JETSON_NAME" 2>/dev/null || {
    echo "⚠️  Tunnel may already exist. Checking..."
}

# Get tunnel ID
TUNNEL_ID=$(su - $ACTUAL_USER -c "cloudflared tunnel list" | grep "$JETSON_NAME" | awk '{print $1}')

if [ -z "$TUNNEL_ID" ]; then
    echo "❌ Failed to create or find tunnel: $JETSON_NAME"
    echo ""
    echo "Existing tunnels:"
    su - $ACTUAL_USER -c "cloudflared tunnel list"
    exit 1
fi

echo "✅ Tunnel ID: $TUNNEL_ID"

# Create or update config
echo "📝 Creating tunnel configuration..."
cat > "$CLOUDFLARED_DIR/config.yml" << EOF
tunnel: $TUNNEL_ID
credentials-file: $CLOUDFLARED_DIR/$TUNNEL_ID.json

ingress:
  - hostname: $JETSON_NAME.uai.tech
    service: http://localhost:$FLASK_PORT
    originRequest:
      noTLSVerify: true
  - service: http_status:404
EOF

chown $ACTUAL_USER:$ACTUAL_USER "$CLOUDFLARED_DIR/config.yml"

# Create DNS route
echo "🌐 Creating DNS route..."
su - $ACTUAL_USER -c "cloudflared tunnel route dns $JETSON_NAME $JETSON_NAME.uai.tech" 2>/dev/null || {
    echo "⚠️  DNS route may already exist"
}

# Install or update cloudflared service
echo "⚙️  Installing cloudflared service..."

# Create systemd service
cat > /etc/systemd/system/cloudflared.service << EOF
[Unit]
Description=Cloudflare Tunnel - $JETSON_NAME
After=network.target
Wants=network-online.target

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
echo "▶️  Starting cloudflared service..."
systemctl enable cloudflared
systemctl restart cloudflared

# Wait for tunnel to connect
sleep 5

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "🎉 Your Jetson is now accessible at:"
echo "   https://$JETSON_NAME.uai.tech"
echo ""
echo "📂 Configuration:"
echo "   Flask app:         $APP_DIR/$FLASK_FILE"
echo "   Flask port:        $FLASK_PORT"
echo "   Tunnel config:     $CLOUDFLARED_DIR/config.yml"
echo ""
echo "📊 Services Status:"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Flask Service (gopro-controller):"
systemctl status gopro-controller --no-pager -l | head -5
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Cloudflare Tunnel:"
systemctl status cloudflared --no-pager -l | head -5
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🧪 Test Commands:"
echo "   # Local test:"
echo "   curl http://localhost:$FLASK_PORT/health"
echo ""
echo "   # Public test (wait 30 seconds for DNS):"
echo "   curl https://$JETSON_NAME.uai.tech/health"
echo ""
echo "📝 Useful Commands:"
echo ""
echo "   # Edit your Flask app:"
echo "   nano $APP_DIR/$FLASK_FILE"
echo ""
echo "   # Restart Flask after changes:"
echo "   sudo systemctl restart gopro-controller"
echo ""
echo "   # Check service status:"
echo "   sudo systemctl status gopro-controller"
echo "   sudo systemctl status cloudflared"
echo ""
echo "   # View logs:"
echo "   sudo journalctl -u gopro-controller -f"
echo "   sudo journalctl -u cloudflared -f"
echo ""
echo "   # Stop/Start services:"
echo "   sudo systemctl stop gopro-controller"
echo "   sudo systemctl start gopro-controller"
echo "   sudo systemctl restart cloudflared"
echo ""
echo "   # Disable auto-start on boot:"
echo "   sudo systemctl disable gopro-controller"
echo "   sudo systemctl disable cloudflared"
echo ""
echo "🌐 Cloudflare Tunnel Info:"
echo "   Tunnel name:       $JETSON_NAME"
echo "   Tunnel ID:         $TUNNEL_ID"
echo "   Public URL:        https://$JETSON_NAME.uai.tech"
echo "   Local service:     http://localhost:$FLASK_PORT"
echo ""
echo "   # View all tunnels:"
echo "   cloudflared tunnel list"
echo ""
echo "   # Get tunnel info:"
echo "   cloudflared tunnel info $JETSON_NAME"
echo ""
