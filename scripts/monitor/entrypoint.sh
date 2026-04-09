#!/bin/bash
set -e

# Start Tailscale daemon in userspace mode (no TUN needed in containers)
tailscaled --tun=userspace-networking --state=/tmp/tailscale-state &
sleep 2

# Authenticate with the auth key (ephemeral = auto-removes on disconnect)
tailscale up --authkey="${TAILSCALE_AUTH_KEY}" --hostname="northflank-monitor"

# Wait for Tailscale to establish connections
sleep 3

# Run the monitoring script
python3 /app/monitor_jetsons.py

# Clean disconnect
tailscale down
