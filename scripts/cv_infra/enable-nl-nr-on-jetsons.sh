#!/usr/bin/env bash
# Roll out `ANGLES_TO_PROCESS=FL,FR,NL,NR` to all production Jetsons so the
# V1 CV shot-detection pipeline has all four camera angles to fuse.
#
# Usage: bash scripts/cv_infra/enable-nl-nr-on-jetsons.sh
#
# Requires:
#   - Tailscale up (Jetsons are addressed by their Tailscale IPs)
#   - SSH key at $UBALL_SSH_KEY (default: ./id_rsa in the repo root)
#   - $JETSON_SUDO_PASSWORD env var set (for systemctl restart)
#
# What this does on each Jetson:
#   1. Backs up /home/developer/Development/gopro-automation-linux/.env
#   2. git pull origin main (so the pulled code matches the new env)
#   3. Rewrites ANGLES_TO_PROCESS (or appends if absent)
#   4. (Best-effort) pip install -r requirements.txt in the venv
#   5. Restarts gopro-controller.service via sudo systemctl
#   6. Reports service status
#
# Idempotent: re-running on an already-configured Jetson skips the env rewrite
# but still pulls + restarts, so it's safe as a periodic "get to latest".

set -euo pipefail

# Tailscale IPs.  NOTE: jetson-nano-001 is reachable via DNS jetson-2.uai.tech
# (names swapped historically — see docs/SESSION_CONTEXT_FOR_TESTING.md).
# Use Tailscale IPs directly to avoid the swap confusion.
JETSONS=(
  "100.106.30.98:jetson-nano-001"
  "100.87.190.71:jetson-nano-002"
)
SSH_USER="${UBALL_SSH_USER:-developer}"
SSH_KEY="${UBALL_SSH_KEY:-$(git rev-parse --show-toplevel)/id_rsa}"
REPO_DIR="/home/developer/Development/gopro-automation-linux"
SERVICE="gopro-controller.service"
TARGET_VALUE="FL,FR,NL,NR"

if [ ! -f "$SSH_KEY" ]; then
  echo "ERROR: SSH key not found at $SSH_KEY"
  echo "       Set UBALL_SSH_KEY to the path of the Jetson developer@ ssh key."
  exit 1
fi

if [ -z "${JETSON_SUDO_PASSWORD:-}" ]; then
  echo "ERROR: JETSON_SUDO_PASSWORD env var not set."
  echo "       Required for the systemctl restart step on each Jetson."
  echo "       export JETSON_SUDO_PASSWORD='<password>' and re-run."
  exit 1
fi

SSH_OPTS=(-i "$SSH_KEY" -o ConnectTimeout=12 -o StrictHostKeyChecking=accept-new)

for entry in "${JETSONS[@]}"; do
  ip="${entry%%:*}"
  name="${entry##*:}"
  echo ""
  echo "================================================================"
  echo "  $name  ($ip)"
  echo "================================================================"

  if ! ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "echo OK" >/dev/null 2>&1; then
    echo "  unreachable — skipping (check Tailscale + key auth)"
    continue
  fi

  ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "
    set -e
    cd '$REPO_DIR'

    echo '  --- before ---'
    echo '  branch: '\$(git rev-parse --abbrev-ref HEAD)
    echo '  head:   '\$(git log --oneline -1)
    echo '  angles: '\$(grep '^ANGLES_TO_PROCESS=' .env 2>/dev/null || echo '(unset)')

    cp .env .env.bak-pre-cv-v1-\$(date -u +%Y%m%d-%H%M%S)
    git pull --ff-only origin main 2>&1 | tail -3

    if grep -q '^ANGLES_TO_PROCESS=' .env; then
      if grep -q '^ANGLES_TO_PROCESS=$TARGET_VALUE\$' .env; then
        echo '  angles already $TARGET_VALUE, no rewrite'
      else
        sed -i 's|^ANGLES_TO_PROCESS=.*|ANGLES_TO_PROCESS=$TARGET_VALUE|' .env
        echo '  angles rewritten to $TARGET_VALUE'
      fi
    else
      printf '\n# V1 CV pipeline: all 4 angles required (Side A=FR+NR, Side B=FL+NL)\nANGLES_TO_PROCESS=$TARGET_VALUE\n' >> .env
      echo '  angles appended ($TARGET_VALUE)'
    fi

    if [ -f requirements.txt ] && [ -d venv ]; then
      source venv/bin/activate
      pip install -q -r requirements.txt 2>&1 | tail -2 || echo '  (pip warnings, continuing)'
    fi
  "

  echo "  restarting $SERVICE..."
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "
    echo '$JETSON_SUDO_PASSWORD' | sudo -S systemctl restart $SERVICE 2>&1 | tail -2
    sleep 4
    echo -n '  service is: '
    echo '$JETSON_SUDO_PASSWORD' | sudo -S systemctl is-active $SERVICE 2>&1
  "
done

echo ""
echo "================================================================"
echo "  DONE"
echo "================================================================"
echo "Next recording session on each Jetson will include NL + NR streams,"
echo "uploading 4 angle .mp4 files to S3 — feeding the V1 CV pipeline."
