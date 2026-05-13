#!/usr/bin/env bash
# Roll out `ANGLES_TO_PROCESS=FL,FR,NL,NR` to all production Jetsons so the
# V1 CV shot-detection pipeline has all four camera angles to fuse.
#
# Usage: bash scripts/cv_infra/enable-nl-nr-on-jetsons.sh
#
# Requires:
#   - Tailscale up (the Jetsons are addressed by their Tailscale magic-DNS names)
#   - SSH key configured for the `uball@` account on each Jetson
#       (default: ~/.ssh/id_rsa or override via $UBALL_SSH_KEY)
#
# The script is idempotent: if ANGLES_TO_PROCESS is already correct it skips
# the rewrite. Backups of /home/uball/gopro-automation-linux/.env are kept as
# .env.bak-<timestamp>.

set -euo pipefail

JETSONS=(
  jetson-nano-001
  jetson-nano-002
)
SSH_USER="${UBALL_SSH_USER:-uball}"
SSH_KEY="${UBALL_SSH_KEY:-$HOME/.ssh/id_rsa}"
TARGET_ENV="/home/uball/gopro-automation-linux/.env"
TARGET_VALUE="FL,FR,NL,NR"

if [ -f "$SSH_KEY" ]; then
  SSH_OPTS=(-i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
else
  SSH_OPTS=(-o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  echo "⚠️  SSH key not found at $SSH_KEY — relying on agent/default keys"
fi

for host in "${JETSONS[@]}"; do
  echo ""
  echo "================================================================"
  echo "  $host"
  echo "================================================================"

  if ! ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "echo OK" >/dev/null 2>&1; then
    echo "  ❌ unreachable — check Tailscale + key auth"
    continue
  fi

  # Inspect current value.
  current=$(ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "
    grep '^ANGLES_TO_PROCESS=' '$TARGET_ENV' 2>/dev/null || echo 'ANGLES_TO_PROCESS=__UNSET__'
  ")
  echo "  current: $current"

  if [ "$current" = "ANGLES_TO_PROCESS=$TARGET_VALUE" ]; then
    echo "  ✅ already correct, no change"
    continue
  fi

  # Rewrite atomically + backup.
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" "
    set -e
    cp '$TARGET_ENV' '$TARGET_ENV.bak-\$(date +%Y%m%d-%H%M%S)'
    if grep -q '^ANGLES_TO_PROCESS=' '$TARGET_ENV'; then
      sed -i 's|^ANGLES_TO_PROCESS=.*|ANGLES_TO_PROCESS=$TARGET_VALUE|' '$TARGET_ENV'
    else
      printf '\n# Added by enable-nl-nr-on-jetsons.sh — V1 CV pipeline needs all 4 angles\nANGLES_TO_PROCESS=$TARGET_VALUE\n' >> '$TARGET_ENV'
    fi
    echo \"  → new value: \$(grep ^ANGLES_TO_PROCESS= '$TARGET_ENV')\"
  "

  echo ""
  echo "  ⚠️  Restart the gopro-automation service so it picks up the new env:"
  echo "      ssh $SSH_USER@$host 'sudo systemctl restart gopro-automation'"
  echo "  (Or whatever the service manager is — service unit name may differ.)"
done

echo ""
echo "================================================================"
echo "  DONE"
echo "================================================================"
echo "Next recording session on each Jetson will include NL + NR streams,"
echo "which uploads as 4 angle .mp4 files to S3 — feeding the CV pipeline."
