#!/usr/bin/env bash
# Set CV_PLAYS_ENABLED on both Jetsons + local Mac .env, restart Jetson services.
#
# Usage: bash scripts/cv_infra/set-cv-plays-enabled.sh [true|false]
#        (default: false — safe shadow-mode behavior; recommended until the
#         V1 far-angle model retrain is validated)
#
# Requires:
#   - Tailscale up (Jetsons reachable via their Tailscale IPs)
#   - SSH key at $UBALL_SSH_KEY (default: ./id_rsa in the repo root)
#   - $JETSON_SUDO_PASSWORD env var set (for systemctl restart)
#
# What this does:
#   1. Validates desired value (true|false only)
#   2. Updates /Users/rohitkale/Cellstrat/GitHub_Repositories/gopro-automation-linux/.env
#      (your local working copy — adds CV_PLAYS_ENABLED line if absent)
#   3. For each Jetson:
#      a. SSH-tests reachability
#      b. Backs up /home/developer/Development/gopro-automation-linux/.env
#      c. git pull origin main (gets the plays_sync filter code)
#      d. Rewrites/appends CV_PLAYS_ENABLED in the Jetson .env
#      e. Restarts gopro-controller.service via sudo
#      f. Reports service status
#
# Idempotent: re-running with the same value skips the rewrite but still
# pulls + restarts (safe as a periodic "get to latest").

set -euo pipefail

VALUE="${1:-false}"
case "$VALUE" in
  true|false) ;;
  *)
    echo "ERROR: argument must be 'true' or 'false' (got: '$VALUE')"
    exit 1
    ;;
esac

# ---- targets ----
JETSONS=(
  "100.106.30.98:jetson-nano-001"
  "100.87.190.71:jetson-nano-002"
)
SSH_USER="${UBALL_SSH_USER:-developer}"
SSH_KEY="${UBALL_SSH_KEY:-$(git rev-parse --show-toplevel)/id_rsa}"
JETSON_REPO_DIR="/home/developer/Development/gopro-automation-linux"
SERVICE="gopro-controller.service"
LOCAL_ENV="$(git rev-parse --show-toplevel)/.env"

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


# ---- helper: idempotent in-place sed on a .env file ----
# args: target_file value
write_env_line() {
  local target="$1"
  local val="$2"
  if [ ! -f "$target" ]; then
    echo "ERROR: .env not found at $target"
    return 1
  fi
  if grep -q '^CV_PLAYS_ENABLED=' "$target"; then
    if grep -q "^CV_PLAYS_ENABLED=${val}\$" "$target"; then
      echo "  CV_PLAYS_ENABLED already = ${val} (no change)"
      return 0
    fi
    # macOS sed needs -i ''; Linux sed needs just -i. Use a temp-file dance for portability.
    cp "$target" "$target.tmp.$$"
    sed "s|^CV_PLAYS_ENABLED=.*|CV_PLAYS_ENABLED=${val}|" "$target.tmp.$$" > "$target"
    rm -f "$target.tmp.$$"
    echo "  CV_PLAYS_ENABLED rewritten to ${val}"
  else
    printf '\n# V1 CV-plays kill-switch (added by set-cv-plays-enabled.sh)\nCV_PLAYS_ENABLED=%s\n' "$val" >> "$target"
    echo "  CV_PLAYS_ENABLED appended (${val})"
  fi
}


# ---- 1. local Mac .env ----
echo ""
echo "================================================================"
echo "  LOCAL  ($LOCAL_ENV)"
echo "================================================================"
if [ -f "$LOCAL_ENV" ]; then
  cp "$LOCAL_ENV" "$LOCAL_ENV.bak-pre-cv-plays-$(date -u +%Y%m%d-%H%M%S)"
  write_env_line "$LOCAL_ENV" "$VALUE"
else
  echo "  WARNING: local $LOCAL_ENV not found, skipping local update"
fi


# ---- 2. each Jetson ----
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

  # Heredoc with VAR substitution on the local side, executed remotely.
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" bash -s -- "$VALUE" <<'REMOTE_SCRIPT'
    set -e
    val="$1"
    cd /home/developer/Development/gopro-automation-linux

    echo "  --- before ---"
    echo "  branch: $(git rev-parse --abbrev-ref HEAD)"
    echo "  head:   $(git log --oneline -1)"
    echo "  flag:   $(grep '^CV_PLAYS_ENABLED=' .env 2>/dev/null || echo '(unset)')"

    cp .env .env.bak-pre-cv-plays-$(date -u +%Y%m%d-%H%M%S)
    git pull --ff-only origin main 2>&1 | tail -3

    if grep -q '^CV_PLAYS_ENABLED=' .env; then
      if grep -q "^CV_PLAYS_ENABLED=${val}\$" .env; then
        echo "  CV_PLAYS_ENABLED already = ${val} (no change)"
      else
        sed -i "s|^CV_PLAYS_ENABLED=.*|CV_PLAYS_ENABLED=${val}|" .env
        echo "  CV_PLAYS_ENABLED rewritten to ${val}"
      fi
    else
      printf '\n# V1 CV-plays kill-switch (added by set-cv-plays-enabled.sh)\nCV_PLAYS_ENABLED=%s\n' "$val" >> .env
      echo "  CV_PLAYS_ENABLED appended (${val})"
    fi
REMOTE_SCRIPT

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
echo "  DONE  (CV_PLAYS_ENABLED=${VALUE})"
echo "================================================================"
echo "Expected behavior:"
if [ "$VALUE" = "false" ]; then
  echo "  • plays_sync filters out CV-emitted events (payload.source == 'cv')"
  echo "  • operator scoreboard events still create plays as today"
  echo "  • CV pipeline keeps running for diagnostics (cv_logs_staging)"
else
  echo "  • CV-emitted events flow into Supabase plays with source='cv'"
  echo "  • each play in the annotation tool will show the (CV) badge"
  echo "  • V1 far-angle accuracy applies — monitor before scaling up"
fi
