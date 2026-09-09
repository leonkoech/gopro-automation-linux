#!/usr/bin/env bash
#
# pi-kiosk-setup.sh — turn a Raspberry Pi into a full-screen browser kiosk.
#
# On boot the Pi logs in automatically and opens ONE URL full-screen in
# Chromium, with no cursor, no toolbars, no screen blanking, and an auto-restart
# if the browser is closed or crashes. The Chromium profile persists, so a login
# you do once survives reboots.
#
#   sudo ./pi-kiosk-setup.sh "https://app.uai.tech/tv-display"
#
# Re-run any time with a different URL to change what it shows:
#   sudo ./pi-kiosk-setup.sh "https://example.com/other"      # then: sudo reboot
#
# Target: Raspberry Pi OS (Bullseye or Bookworm), Lite or Desktop, on a Pi 3/4/5.
# It forces an X11 session (not Wayland) so behaviour is identical across OS
# versions. Wi-Fi / SSH / hostname should already be set (do it in Raspberry Pi
# Imager when you flash the card).
#
# NOTE: if this card currently runs PiSignage, reflash it with plain Raspberry
# Pi OS first — this script does not try to uninstall PiSignage (see README).

set -euo pipefail

# ---------------------------------------------------------------------------
URL="${1:-}"
KIOSK_USER="${KIOSK_USER:-pi}"
ROTATE="${ROTATE:-normal}"   # normal | left | right | inverted  (display_rotate)
# ---------------------------------------------------------------------------

die() { echo "ERROR: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "run with sudo"
[[ -n "$URL" ]] || die "usage: sudo $0 <URL> [ROTATE=left|right|inverted]"
[[ "$URL" =~ ^https?:// ]] || die "URL must start with http:// or https://"
id "$KIOSK_USER" &>/dev/null || die "user '$KIOSK_USER' does not exist (set KIOSK_USER=...)"

if [[ -f /boot/pisignage_version.txt || -f /boot/firmware/pisignage_version.txt || -d /home/pi/pisignage ]]; then
  cat >&2 <<'EOF'
ERROR: this looks like a PiSignage card. This script expects a plain Raspberry
Pi OS install. Reflash the SD card with Raspberry Pi OS (Raspberry Pi Imager),
set Wi-Fi + SSH + hostname in the imager's settings, boot it, then run this
script over SSH. (Or use PiSignage's own "Add Link" asset — see README.)
EOF
  [[ "${FORCE:-}" == "1" ]] || exit 1
  echo "FORCE=1 set — continuing anyway." >&2
fi

HOME_DIR="$(getent passwd "$KIOSK_USER" | cut -d: -f6)"
echo "==> kiosk user: $KIOSK_USER  home: $HOME_DIR"
echo "==> URL:        $URL"

# --- 1. packages ----------------------------------------------------------
echo "==> installing packages (this can take a few minutes)…"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends \
  xserver-xorg xserver-xorg-legacy xinit x11-xserver-utils \
  openbox unclutter ca-certificates

CHROMIUM="$(command -v chromium-browser || command -v chromium || true)"
if [[ -z "$CHROMIUM" ]]; then
  apt-get install -y --no-install-recommends chromium-browser \
    || apt-get install -y --no-install-recommends chromium
  CHROMIUM="$(command -v chromium-browser || command -v chromium)"
fi
echo "==> chromium: $CHROMIUM"

# --- 2. let a normal user start X on tty1 --------------------------------
install -m 644 /dev/stdin /etc/X11/Xwrapper.config <<'EOF'
allowed_users=anybody
needs_root_rights=yes
EOF

# --- 3. console autologin on tty1 --------------------------------------
mkdir -p /etc/systemd/system/getty@tty1.service.d
cat > /etc/systemd/system/getty@tty1.service.d/autologin.conf <<EOF
[Service]
ExecStart=
ExecStart=-/sbin/agetty --autologin ${KIOSK_USER} --noclear %I \$TERM
EOF

# --- 4. persist the URL + display rotation ----------------------------
echo "$URL" > /etc/kiosk-url
chmod 644 /etc/kiosk-url

case "$ROTATE" in
  normal|left|right|inverted) ;;
  *) die "ROTATE must be normal|left|right|inverted" ;;
esac
BOOTCFG=/boot/firmware/config.txt; [[ -f $BOOTCFG ]] || BOOTCFG=/boot/config.txt
declare -A RMAP=( [normal]=0 [right]=1 [inverted]=2 [left]=3 )
sed -i '/^display_rotate=/d' "$BOOTCFG"
echo "display_rotate=${RMAP[$ROTATE]}" >> "$BOOTCFG"

# --- 5. the kiosk launcher (openbox autostart) -----------------------
install -d -o "$KIOSK_USER" -g "$KIOSK_USER" \
  "$HOME_DIR/.config" "$HOME_DIR/.config/openbox"

cat > "$HOME_DIR/.xinitrc" <<'EOF'
#!/bin/sh
exec openbox-session
EOF

cat > "$HOME_DIR/.config/openbox/autostart" <<'EOF'
# --- kiosk autostart -------------------------------------------------
URL="$(cat /etc/kiosk-url 2>/dev/null || echo about:blank)"
CHROMIUM="$(command -v chromium-browser || command -v chromium)"
PROFILE="$HOME/.config/kiosk-chromium"

# no screen blanking / power saving, hide the cursor
xset s off -dpms s noblank &
unclutter -idle 0.5 -root &

# clear the "Chrome didn't shut down correctly" / restore-tabs prompt
mkdir -p "$PROFILE/Default"
sed -i 's/"exited_cleanly":false/"exited_cleanly":true/; s/"exit_type":"[^"]\+"/"exit_type":"Normal"/' \
  "$PROFILE/Default/Preferences" 2>/dev/null || true

# keep it up: relaunch if Chromium is closed or crashes
while true; do
  "$CHROMIUM" \
    --user-data-dir="$PROFILE" \
    --kiosk --start-fullscreen \
    --app="$URL" \
    --noerrdialogs --disable-infobars --disable-session-crashed-bubble \
    --disable-features=Translate,InfiniteSessionRestore \
    --no-first-run --fast --fast-start \
    --check-for-update-interval=31536000 \
    --overscroll-history-navigation=0 \
    --disable-pinch
  sleep 3
done
EOF

# --- 6. start X automatically on the autologin console --------------
PROFILE_SNIP='# kiosk: start X on tty1
if [ -z "${DISPLAY:-}" ] && [ "$(tty)" = "/dev/tty1" ]; then
  exec startx -- -nocursor >/dev/null 2>&1
fi'
touch "$HOME_DIR/.bash_profile"
grep -q "kiosk: start X on tty1" "$HOME_DIR/.bash_profile" \
  || printf '\n%s\n' "$PROFILE_SNIP" >> "$HOME_DIR/.bash_profile"

chown -R "$KIOSK_USER:$KIOSK_USER" \
  "$HOME_DIR/.xinitrc" "$HOME_DIR/.bash_profile" "$HOME_DIR/.config"

# --- 7. boot to console (not desktop), enable autologin -------------
if command -v raspi-config >/dev/null; then
  raspi-config nonint do_boot_behaviour B2 || true   # console + autologin
fi
systemctl set-default multi-user.target
systemctl daemon-reload

cat <<EOF

==> Done.
    URL file : /etc/kiosk-url   ($URL)
    Reboot now:  sudo reboot

    To change the URL later:  sudo $0 "<new-url>"   &&  sudo reboot
    To watch/quit for debugging: plug in a keyboard, Ctrl+Alt+F2 for a console,
      log in, 'sudo pkill chromium; sudo pkill X' drops you to a shell.
EOF
