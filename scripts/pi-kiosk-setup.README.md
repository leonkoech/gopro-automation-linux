# Raspberry Pi browser kiosk

Turn a Raspberry Pi into a single-purpose display: on boot it opens **one URL**
full-screen in Chromium and keeps it there (no cursor, no toolbars, no screen
blank, auto-restart on crash). Used to put a web page — e.g. the scoreboard
`/tv-display` or a `/recap` loop — on a TV that has no usable browser of its own.

There are two ways to do it. Pick based on whether you want to reflash the card.

---

## Option A — plain Raspberry Pi OS + `pi-kiosk-setup.sh`  (recommended)

Reliable, current Chromium, full control. Requires reflashing the SD card.

### 1. Flash the card

Use **Raspberry Pi Imager**. Choose **Raspberry Pi OS Lite (64-bit)**. Before
writing, open the settings (gear / "Edit settings") and set:

- hostname (e.g. `court-tv-1`)
- **Enable SSH** (password auth is fine)
- Wi-Fi SSID + password + country
- locale / timezone

Write it, put the card in the Pi, power on, wait ~1 min.

### 2. Run the script over SSH

```bash
ssh pi@court-tv-1.local          # or the Pi's IP
git clone https://github.com/leonkoech/gopro-automation-linux.git
cd gopro-automation-linux
sudo ./scripts/pi-kiosk-setup.sh "https://app.uai.tech/tv-display"
sudo reboot
```

The Pi boots straight into the page, full-screen.

### 3. Log in once (if the page needs it)

Plug in a USB keyboard/mouse, or `ssh` in and use `DISPLAY=:0`. Log into the web
app once — Chromium's profile persists at `~/.config/kiosk-chromium`, so the
session survives reboots.

### Changing the URL later

```bash
sudo ./scripts/pi-kiosk-setup.sh "https://app.uai.tech/recap"
sudo reboot
```

(or just edit `/etc/kiosk-url` and `sudo reboot`.)

### Options

| | |
|---|---|
| `KIOSK_USER=someuser sudo ./pi-kiosk-setup.sh <url>` | run as a user other than `pi` |
| `ROTATE=left sudo ./pi-kiosk-setup.sh <url>` | rotate the display (`left`/`right`/`inverted`) |

### Debugging on the Pi

- `Ctrl+Alt+F2` → a login console. `sudo pkill chromium; sudo pkill X` to stop the kiosk.
- Logs: `journalctl -b -e`, and `~/.local/share/xorg/Xorg.0.log`.
- Kill the auto-restart loop: comment the `while true` block in
  `~/.config/openbox/autostart`.

---

## Option B — keep PiSignage, add a "Link" asset  (no reflash)

The Pi currently runs **PiSignage** (`/boot/pisignage_version.txt` →
`buster_10.10_admin`). If you don't want to reflash:

1. Find the Pi's IP (check your router, or `ping pisignage.local`).
2. Open the PiSignage admin UI: `http://<pi-ip>:8000` (default login `pi` / `pi`).
3. **Assets → Add → Link/URL**, enter the page URL.
4. **Playlists** → new playlist containing only that link, set its duration high
   (e.g. `86400` s) and enable loop.
5. **Groups / Player settings** → set that playlist as the default and **Deploy**.
6. In player settings turn off any clock / status overlays.

Caveats: PiSignage buster 10.10 ships an **old Chromium (~2021)** and renders
links through its own player — a heavy SPA may lag or mis-render, and you get
less control over crashes/blanking. For a page that has to be rock-solid on a
TV, Option A is the better bet.
