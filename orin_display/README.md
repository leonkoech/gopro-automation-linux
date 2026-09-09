# orin_display — LAN scoreboard display (read-only relay)

A standalone service that serves a full-screen scoreboard page **over plain HTTP
on the venue LAN** and streams live game state to it via Server-Sent Events.

**This is additive.** It runs as its own systemd unit on its own port (8090),
imports nothing from `agx_pipeline/`, and shares no state with the recording
pipeline. It only *reads* Firestore — it never writes. It does not modify or
replace `/tv-display` or anything Rohit owns; it's a parallel path you can A/B
against the existing TV display.

## Why

The Firebase-hosted `/tv-display` page is HTTPS, so the browser blocks it from
opening a plain-HTTP or `ws://` connection to a LAN box (mixed content). A page
the Orin serves *itself* over HTTP has no such restriction — so the Fire TV holds
a fast, reliable LAN connection to the Orin instead of a cloud connection
fighting congested gym Wi-Fi.

```
iPad /admin-controls ──▶ Firestore ──▶  Orin (this service)  ──SSE/LAN──▶  TV
        (unchanged)        (cloud)      reads live/state doc              http://orin:8090
```

The iPad and the scoring flow are **unchanged** — this only moves the TV's read
path onto the LAN. Making the iPad local too is a separate, larger piece
(`docs/ORIN_LOCAL_SCOREBOARD_PLAN.md`).

## Test it now (no game, no Firebase)

```bash
# on any machine with Flask installed
ORIN_DISPLAY_DEMO=1 python3 -m orin_display.app
# open http://localhost:8090
```

The demo runs a synthetic game — clock ticking, scores changing, halftime — so
you can see it on the actual Fire TV before a game night.

## Deploy on the AGX

The repo autodeploys to the box, so the code arrives on its own. One-time setup:

```bash
sudo cp /home/dev/gopro-automation-linux/orin_display/orin-display.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now orin-display
systemctl status orin-display
curl -s localhost:8090/healthz
```

`FIREBASE_CREDENTIALS_PATH` is picked up from `.env.agx` (already set for
`agx-ingestion`). Make sure LAN clients can reach port 8090 (open it if the box
has a firewall; on a flat venue LAN it's typically already reachable).

## Point the TV at it

Give the AGX a **DHCP reservation** (fixed LAN IP) on the venue router, then on
the Fire TV browser open:

```
http://<orin-lan-ip>:8090
```

`agxorin001.local:8090` also works on devices with solid mDNS (the iPad); Fire TV
Silk is unreliable with `.local`, so use the IP there.

Put it side-by-side with `/tv-display` during a game and compare responsiveness.

## What the page shows

Team names + colors, scores, an anchor-based game clock (same formula as the real
display, corrected for device clock skew via `serverEpochMs`), fouls, timeouts,
period, and possession. The bottom-left footer is a live diagnostic:

```
● SSE open · upd 2s · evt 41 · demo · a1b2c3d4
```

- **SSE open / reconnecting** — the LAN connection state
- **upd Ns** — seconds since the last state update landed
- **evt N** — total updates received this session
- **source · gameId** — `firebase` or `demo`, and the active game

If the footer says `open` and `upd` stays low while `/tv-display` next to it
lags, the Fire TV's cloud sync is the problem and the LAN path fixes it.

## Endpoints

| Route | Purpose |
|---|---|
| `GET /` | the display page |
| `GET /events` | SSE stream of state |
| `GET /state` | current state as JSON (debugging) |
| `GET /healthz` | service status, mode, state age |
| `GET /build` | build stamp |

## Environment

| Var | Default | Notes |
|---|---|---|
| `ORIN_DISPLAY_PORT` | `8090` | listen port |
| `ORIN_DISPLAY_DEMO` | off | `1` → synthetic game, no Firebase |
| `ORIN_DISPLAY_BUILD` | today's date | stamp shown on the page |
| `FIREBASE_CREDENTIALS_PATH` | — | admin SDK JSON (from `.env.agx`) |
