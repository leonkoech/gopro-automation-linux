"""Orin LAN scoreboard display — a standalone, read-only relay.

Serves a full-screen scoreboard page over plain HTTP on the venue LAN and pushes
live game state to it via Server-Sent Events. State is read from the same
Firestore `basketball-games/{id}/live/state` doc the web TV display subscribes to
— this process is a *reader only* and never writes.

Runs as its own systemd service on its own port. It imports nothing from
`agx_pipeline/` and shares no state with the recording pipeline; the only thing
it touches is a Firestore read snapshot.

Why it exists: the Firebase-hosted `/tv-display` page is HTTPS, so it cannot open
a plain-HTTP/`ws://` connection to a LAN box (mixed-content). A page the Orin
serves itself over HTTP can — so the Fire TV holds a low-latency LAN connection
instead of a cloud one fighting gym Wi-Fi. See orin_display/README.md.
"""
