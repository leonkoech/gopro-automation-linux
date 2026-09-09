"""Standalone Flask app: serves the LAN scoreboard page + an SSE state stream.

    python3 -m orin_display.app

Environment:
    ORIN_DISPLAY_PORT          listen port (default 8090)
    ORIN_DISPLAY_DEMO          "1" -> synthetic game, no Firebase (default off)
    ORIN_DISPLAY_BUILD         build stamp shown on the page (default: date)
    FIREBASE_CREDENTIALS_PATH  admin SDK JSON (same var agx_pipeline uses)

Read-only: opens Firestore snapshot listeners, never writes. Runs on its own
port and systemd unit; imports nothing from agx_pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date
from pathlib import Path

from flask import Flask, Response, jsonify, send_from_directory

from .hub import StateHub
from .sources import DemoSource, FirestoreSource

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
logger = logging.getLogger("orin_display")

STATIC_DIR = Path(__file__).parent / "static"
PORT = int(os.getenv("ORIN_DISPLAY_PORT", "8090"))
DEMO = os.getenv("ORIN_DISPLAY_DEMO", "").strip().lower() in ("1", "true", "yes", "on")
BUILD = os.getenv("ORIN_DISPLAY_BUILD", f"orin-display {date.today().isoformat()}")

app = Flask(__name__)
hub = StateHub()
_source = None


def _start_source() -> None:
    global _source
    if DEMO:
        logger.info("starting in DEMO mode (no Firebase)")
        _source = DemoSource(hub)
        _source.start()
        return
    try:
        _source = FirestoreSource(hub)
        _source.start()
    except Exception as e:  # noqa: BLE001 — surface the failure on the screen, don't crash
        logger.error("Firestore source failed to start: %s", e)
        hub.publish({
            "status": "error", "gameId": None, "error": str(e)[:300],
            "left": {}, "right": {}, "period": "", "possession": None,
            "clock": {"baseGameTime": 0, "isRunning": False, "anchorMs": int(time.time() * 1000)},
            "source": "firebase", "serverEpochMs": int(time.time() * 1000), "updatedAt": None,
        })


@app.route("/")
def index() -> Response:
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/events")
def events() -> Response:
    def stream():
        last_version = -1
        # Send whatever we have immediately so a fresh page paints at once.
        version, state = hub.snapshot()
        yield f"data: {json.dumps(state)}\n\n"
        last_version = version
        while True:
            result = hub.wait(last_version, timeout=15.0)
            if result is None:
                yield ": ping\n\n"  # heartbeat keeps the connection warm through proxies
                continue
            last_version, state = result
            yield f"data: {json.dumps(state)}\n\n"

    return Response(stream(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    })


@app.route("/state")
def state() -> Response:
    _, s = hub.snapshot()
    return jsonify(s)


@app.route("/healthz")
def healthz() -> Response:
    version, s = hub.snapshot()
    return jsonify({
        "ok": True,
        "mode": "demo" if DEMO else "firebase",
        "build": BUILD,
        "version": version,
        "state_age_s": round(hub.age_seconds(), 1),
        "gameId": s.get("gameId"),
        "status": s.get("status"),
    })


@app.route("/build")
def build() -> Response:
    return jsonify({"build": BUILD})


def main() -> None:
    _start_source()
    logger.info("orin-display %s listening on 0.0.0.0:%d (mode=%s)",
                BUILD, PORT, "demo" if DEMO else "firebase")
    # threaded=True: each SSE client holds a worker thread — fine for a handful
    # of TVs on the LAN. No reloader (this runs under systemd).
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
