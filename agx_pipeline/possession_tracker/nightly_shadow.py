"""Nightly SHADOW sweep: run track_shadow.py on every game ingested in the last 20 h.

Same game selection as /home/dev/shot_typing/nightly_typing.py, minus its "already typed"
skip: the shadow runs after production typing and writes only <shadow>/<label>.jsonl, never a
card. track_shadow.py resumes per card and waits whenever the box is recording, so a sweep
that is cut short by a game simply continues the next night.

Cron (proposed, after the 04:15 production typing):
  45 5 * * * cd /home/dev/possession && bash -c "set -a; . /home/dev/gopro-automation-linux/.env.agx; \
      set +a; . /home/dev/sam3env.sh; python3 nightly_shadow.py >> shadow/nightly.log 2>&1"
"""
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/home/dev/gopro-automation-linux")
import firebase_admin  # noqa: E402
from firebase_admin import credentials, firestore  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REC = "/home/dev/app/recordings"
LOOKBACK_H = 20

try:
    firebase_admin.get_app()
except ValueError:
    firebase_admin.initialize_app(credentials.Certificate(
        "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
db = firestore.client()


def label_for(fb_game_id):
    for s in (db.collection("recording-sessions")
              .where("firebaseGameId", "==", fb_game_id).limit(3).stream()):
        seg = (s.to_dict() or {}).get("segmentSession") or ""
        if "_" in seg:
            return seg.rsplit("_", 1)[0]
    return None


def production_typing_running() -> bool:
    """True while the 04:15 production typing sweep is still going: two GPU jobs at once has
    wedged this box before, so the shadow waits its turn."""
    out = subprocess.run(["pgrep", "-f", "nightly_typing.py|shot_typing.py"], capture_output=True, text=True).stdout
    return bool(out.strip())


def main():
    while production_typing_running():
        print("[shadow-nightly] production typing still running — waiting", flush=True)
        time.sleep(300)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_H)
    runs = [d.to_dict() | {"_id": d.id} for d in
            db.collection("ingestion-runs").order_by("started_at",
            direction=firestore.Query.DESCENDING).limit(15).stream()]
    for r in runs:
        started = r.get("started_at")
        try:
            if started and datetime.fromisoformat(str(started).replace("Z", "+00:00")) < cutoff:
                continue
        except Exception:  # noqa: BLE001
            pass
        ug, fg = r.get("uball_game_id"), r.get("firebase_game_id")
        if not (ug and fg):
            continue
        label = label_for(fg)
        if not label or not os.path.isdir("%s/%s" % (REC, label)):
            print("[shadow-nightly] %s: no label/footage — skipped" % ug, flush=True)
            continue
        print("[shadow-nightly] %s (uball %s)..." % (label, ug[:8]), flush=True)
        t0 = time.time()
        subprocess.run(["python3", "track_shadow.py", "--label", label, "--uball-game-id", ug], cwd=HERE)
        print("[shadow-nightly] %s done in %.0f min" % (label, (time.time() - t0) / 60), flush=True)
    print("[shadow-nightly] sweep complete", flush=True)


if __name__ == "__main__":
    main()
