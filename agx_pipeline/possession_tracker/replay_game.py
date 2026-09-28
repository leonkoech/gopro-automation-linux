"""Replay a recorded game through the tracker queue: enqueue every ready highlight clip of one
game exactly as the live service would (agx_pipeline.tracker_queue), in game order.

Usage: replay_game.py <firebase_game_id> <label> [--queue DIR] [--limit N] [--pace SECONDS]
  --pace  wait between clips (e.g. 50 = one make every ~50 s, like a live game); default 0
"""
import argparse
import os
import sys
import time

sys.path.insert(0, "/home/dev/gopro-automation-linux")

CLIPS = os.environ.get("HIGHLIGHT_CLIPS_DIR", "/home/dev/app/recordings/highlight_clips")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("game")
    ap.add_argument("label")
    ap.add_argument("--queue", default=os.environ.get("TRACKER_QUEUE_DIR", "/home/dev/possession/queue"))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--pace", type=float, default=0)
    ap.add_argument("--only", help="json file with {'keep': {log_id: ...}}: enqueue just those clips")
    a = ap.parse_args()
    import firebase_admin
    from firebase_admin import credentials, firestore
    from agx_pipeline.tracker_queue import enqueue_tracker_job
    firebase_admin.initialize_app(credentials.Certificate(
        "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
    d = firestore.client().collection("basketball-games").document(a.game).get().to_dict() or {}
    hs = sorted(((k, h) for k, h in (d.get("highlights") or {}).items()
                 if h.get("status") == "ready" and h.get("angle") in ("FL", "FR")),
                key=lambda kv: kv[1].get("updatedAt") or kv[0])
    if a.only:
        import json
        keep = json.load(open(a.only))["keep"]
        hs = [(k, h) for k, h in hs if k in keep]
    if a.limit:
        hs = hs[:a.limit]
    n = 0
    for k, h in hs:
        clip = os.path.join(CLIPS, a.label, "%s_%s.mp4" % (k, h["angle"]))
        if not os.path.isfile(clip):
            print("missing", clip)
            continue
        enqueue_tracker_job(a.game, k, h["angle"], clip, float(h.get("pre") or 5.0), h.get("s3_key"),
                            h.get("made"), queue_dir=a.queue)
        n += 1
        if a.pace:
            time.sleep(a.pace)
    print("enqueued", n, "clips")


if __name__ == "__main__":
    main()
