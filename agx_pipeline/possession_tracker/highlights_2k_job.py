"""Post-game 2K highlight job: render every ready FL/FR highlight of one game in the 2K style
and, with --publish, upload it next to the plain clip and record `url_2k` on the highlight.

The plain clip is never replaced or modified: the TV's green button keeps using it, and the
Core reel switches to `url_2k` only where CORE_REEL_USE_2K is on (core_highlight.py). A clip
whose render fails simply has no `url_2k`. Waits whenever the box is recording a game.

  python3 highlights_2k_job.py --firebase-game-id <fb id> --label game_20260919_022107 [--publish]

Without --publish it only renders into <clips>/<label>/2k/ (for review).
"""
import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/gopro-automation-linux")

CLIPS = os.environ.get("HIGHLIGHT_CLIPS_DIR", "/home/dev/app/recordings/highlight_clips")
CDN = os.environ.get("HIGHLIGHT_CDN_DOMAIN", "d22gul8sdref0l.cloudfront.net")
BUCKET = os.environ.get("UPLOAD_BUCKET", "uball-videos-production")


def log(msg):
    print("[2k] %s" % msg, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--firebase-game-id", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    import firebase_admin
    from firebase_admin import credentials, firestore
    import track_shadow as T
    from highlight_2k import render_highlight_2k

    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(credentials.Certificate(
            "/home/dev/gopro-automation-linux/uball-gopro-fleet-firebase-adminsdk.json"))
    ref = firestore.client().collection("basketball-games").document(a.firebase_game_id)
    highlights = (ref.get().to_dict() or {}).get("highlights") or {}
    todo = [(lid, h) for lid, h in sorted(highlights.items())
            if h.get("status") == "ready" and h.get("angle") in ("FL", "FR") and not h.get("url_2k")
            and h.get("made") is not False]      # CV misses are stored but never reach the Core reel
    if a.limit:
        todo = todo[:a.limit]
    log("%s: %d highlights, %d to render (publish=%s)" % (a.label, len(highlights), len(todo), a.publish))
    s3 = None
    n_ok = 0
    for lid, h in todo:
        while T.recording():
            time.sleep(60)
        src = os.path.join(CLIPS, a.label, "%s_%s.mp4" % (lid, h["angle"]))
        if not os.path.isfile(src):
            log("%s: local clip missing (%s) — skipped" % (lid, src))
            continue
        out = os.path.join(CLIPS, a.label, "2k", "%s_%s_2k.mp4" % (lid, h["angle"]))
        t0 = time.time()
        made = out if os.path.isfile(out) else render_highlight_2k(src, out)
        if not made:
            log("%s: render failed — plain clip stays" % lid)
            continue
        n_ok += 1
        if a.publish and h.get("s3_key"):
            import boto3
            s3 = s3 or boto3.client("s3")
            key = h["s3_key"].replace(".mp4", "_2k.mp4")
            s3.upload_file(made, BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
            ref.update({"highlights.%s.url_2k" % lid: "https://%s/%s" % (CDN, key),
                        "highlights.%s.s3_key_2k" % lid: key})
        log("%s: 2K %s (%.0fs)%s" % (lid, "published" if a.publish else "rendered", time.time() - t0,
                                      "" if a.publish else " -> " + made))
    log("DONE %d/%d rendered" % (n_ok, len(todo)))


if __name__ == "__main__":
    main()
