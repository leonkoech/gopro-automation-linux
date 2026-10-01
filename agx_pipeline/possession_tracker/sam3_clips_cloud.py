"""SAM3 over a list of clips on a cloud GPU: type AND jersey reads per clip, for game_test.py.

Per clip (names from a list file, clips in a dir): SAM3 perception (models loaded once), the
possession logic picks the shooter, takeoff feet -> zone on production's golden arcs (type), his
boxes are read with the jersey stack (who). Writes one JSON line per clip (name, sam3_s, zone,
reads) and pushes the file to S3 every few clips; resumable.

Usage: sam3_clips_cloud.py <clips dir> <names.txt> <out.jsonl> [<s3 uri>]
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, "/home/dev/who_deps_staging/pysrc")
import holder as H  # noqa: E402
import perceive as P  # noqa: E402
import track_shadow as T  # noqa: E402
import who_eval as W  # noqa: E402

PUSH_EVERY = 5


def push(path, uri):
    if uri:
        subprocess.run(["aws", "s3", "cp", path, uri, "--quiet"], check=False)


def main():
    clips, names_file, out = sys.argv[1:4]
    uri = sys.argv[4] if len(sys.argv) > 4 else None
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    arcs = {c: T.load_arcs(c, 1920) for c in ("FL", "FR")}
    names = [l.strip() for l in open(names_file) if l.strip()]
    done = {json.loads(l)["name"] for l in open(out)} if os.path.exists(out) else set()
    work = "/tmp/sam3_clips"
    n = 0
    with open(out, "a") as fo:
        for name in names:
            if name in done:
                continue
            cam = name.split("_")[0]
            clip = os.path.join(clips, name + ".mp4")
            rec = {"name": name}
            try:
                _, secs = P.perceive(clip, work, T.PRE, keep=True)
                rec["sam3_s"] = round(secs, 1)
                S = H.load(work, name)
                res = H.analyse(S, H.Camera(cam))
                cand = [x for x in res.get("shots", []) if x.get("shooter") is not None]
                if cand:
                    shot = min(cand, key=lambda x: abs(x["rim_t"] - T.PRE))
                    boxes = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
                    feet, still = T.takeoff_feet(boxes, shot["release_t"])
                    rec["zone"] = T.zone_of(arcs[cam], feet, still) if feet else None
                    rec["reads"] = W.read_track(js, clip, boxes) if boxes else []
                else:
                    rec["zone"], rec["reads"] = None, []
            except Exception as e:  # noqa: BLE001
                rec["error"] = "%s: %s" % (type(e).__name__, str(e)[:200])
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
            n += 1
            if n % PUSH_EVERY == 0:
                push(out, uri)
    push(out, uri)
    print("SAM3_CLIPS_DONE", flush=True)


if __name__ == "__main__":
    main()
