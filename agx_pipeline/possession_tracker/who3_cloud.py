"""WHO on SAM3 tracks for the unseen validation games, on a cloud GPU: the benchmark + validation.

For every annotated shot of one game (who_gt.json): SAM3 perception of the clip (models loaded
once, as a long-running worker would), the possession logic picks the shooter, his boxes are
read with the jersey stack. Records the reads (scored later against the fast tracker's
who_extend.jsonl) and the seconds SAM3 took per clip. Results are pushed to S3 every few shots,
so a spot interruption loses little and a rerun skips what is already there.

Paths mirror the AGX (/home/dev/...), so the same modules run unchanged.
WHO3_ONLY=<json {game: [t, ...]}> restricts the run to those shots (the production policy runs
SAM3 only where the fast tracker is silent, so only those need validating).

Usage: who3_cloud.py <game uuid> <out.jsonl> [<s3 uri for the out file>]
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

VAL = "/home/dev/validate"
PUSH_EVERY = 5


def push(path, uri):
    if uri:
        subprocess.run(["aws", "s3", "cp", path, uri, "--quiet"], check=False)


def main():
    game, out = sys.argv[1], sys.argv[2]
    uri = sys.argv[3] if len(sys.argv) > 3 else None
    from uball_cc.tracking.jersey_stack import JerseyStack
    js = JerseyStack()
    shots = [s for s in json.load(open(os.path.join(HERE, "who_gt.json")))["shots"] if s["game"] == game]
    if os.environ.get("WHO3_ONLY"):
        only = set(json.load(open(os.environ["WHO3_ONLY"])).get(game, []))
        shots = [s for s in shots if s["t"] in only]
    done = set()
    if os.path.exists(out):
        done = {j["t"] for j in map(json.loads, open(out))}
    work = os.path.join("/tmp", "who3_" + game[:8])
    n = 0
    with open(out, "a") as fo:
        for s in shots:
            if s["t"] in done:
                continue
            cam = "FL" if (s["side"] or "LEFT").upper() == "LEFT" else "FR"
            name = "%s_t%07.1f" % (cam, s["t"])
            clip = os.path.join(VAL, game, "clips", name + ".mp4")
            rec = {"game": game, "t": s["t"], "cam": cam, "gt": str(s["jersey"]), "team": s["team"]}
            if not os.path.exists(clip):
                rec["skip"] = "no clip"
            else:
                try:
                    _, secs = P.perceive(clip, work, T.PRE, keep=True)
                    rec["sam3_s"] = round(secs, 1)
                    t0 = time.time()
                    S = H.load(work, name)
                    res = H.analyse(S, H.Camera(cam))
                    cand = [x for x in res.get("shots", []) if x.get("shooter") is not None]
                    if cand:
                        shot = min(cand, key=lambda x: abs(x["rim_t"] - T.PRE))
                        boxes = (res.get("tracks") or {}).get(str(shot["shooter"])) or []
                        rec["reads"] = W.read_track(js, clip, boxes) if boxes else []
                    else:
                        rec["skip"] = "no shooter"
                    rec["read_s"] = round(time.time() - t0, 1)
                except Exception as e:  # noqa: BLE001
                    rec["skip"] = "%s: %s" % (type(e).__name__, str(e)[:200])
            fo.write(json.dumps(rec) + "\n")
            fo.flush()
            n += 1
            if n % PUSH_EVERY == 0:
                push(out, uri)
    push(out, uri)
    print("WHO3_GAME_DONE", game, flush=True)


if __name__ == "__main__":
    main()
