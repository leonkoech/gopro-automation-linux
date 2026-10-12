"""Score holder variants on the UNSEEN validation games from their cached fast tracks.

For each validation shot (results.jsonl: annotator type + production's type), run holder with
the variant's env, take the attempt nearest the card time, type it from the shooter's median
takeoff feet on production's arcs, fall back to production when the tracker has no answer, and
compare with the annotator. Variants: base, front (HOLDER_FRONT=1), tip (HOLDER_TIP=1), both.

Usage: val_variants.py <uuid> [<uuid> ...]    (box, /home/dev/possession, CUDA env not needed)
"""
import importlib
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

VARIANTS = {"base": {}, "front": {"HOLDER_FRONT": "1"}, "tip": {"HOLDER_TIP": "1"},
            "both": {"HOLDER_FRONT": "1", "HOLDER_TIP": "1"}}


def run(games, env):
    for k in ("HOLDER_FRONT", "HOLDER_TIP"):
        os.environ.pop(k, None)
    os.environ.update(env)
    import holder
    import track_shadow as T
    importlib.reload(holder)
    out = {}
    for g in games:
        d = "/home/dev/validate/" + g
        for line in open(os.path.join(d, "results.jsonl")):
            r = json.loads(line)
            if "error" in r:
                continue
            name = "%s_t%07.1f" % (r["cam"], r["t"])
            if not os.path.exists(os.path.join(d, "cache", name + ".npz")):
                continue
            ans = None
            try:
                S = holder.load(d, name)
                res = holder.analyse(S, holder.Camera(r["cam"]))
                shots = [s for s in res.get("shots", []) if s.get("shooter") is not None]
                if shots:
                    shot = min(shots, key=lambda s: abs(s["rim_t"] - T.PRE))
                    feet, still = T.takeoff_feet((res.get("tracks") or {}).get(str(shot["shooter"]), []),
                                                 shot["release_t"])
                    if feet:
                        ans = T.zone_of(T.load_arcs(r["cam"], 1920), feet, still)
            except Exception:  # noqa: BLE001
                pass
            out[g + name] = {"gt": r["gt"], "prod": r.get("prod"), "track": ans}
    return out


def main():
    games = sys.argv[1:]
    base = None
    for v, env in VARIANTS.items():
        res = run(games, env)
        ok = {k: (x["track"] or x["prod"]) == x["gt"] for k, x in res.items()}
        prod = sum(x["prod"] == x["gt"] for x in res.values())
        if base is None:
            base = ok
        fx = sum(1 for k in ok if ok[k] and not base.get(k))
        br = sum(1 for k in ok if base.get(k) and not ok[k])
        print("%-6s tracker+fallback %d/%d  (production %d)   vs base: fixed %d broke %d"
              % (v, sum(ok.values()), len(ok), prod, fx, br), flush=True)


if __name__ == "__main__":
    main()
