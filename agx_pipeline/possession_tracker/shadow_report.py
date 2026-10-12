"""Score a shadow night: production's type vs the tracker's type on each CV card, against the
annotators' manual cards once they exist. Pairing: same side, same make/miss, one-to-one,
nearest within 4 s (the rule the Compare view and loop_score use).

Usage: shadow_report.py <shadow/label.jsonl> <uball_game_id>
"""
import json
import sys

sys.path.insert(0, "/home/dev/gopro-automation-linux")

TOL = 4.0
TYPE_OF = {"FG": "2PT", "2PT": "2PT", "3PT": "3PT", "4PT": "4PT", "FREE_THROW": "FREE_THROW"}
SIDE = {"FL": "LEFT", "FR": "RIGHT"}


def split(cl):
    cl = str(cl or "")
    for res in ("MAKE", "MISS"):
        if cl.endswith("_" + res):
            return TYPE_OF.get(cl[:-5]), res
    return None, None


def main():
    path, ug = sys.argv[1:3]
    from uball_client import UballClient
    rows = [json.loads(l) for l in open(path) if l.strip()]
    gt = []
    for p in UballClient().list_plays(ug):
        if (p.get("source") or "").lower() == "cv" or p.get("timestamp_seconds") is None:
            continue
        t, res = split(p.get("classification"))
        if t:
            gt.append({"t": float(p["timestamp_seconds"]), "type": t, "result": res,
                       "side": (p.get("angle") or "").upper()})
    pairs = []
    for i, r in enumerate(rows):
        pt, pres = split(r.get("prod_classification"))
        for j, g in enumerate(gt):
            if (abs(r["ts"] - g["t"]) <= TOL and g["result"] == pres
                    and (not g["side"] or g["side"] == SIDE.get(r["angle"]))):
                pairs.append((abs(r["ts"] - g["t"]), i, j))
    used_r, used_g, n, prod_ok, track_ok, track_ans, flips = set(), set(), 0, 0, 0, 0, []
    for _, i, j in sorted(pairs):
        if i in used_r or j in used_g:
            continue
        used_r.add(i); used_g.add(j)
        r, g = rows[i], gt[j]
        n += 1
        prod_t = split(r.get("prod_classification"))[0]
        prod_ok += prod_t == g["type"]
        if r.get("track_zone"):
            track_ans += 1
            track_ok += r["track_zone"] == g["type"]
        if prod_t != r.get("track_zone"):
            flips.append("%.1f %s GT %s prod %s track %s" % (r["ts"], r["angle"], g["type"], prod_t, r.get("track_zone")))
    print("%s: %d CV cards, %d paired with a manual card" % (path, len(rows), n))
    print("  production right %d/%d   tracker right %d/%d (answered %d)" % (prod_ok, n, track_ok, n, track_ans))
    print("  cards where they disagree (%d):" % len(flips))
    for f in flips:
        print("   ", f)


if __name__ == "__main__":
    main()
