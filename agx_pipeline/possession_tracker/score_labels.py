"""Score holder.py's shooter against the human shooter labels (cb9e1294).

A label is the shooter's feet pixel on the labelled camera, clicked in the tool's RELEASE
mode (its t_clip field always reads the default 11.0 = the rim moment, but the clicks sit on
the shooter's release-pose feet). Correct = the tracked shooter's box within +-WIN s of the
DETECTED release contains the feet (box widened 25% each side; feet in its lower half, or up
to 15% of its height below it). A looser 2s window is useless: ~1.25 other players per clip,
usually the closing defender, pass it too. Identity is ignored on purpose.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ver = sys.argv[1] if len(sys.argv) > 1 else "v1"
WIN = 0.3
labels = json.load(open(os.path.join(HERE, "calib", "shooter_labels.json")))
ok = n = 0
for k, v in sorted(labels.items(), key=lambda kv: float(kv[0])):
    if v.get("skipped"):
        continue
    cam = v["cam"] if v["cam"] in ("FL", "FR") else v.get("takeoff_cam")
    if cam not in ("FL", "FR"):
        continue
    feet = v["feet_px"] if v["cam"] == cam else v["takeoff_px"]
    t_lab = 8.0 if v["cam"] == cam else 8.0 - (v["t_clip"] - v["takeoff_t"])
    name = "L_%s_%.2f" % (cam, v["rim_abs"])
    p = os.path.join(HERE, "out", ver, name + ".json")
    if not os.path.exists(p):
        continue
    r = json.load(open(p))
    n += 1
    tr = r.get("shooter_track") or []
    verdict, why = "WRONG", "no shooter"
    if tr:
        def inside(b):
            w, h = b[2] - b[0], b[3] - b[1]
            return (b[0] - 0.25 * w <= feet[0] <= b[2] + 0.25 * w
                    and b[1] + 0.5 * h <= feet[1] <= b[3] + 0.15 * h)
        rt = r.get("release_t")
        win = [(t, b) for t, b in tr if rt is not None and abs(t - rt) <= WIN]
        hit = [t for t, b in win if inside(b)]
        if not win:
            why = "no release, or shooter not tracked at it"
        elif hit:
            verdict, why = "OK", "feet %s inside shooter box at t=%.2f" % ([round(f) for f in feet], hit[0])
        else:
            t, b = min(win, key=lambda tb: abs(tb[0] - (r.get("release_t") or t_lab)))
            why = "feet %s never in shooter box (at release %s)" % ([round(f) for f in feet], b)
    ok += verdict == "OK"
    print("%-16s %-5s rel=%s rim_i=%s ball=%s  %s" % (name, verdict, r.get("release_t"), r["rim_i"], r["ball_frames"], why))
print("\n%s: shooter correct %d/%d" % (ver, ok, n))
