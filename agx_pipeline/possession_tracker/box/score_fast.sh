# holder + whole-game eval on the FAST caches; then the same-shots comparison vs SAM3 (v7) and prod
set -a; . /home/dev/gopro-automation-linux/.env.agx; set +a; . /home/dev/sam3env.sh
cd /home/dev/possession
HOLDER_VER=vfast NO_RENDER=1 python3 holder.py out_fast x $(ls out_fast/cache | grep -v "^L_" | sed s/.npz//) > /dev/null 2>&1
HOLDER_VER=vfast NO_RENDER=1 python3 holder.py out7_fast x $(ls out7_fast/cache | sed s/.npz//) > /dev/null 2>&1
cd /home/dev/confirm_test
python3 /home/dev/possession/eval_full.py cb9e1294 types_cb9e1294_med_prod.json cb9e1294-e0ca-4f1b-ac2f-7d890a6a36b6 /home/dev/possession/out_fast vfast /home/dev/typing_eval/fit_cb9e1294 > /dev/null 2>&1
python3 /home/dev/possession/eval_full.py 7cef734e types_7cef734e_sync_med_prod.json 7cef734e-07ed-4ad0-95c8-612308b8d7cf /home/dev/possession/out7_fast vfast /home/dev/typing_eval/fit_7cef734e > /dev/null 2>&1
cd /home/dev/possession
python3 - <<"PY"
import json
tot = {"n": 0, "prod": 0, "sam3": 0, "fast": 0}
for sd, fd, g in (("out", "out_fast", "cb9e1294"), ("out7", "out7_fast", "7cef734e")):
    S = {r["clip"]: r for r in json.load(open("%s/eval_%s_v7.json" % (sd, g)))["rows"]}
    F = {r["clip"]: r for r in json.load(open("%s/eval_%s_vfast.json" % (fd, g)))["rows"]}
    both = [c for c in S if S[c].get("track_med") != "NOT_RUN" and F.get(c, {}).get("track_med") not in (None, "NOT_RUN")]
    n = len(both); p = sum(S[c]["prod"] == S[c]["gt"] for c in both)
    s = sum(S[c]["track_med"] == S[c]["gt"] for c in both); f = sum(F[c]["track_med"] == F[c]["gt"] for c in both)
    print("%s: %d shots  prod %d  SAM3 tracker %d  FAST tracker %d" % (g, n, p, s, f))
    for k, v in (("n", n), ("prod", p), ("sam3", s), ("fast", f)):
        tot[k] += v
print("BOTH: %(n)d shots  prod %(prod)d  SAM3 %(sam3)d  FAST %(fast)d" % tot)
PY
