# holder (latest) on every cached clip of both games, then the whole-game eval; CPU only
set -a; . /home/dev/gopro-automation-linux/.env.agx; set +a; . /home/dev/sam3env.sh
V=${1:-v6}
cd /home/dev/possession
HOLDER_VER=$V NO_RENDER=1 python3 holder.py out x $(ls out/cache | grep -v "^L_" | sed s/.npz//) > holder_out.log 2>&1
HOLDER_VER=$V NO_RENDER=1 python3 holder.py out7 x $(ls out7/cache | sed s/.npz//) > holder_out7.log 2>&1
cd /home/dev/confirm_test
python3 /home/dev/possession/eval_full.py cb9e1294 types_cb9e1294_med_prod.json cb9e1294-e0ca-4f1b-ac2f-7d890a6a36b6 /home/dev/possession/out $V /home/dev/typing_eval/fit_cb9e1294 2>&1 | grep -v Warn
python3 /home/dev/possession/eval_full.py 7cef734e types_7cef734e_sync_med_prod.json 7cef734e-07ed-4ad0-95c8-612308b8d7cf /home/dev/possession/out7 $V /home/dev/typing_eval/fit_7cef734e 2>&1 | grep -v Warn
