#!/bin/bash
# Phase B: re-run the 0d96e12a 28-shot benchmark with attribution engine = $1 (possession|prox).
# Calibration (calib_arcs_*) already on the box. FR ts gets the -1s sync offset.
cd /home/dev/scratch_shot_timing
NVLIBS=$(ls -d /home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib 2>/dev/null | tr '\n' ':')
export LD_LIBRARY_PATH=${NVLIBS}/usr/local/cuda-12.6/targets/aarch64-linux/lib:/usr/local/cuda-12.6/lib64
export SHOT_ATTRIB=${1:-possession}
echo "== attrib=$SHOT_ATTRIB =="
run() {  # cam ts gt
  local cam=$1 ts=$2 gt=$3 vts=$2
  [ "$cam" = FR ] && vts=$(python3 -c "print(round($ts-1,1))")
  local line=$(PYTHONPATH=src python3 agx_classify.py $cam 0d96_${cam}.mp4 $vts g 2>/dev/null | grep "^RESULT" | sed -E "s/ \| [0-9.]+ det-fps.*//")
  echo "GT $cam $ts $gt :: $line"
}
while read cam ts gt; do [ -n "$cam" ] && run $cam $ts $gt; done <<'GT'
FL 216.0 4PT
FL 297.0 3PT
FL 370.5 4PT
FL 570.2 4PT
FL 643.0 4PT
FL 1005.9 4PT
FL 1050.4 3PT
FL 1213.8 4PT
FL 1356.3 4PT
FL 1433.9 3PT
FR 205.0 4PT
FR 236.1 3PT
FR 389.4 4PT
FR 479.2 3PT
FR 510.3 3PT
FR 515.8 3PT
FR 556.7 3PT
FR 561.7 4PT
FR 650.1 4PT
FR 708.3 4PT
FR 730.8 3PT
FR 787.2 3PT
FR 794.7 3PT
FR 886.4 4PT
FR 947.0 4PT
FR 1204.9 4PT
FR 1237.4 3PT
FR 1415.9 3PT
GT
echo PHASEB_DONE
