#!/bin/bash
# W4: re-run the 28-shot benchmark with the SL/SR precise rim-time anchor (SHOT_RIM_TS).
# rim values = rim_base from w1_release.json; FR video ts = base - 1 (sync offset).
cd /home/dev/scratch_shot_timing
NVLIBS=$(ls -d /home/dev/.local/lib/python3.10/site-packages/nvidia/*/lib 2>/dev/null | tr '\n' ':')
export LD_LIBRARY_PATH=${NVLIBS}/usr/local/cuda-12.6/targets/aarch64-linux/lib:/usr/local/cuda-12.6/lib64
export SHOT_ATTRIB=possession SHOT_FEET=bbox
run() {  # cam ts gt rim_base
  local cam=$1 ts=$2 gt=$3 rim=$4 vts=$2 vrim=$4
  if [ "$cam" = FR ]; then
    vts=$(python3 -c "print(round($ts-1,1))")
    [ "$rim" != 0 ] && vrim=$(python3 -c "print(round($rim-1,2))")
  fi
  local line=$(SHOT_RIM_TS=$vrim PYTHONPATH=src python3 agx_classify.py $cam 0d96_${cam}.mp4 $vts g 2>/dev/null | grep "^RESULT" | sed -E "s/ \| [0-9.]+ det-fps.*//")
  echo "GT $cam $ts $gt :: $line"
}
while read cam ts gt rim; do [ -n "$cam" ] && run $cam $ts $gt $rim; done <<'GT'
FL 216.0 4PT 217.78
FL 297.0 3PT 298.73
FL 370.5 4PT 371.18
FL 570.2 4PT 572.39
FL 643.0 4PT 645.29
FL 1005.9 4PT 1006.88
FL 1050.4 3PT 1052.68
FL 1213.8 4PT 1215.47
FL 1356.3 4PT 1357.77
FL 1433.9 3PT 1433.95
FR 205.0 4PT 206.51
FR 236.1 3PT 238.38
FR 389.4 4PT 391.20
FR 479.2 3PT 481.27
FR 510.3 3PT 511.93
FR 515.8 3PT 518.13
FR 556.7 3PT 558.90
FR 561.7 4PT 563.79
FR 650.1 4PT 652.18
FR 708.3 4PT 708.25
FR 730.8 3PT 733.18
FR 787.2 3PT 788.22
FR 794.7 3PT 794.62
FR 886.4 4PT 888.72
FR 947.0 4PT 946.99
FR 1204.9 4PT 1206.58
FR 1237.4 3PT 0
FR 1415.9 3PT 1416.54
GT
echo PHASEW4_DONE
