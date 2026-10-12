#!/bin/bash
# EC2 user-data: SAM3 WHO validation for the listed games (GAMES is substituted by launch.sh).
# Small pieces + installs + a jersey-reader preflight first (fails in minutes, not after the 5 GB
# download), then the SAM3 weights and clips, then who3_cloud.py; results + this log go to S3 and
# the instance powers off (terminates).
exec > /var/log/who3_boot.log 2>&1
set -x
GAMES="__GAMES__"
G8=who3
B=s3://uball-videos-production/cloud_eval/who3
finish() { aws s3 cp /var/log/who3_boot.log $B/results/${G8}_bootstrap.log --quiet; shutdown -h now; }
trap finish EXIT
shutdown -h +180 || true                      # hard cap: 3 h whatever happens
mkdir -p /home/dev/validate /root/.cache/torch
aws s3 cp $B/pkg/code.tgz - | tar xz -C /
aws s3 cp $B/pkg/who_deps.tgz - | tar xz -C /home/dev
aws s3 cp s3://uball-videos-production/cloud_eval/pkg/torchhub_parseq.tgz - | tar xz -C /root/.cache/torch
PY=/opt/pytorch/bin/python3
$PY -m pip freeze | grep -iE "^(torch|torchvision)==" > /tmp/pins.txt
cat /tmp/pins.txt
$PY -m pip install -q -c /tmp/pins.txt "transformers==5.17.0" ultralytics lap opencv-python-headless \
    timm pytorch_lightning nltk pyyaml lmdb
$PY -c "import torch, transformers; print('torch', torch.__version__, torch.cuda.is_available(), 'transformers', transformers.__version__)"
PYTHONPATH=/home/dev/who_deps_staging/pysrc $PY -c "from uball_cc.tracking.jersey_stack import JerseyStack; JerseyStack(); print('JERSEY_STACK_OK')" || exit 1
aws s3 cp $B/pkg/sam3_model.tar - | tar x -C /home/dev
aws s3 cp $B/pkg/who3_cloud.py /home/dev/possession/who3_cloud.py
aws s3 cp $B/pkg/who3_only.json /home/dev/possession/who3_only.json
nvidia-smi --query-gpu=name,memory.total --format=csv
cd /home/dev/possession
for GAME in $GAMES; do
  g8=${GAME:0:8}
  aws s3 cp $B/pkg/clips_${g8}.tar - | tar x -C /home/dev/validate
  aws s3 cp $B/results/who3_${g8}.jsonl /home/dev/who3_${g8}.jsonl --quiet || true    # resume
  WHO3_ONLY=/home/dev/possession/who3_only.json $PY who3_cloud.py $GAME /home/dev/who3_${g8}.jsonl $B/results/who3_${g8}.jsonl
done
