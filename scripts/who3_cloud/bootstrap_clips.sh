#!/bin/bash
# EC2 user-data: SAM3 over one shard of a test game's clips (G8 / SHARD substituted by launch_clips.sh).
# Small pieces + installs + a jersey-reader preflight first (fails in minutes, not after the 5 GB
# download), then the SAM3 weights and clips, then who3_cloud.py; results + this log go to S3 and
# the instance powers off (terminates).
exec > /var/log/who3_boot.log 2>&1
set -x
G8=__G8__
SHARD=__SHARD__
GT=s3://uball-videos-production/cloud_eval/gametest/$G8
B=s3://uball-videos-production/cloud_eval/who3
finish() { aws s3 cp /var/log/who3_boot.log $GT/boot_${SHARD}.log --quiet; shutdown -h now; }
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
aws s3 cp $B/pkg/sam3_clips_cloud.py /home/dev/possession/sam3_clips_cloud.py
mkdir -p /home/dev/gt_clips && aws s3 cp $GT/clips.tar - | tar x -C /home/dev/gt_clips
aws s3 cp $GT/names_${SHARD}.txt /home/dev/names.txt
aws s3 cp $GT/sam3_${SHARD}.jsonl /home/dev/sam3_${SHARD}.jsonl --quiet || true     # resume
nvidia-smi --query-gpu=name,memory.total --format=csv
cd /home/dev/possession
$PY sam3_clips_cloud.py /home/dev/gt_clips/clips /home/dev/names.txt /home/dev/sam3_${SHARD}.jsonl $GT/sam3_${SHARD}.jsonl
