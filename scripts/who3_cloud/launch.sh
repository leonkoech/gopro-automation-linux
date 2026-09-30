#!/bin/bash
# Launch ONE GPU instance that runs the SAM3 WHO validation over the given games in turn.
# Usage: launch.sh <game uuid> [...]
# Tries spot g5.xlarge / g6.xlarge / g5.2xlarge in every zone, then on-demand g5.xlarge.
AMI=$(aws ssm get-parameter --region us-east-1 --name /aws/service/deeplearning/ami/x86_64/oss-nvidia-driver-gpu-pytorch-2.7-ubuntu-22.04/latest/ami-id --query Parameter.Value --output text)
DIR=$(cd "$(dirname "$0")" && pwd)
SUBNETS=$(aws ec2 describe-subnets --region us-east-1 --filters Name=vpc-id,Values=vpc-dd9cf6a7 --query 'Subnets[].SubnetId' --output text)
SPOT='{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}'
try() {  # type subnet market-json-or-empty
  aws ec2 run-instances --region us-east-1 --image-id "$AMI" --instance-type "$1" --subnet-id "$2" \
    ${3:+--instance-market-options "$3"} \
    --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile Name=uball-cv-worker --key-name uball-cv-eval \
    --security-group-ids sg-0feace425a2708f1f \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --user-data "file://$UD" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=who3-${GAME:0:8}},{Key=project,Value=uball-who3}]" \
    --query 'Instances[0].[InstanceId,InstanceType,Placement.AvailabilityZone]' --output text 2>/dev/null
}
for GAME in who3; do
  UD=/tmp/who3_ud.sh
  sed "s/__GAMES__/$*/" "$DIR/bootstrap.sh" > "$UD"
  got=""
  for T in g5.xlarge g6.xlarge g5.2xlarge; do
    for SN in $SUBNETS; do got=$(try $T $SN "$SPOT") && [ -n "$got" ] && break 2; done
  done
  if [ -z "$got" ]; then
    for SN in $SUBNETS; do got=$(try g5.xlarge $SN "") && [ -n "$got" ] && { got="$got on-demand"; break; }; done
  fi
  echo "${GAME:0:8}: ${got:-NO CAPACITY}"
done
