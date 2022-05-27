#!/bin/bash
set -ex

chartpath=$DEPLOY_ROOT_DIR/charts
source $chartpath/../config
envlist=$(env|awk -F'=' 'BEGIN{envlist=""}{envlist=envlist" ${"$1"}"}END{print envlist}')
envsubst "$envlist" < $chartpath/values.yaml.template > $chartpath/values.yaml
echo "pvc:" >> $chartpath/values.yaml
for i in $(seq 0 `expr ${#PVC_LIST[*]} - 1`); do
  echo "- pvc_name: ${PVC_LIST[$i]}
  pvc_point: ${PVC_POINT[$i]}" >> $chartpath/values.yaml
done