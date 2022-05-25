#!/bin/bash
set -ex

chartpath=$DEPLOY_ROOT_DIR/charts
source $chartpath/../config.sh
envlist=$(env|awk -F'=' 'BEGIN{envlist=""}{envlist=envlist" ${"$1"}"}END{print envlist}')
#envsubst "$envlist" < $chartpath/values.yaml.template > $chartpath/values.yaml
envsubst "$envlist" < $chartpath/paddleflow-server/values.yaml.template > $chartpath/paddleflow-server/values.yaml
envsubst "$envlist" < $chartpath/pfs-csi-plugin/values.yaml.template > $chartpath/pfs-csi-plugin/values.yaml
envsubst "$envlist" < $chartpath/pfs-csi-provisioner/values.yaml.template > $chartpath/pfs-csi-provisioner/values.yaml
