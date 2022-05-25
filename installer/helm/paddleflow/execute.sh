#!/bin/bash
set -x

kubectl get namespace ${DEPLOY_NAMESPACE}
retCode=$?
if [[ $retCode != 0 ]];then
  kubectl create namespace ${DEPLOY_NAMESPACE}
fi

# generate values.yaml
chartpath=$DEPLOY_ROOT_DIR/charts


# install paddleflow fs csi plugin
${PADDLEFLOW_HELM_BIN} install paddleflow-server -f ${chartpath}/paddleflow-server/values.yaml \
--namespace ${DEPLOY_NAMESPACE} --create-namespace $chartpath/paddleflow-server \
--set paddleflowServer.enable=${PADDLEFLOW_SERVER}