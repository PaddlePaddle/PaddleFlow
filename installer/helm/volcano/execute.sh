#!/bin/bash
set -x

# generate values.yaml
chartpath=$DEPLOY_ROOT_DIR/charts

kubectl create -f ${chartpath}/crds/volcano/*

kubectl delete secrets/volcano-admission-secret -n ${VOLCANO_NAMESPACE}
kubectl create namespace ${VOLCANO_NAMESPACE}
${PADDLEFLOW_HELM_BIN} install -f ${chartpath}/volcano-admission-init/values.yaml \
volcano-admission-init \
$chartpath/volcano-admission-init \
--namespace ${VOLCANO_NAMESPACE} --set volcano.enable=${VOLCANO}

${PADDLEFLOW_HELM_BIN} install  volcano-admission -f ${chartpath}/volcano-admission/values.yaml \
  --namespace ${VOLCANO_NAMESPACE} $chartpath/volcano-admission \
  --set volcano.enable=${VOLCANO}

${PADDLEFLOW_HELM_BIN} install  volcano-controller -f ${chartpath}/volcano-controller/values.yaml \
  --namespace ${VOLCANO_NAMESPACE} $chartpath/volcano-controller \
  --set volcano.enable=${VOLCANO}

${PADDLEFLOW_HELM_BIN} install  volcano-scheduler -f ${chartpath}/volcano-scheduler/values.yaml \
  --namespace ${VOLCANO_NAMESPACE} $chartpath/volcano-scheduler \
  --set volcano.enable=${VOLCANO}
