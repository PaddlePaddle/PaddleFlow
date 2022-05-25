#!/bin/bash
set -x


echo "Delete the PaddleFlow [volcano] service···············"
kubectl delete mutatingwebhookconfiguration `kubectl get mutatingwebhookconfiguration| awk '/volcano/{print $1}'`
${PADDLEFLOW_HELM_BIN} del  volcano-admission-init -nvolcano-system
${PADDLEFLOW_HELM_BIN} del  volcano-admission -nvolcano-system
${PADDLEFLOW_HELM_BIN} del  volcano-controller -nvolcano-system
${PADDLEFLOW_HELM_BIN} del  volcano-scheduler -nvolcano-system

kubectl delete secrets/volcano-admission-secret -n ${VOLCANO_NAMESPACE}
kubectl delete namespace ${VOLCANO_NAMESPACE}