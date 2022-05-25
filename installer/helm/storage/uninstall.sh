#!/bin/bash

echo "Delete the PaddleFlow [paddleflow-storage] service···············"
kubectl delete configmap k8s-client-config -npaddleflow
${PADDLEFLOW_HELM_BIN} del pfs-csi-plugin  -npaddleflow
${PADDLEFLOW_HELM_BIN} del pfs-csi-provisioner  -npaddleflow
