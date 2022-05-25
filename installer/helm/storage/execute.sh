#!/bin/bash
set -x


# generate values.yaml
chartpath=$DEPLOY_ROOT_DIR/charts

# create configmap k8s-client-config
if [ $RUN_TIME_ENV == "k3s" ];then
    kubectl create configmap k8s-client-config --from-file=kube.config=${KUBE_CONFIG_FILE} -n ${DEPLOY_NAMESPACE}
else
    kubectl create configmap k8s-client-config --from-file=kube.config=${KUBE_CONFIG_FILE} -n ${DEPLOY_NAMESPACE}
fi

${PADDLEFLOW_HELM_BIN} install pfs-csi-plugin -f ${chartpath}/pfs-csi-plugin/values.yaml \
--namespace ${DEPLOY_NAMESPACE} --create-namespace $chartpath/pfs-csi-plugin \
--set csi.enable=${PADDLEFLOW_STORAGE}

${PADDLEFLOW_HELM_BIN} install pfs-csi-provisioner -f ${chartpath}/pfs-csi-provisioner/values.yaml \
--namespace ${DEPLOY_NAMESPACE} --create-namespace $chartpath/pfs-csi-provisioner \
--set csi.enable=${PADDLEFLOW_STORAGE}
