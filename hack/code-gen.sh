#!/usr/bin/env bash

set -x
set -o errexit
set -o nounset
set -o pipefail

base_path=$(cd `dirname $0`; pwd)/../
codegen_pkg_relative_path=vendor/k8s.io/code-generator
client_relative_path=paddleflow/pkg/client
apis_relative_path=paddleflow/pkg/apis

pushd ${base_path}

current_dir_name=`dirname $0`
if [[ ${current_dir_name} == "paddleflow" ]]; then
  echo "current_dir_name[${current_dir_name}] is not paddleflow"
  exit 1
fi

if [[ -z ${client_relative_path} ]]; then
  echo "client_relative_path is null"
  exit 1
fi

if [[ -d "../${client_relative_path}/volcano" ]]; then
  rm -rf ../${client_relative_path}/volcano
fi

bash ${codegen_pkg_relative_path}/generate-groups.sh "deepcopy,client,lister,informer" \
  ${client_relative_path}/volcano ${apis_relative_path}/volcano \
  "batch:v1alpha1 bus:v1alpha1 scheduling:v1beta1" \
  --go-header-file ./hack/boilerplate.go.txt \
  --output-base ../

if [[ -d "../${client_relative_path}/spark-operator" ]]; then
  rm -rf ../${client_relative_path}/spark-operator
fi

bash ${codegen_pkg_relative_path}/generate-groups.sh "deepcopy,client,lister,informer" \
  ${client_relative_path}/spark-operator ${apis_relative_path}/spark-operator \
  "sparkoperator.k8s.io:v1beta2" \
  --go-header-file ./hack/boilerplate.go.txt \
  --output-base ../

popd
