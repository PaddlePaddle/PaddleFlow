#!/bin/bash

set -x

# enter baidu/bmlc/paddleflow/
work_dir=`pwd`/../../
pushd ${work_dir}

if [[ ! -d output ]]; then
  echo "Directory[output] not exist in ${work_dir}"
  popd
  exit 255
fi

docker build -f ./installer/dockerfile/paddleflow_server/Dockerfile -t iregistry.baidu-int.com/bmlc/paddleflow-server:latest .
docker push iregistry.baidu-int.com/bmlc/paddleflow-server:latest

docker build -f ./installer/dockerfile/paddleflow_csiplugin/Dockerfile -t iregistry.baidu-int.com/bmlc/pfs-csi-plugin:latest .
docker push iregistry.baidu-int.com/bmlc/pfs-csi-plugin:latest

popd
