#!/bin/bash

set -x

# enter paddlepaddle/paddleflow/
work_dir=`pwd`/../../
pushd ${work_dir}

if [[ ! -d output ]]; then
  echo "Directory[output] not exist in ${work_dir}"
  popd
  exit 255
fi

docker build -f ./installer/dockerfile/paddleflow_server/Dockerfile -t iregistry.public.com/bmlc/paddleflow-server:latest .
docker push iregistry.public.com/bmlc/paddleflow-server:latest

docker build -f ./installer/dockerfile/paddleflow_csiplugin/Dockerfile -t iregistry.public.com/bmlc/pfs-csi-plugin:latest .
docker push iregistry.public.com/bmlc/pfs-csi-plugin:latest

popd
