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

docker build -f ./installer/dockerfile/paddleflow-server/Dockerfile -t paddleflow/paddleflow-server:1.4.3 .
docker push paddleflow/paddleflow-server:latest

docker build -f ./installer/dockerfile/paddleflow-csi-plugin/Dockerfile -t paddleflow/pfs-csi-plugin:1.4.3 .
docker push paddleflow/pfs-csi-plugin:latest

popd
