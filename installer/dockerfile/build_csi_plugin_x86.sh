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

docker build -f ./installer/dockerfile/paddleflow-csi-plugin/Dockerfile -t iregistry.baidu-int.com/planet/paddleflow/pfs-csi-plugin:1.4.2 .
docker push iregistry.baidu-int.com/planet/paddleflow/pfs-csi-plugin:1.4.2

popd
