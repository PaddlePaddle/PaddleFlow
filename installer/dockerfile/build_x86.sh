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

docker build -f ./installer/dockerfile/paddleflow-server/Dockerfile -t iregistry.baidu-int.com/planet/paddleflow/paddleflow-server:2.0.0 .
docker push iregistry.baidu-int.com/planet/paddleflow/paddleflow-server:2.0.0

popd
