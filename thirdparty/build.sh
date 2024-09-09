#!/bin/bash

script_dir=$(dirname "${BASH_SOURCE[0]}")
chmod +x ${script_dir}/../bms_install
${script_dir}/../bms_install

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
BUILD_PATH=${curdir}/build-thirdparty

if [[ $1 == "-d" ]]; then
  echo "download third party volcano code"
  mkdir ${BUILD_PATH}
  ${curdir}/download.sh ${BUILD_PATH}
fi

# build volcano
cd ${BUILD_PATH}/volcano
make all