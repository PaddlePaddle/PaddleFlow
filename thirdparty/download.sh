#!/bin/bash

if [[ "" = "$1" ]]; then
  echo "please set download path!"
  exit 1
fi

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
PATCH_PATH=${curdir}/patches
BUILD_PATH=$1

check_remote_repo() {
  if [[ -z $1 ]]; then
    echo "repo url should specified to check."
    exit 1
  fi

  git ls-remote $1 -q
  if [[ $? != 0 ]]; then
    echo "remote repo $1 not exist"
    exit 1
  fi
}

# set Download parameters
source ${curdir}/vars.sh

# check remote repo
check_remote_repo ${VOLCANO_GIT_URL}
check_remote_repo ${VOLCANO_APIS_GIT_URL}

# download volcano-sh/apis
if [[ -d ${BUILD_PATH}/apis ]]; then
  echo "update volcano-sh/apis!"
  pushd ${BUILD_PATH}/apis
  git pull
else
  echo "clone volcano-sh/apis!"
  git clone ${VOLCANO_APIS_GIT_URL} ${BUILD_PATH}/apis
  pushd ${BUILD_PATH}/apis
fi
git checkout -b ${VOLCANO_APIS_PATCH_FILE} ${VOLCANO_APIS_TAG}
popd

# download volcano-sh/volcano
if [[ -d ${BUILD_PATH}/volcano ]]; then
  echo "update volcano-sh/volcano!"
  pushd ${BUILD_PATH}/volcano
  git pull
  git checkout -b ${VOLCANO_TAG} ${VOLCANO_TAG}
  popd
else
  echo "clone volcano-sh/volcano!"
  git clone -b ${VOLCANO_TAG} ${VOLCANO_GIT_URL} ${BUILD_PATH}/volcano
fi

# patch volcano-sh/apis
pushd ${BUILD_PATH}/apis
git apply ${PATCH_PATH}/${VOLCANO_APIS_PATCH_FILE}.patch
popd

# patch volcano-sh/volcano
pushd ${BUILD_PATH}/volcano
git apply ${PATCH_PATH}/${VOLCANO_PATCH_FILE}.patch
popd