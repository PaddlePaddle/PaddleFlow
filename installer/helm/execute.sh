#! /bin/bash
set -x

dirpath=$(dirname $0|xargs realpath)

for model in $INSTALL_MODULE_LIST
do
  if [ "${model:0:1}" == "#" ];then
    continue
  fi

  cd ${model}
  sh execute.sh
  cd $dirpath
done
