#! /bin/bash
set -x

dirpath=$(dirname $0|xargs realpath)

for model in $UNINSTALL_MODULE_LIST
do
  if [ "${model:0:1}" == "#" ];then
    continue
  fi

  cd ${model}
  sh uninstall.sh
  cd $dirpath
done
