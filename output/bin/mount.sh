#!/bin/bash

logpath=./log/pfs-fuse-log
mkdir -p $logpath

args=$*
array=($args)
mountPoint=${array[-1]}
mountPath=$(echo $mountPoint | awk -F= '{print $2}')

echo $array
time=$(date "+%Y%m%d-%H%M%S")
nohup ./pfs-fuse mount $args > $logpath/pfs-fuse-$time.log 2>&1 &

exitCode=-1
for (( i = 0; i < 5; i++ )); do
    timeout 5 mountpoint $mountPath
    ret=$?
    if [ $ret = 0 ]; then
      exitCode=0
      break
    fi
    sleep 1
done
if [ $exitCode != 0 ]; then
   echo "pfs-fuse mount file system failed"
fi
exit $exitCode