#!/bin/bash

logpath=./log/pfs-fuse-log
mkdir -p $logpath

args=$*
array=($args)
mountPoint=${array[-2]}
mountPath=$(echo $mountPoint | awk -F= '{print $1}')

echo $array
time=$(date "+%Y%m%d-%H%M%S")
nohup /home/paddleflow/afs_mount $args > $logpath/afs-$time.log 2>&1 &

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
   echo "afs mount file system failed"
fi
exit $exitCode