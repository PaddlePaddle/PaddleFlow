#!/bin/bash

set -e

workdir=`cd $(dirname $0); pwd`
root=$workdir/..
supervise_dir=$root/status/paddleflow
status_file=$supervise_dir/status
process_info="./bin/paddleflow"

StopBySupervise() {
  pid_new=$(od -d $status_file | head -n 2 | tail -n 1 | awk '{print $2}')
  while [[ "X"$pid_new != "X0" ]]; do
      echo 'find paddleflow process alive. pid: '$pid_new
      echo 'd' > $supervise_dir/control
      echo 'x' > $supervise_dir/control
      sleep 1
      pid_new=$(od -d $status_file | head -n 2 | tail -n 1 | awk '{print $2}')
  done
}

StopByForceKill() {
  pid=$(ps -ef | grep "${process_info}" | grep -v "grep" | grep -v "supervise" | awk '{print $2}')
  if [ -n "$pid" ]; then
      echo 'find paddleflow process alive. pid: '$pid
      kill -9 $pid
  else
      echo "no paddleflow process found"
  fi
}

# 首先判断supervise进程是否存在
supervise_pid=`ps -ef | grep "${process_info}" | grep "supervise" | grep -v "grep" | awk '{print $2}'`
if [ -z "$supervise_pid" ]; then
   echo "no supervise process found"
   StopByForceKill
else
   echo 'find supervise process alive. pid: '$supervise_pid
   StopBySupervise
fi
