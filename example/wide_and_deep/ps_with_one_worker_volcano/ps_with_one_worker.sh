#!/usr/bin/bash
set -x

volcano_path=/etc/volcano
#volcano_path=./
#VK_TASK_INDEX=1
export PADDLE_PORT=${PF_JOB_PS_PORT}
echo ${PF_JOB_PS_PORT}
export PADDLE_PSERVERS_IP_PORT_LIST=`cat ${volcano_path}/ps.host | sed 's/$/&:8001/g' | tr "\n" ","`
export PADDLE_TRAINER_ENDPOINTS=`cat ${volcano_path}/worker.host | sed 's/$/&:8001/g' | tr "\n" ","`
worker_arr=(`cat ${volcano_path}/worker.host`)
export PADDLE_TRAINERS_NUM=${#worker_arr[@]}
export TRAINING_ROLE=PSERVER
ps_arr=(`cat ${volcano_path}/ps.host`)
echo ps_arr:${ps_arr[@]}
echo VK_TASK_INDEX:${VK_TASK_INDEX}
export POD_IP=${ps_arr[${VK_TASK_INDEX}]}
python3.7 train.py

