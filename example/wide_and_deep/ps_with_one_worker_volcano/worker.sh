#!/usr/bin/bash
set -x

volcano_path=/etc/volcano
#volcano_path=./
#VK_TASK_INDEX=0
echo ${PF_JOB_PS_PORT}
export PADDLE_PSERVERS_IP_PORT_LIST=`cat ${volcano_path}/ps.host | sed 's/$/&:8001/g' | tr "\n" ","`
export PADDLE_TRAINER_ENDPOINTS=`cat ${volcano_path}/worker.host | sed 's/$/&:8001/g' | tr "\n" ","`
worker_arr=(`cat ${volcano_path}/worker.host`)
echo worker_arr:${worker_arr[@]}
export PADDLE_TRAINERS_NUM=${#worker_arr[@]}
export TRAINING_ROLE=TRAINER
export PADDLE_TRAINER_ID=${VK_TASK_INDEX}
python3.7 train.py
