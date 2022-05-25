#!/bin/bash

echo "$@"

echo "train data: "
cat $2/data

echo "Trainning..."

mkdir -p $3
echo "step train: get inputAtf[train_data] in env: $PF_INPUT_ARTIFACT_TRAIN_DATA" >> $3/model
echo "step train: get outputAtf[train_model] in env: $PF_OUTPUT_ARTIFACT_TRAIN_MODEL" >> $3/model
echo "step train: paddleflow model in path: $PF_OUTPUT_ARTIFACT_TRAIN_MODEL/model" >> $3/model

sleep 5

echo "ending..."
