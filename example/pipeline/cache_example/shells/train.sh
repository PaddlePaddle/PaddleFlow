#!/bin/bash

echo "$@"

echo "train data: "
cat $2/data

echo "Trainning..."

mkdir -p $3
echo "paddleflow model in path: $PF_OUTPUT_ARTIFACT_TRAIN_MODEL/model" > $3/model
echo "get outputAtf in env: $PF_OUTPUT_ARTIFACT_TRAIN_MODEL/model"
echo "MODEL PATH: $3/model"

mkdir $PF_OUTPUT_ARTIFACT_TRAIN_MODEL

sleep 30

echo "ending..."