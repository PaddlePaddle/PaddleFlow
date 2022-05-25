#!/bin/bash

printenv

echo "$@"

echo "starting..."

mkdir -p $1
echo "hello paddleflow" >> $1/data
echo "get output artifact[train_data] in env variable: ${PF_OUTPUT_ARTIFACT_TRAIN_DATA}\n" >> $1/data
echo "get output artifact[validate_data] in env variable: ${PF_OUTPUT_ARTIFACT_VALIDATE_DATA}\n" >> $1/data

mkdir -p $2
echo "hello paddleflow train" >> $2/data

mkdir -p $3
echo "hello paddleflow validate" >> $3/data

echo "ending..."