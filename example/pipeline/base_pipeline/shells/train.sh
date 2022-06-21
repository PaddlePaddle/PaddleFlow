#!/bin/bash

echo "$@"

echo "train data: "
cat $2/data

echo "Trainning..."

mkdir -p $3
echo "step train: paddleflow model in path: $3/model" >> $3/model

sleep 10

echo "ending..."
