#!/bin/bash

echo "$@"

echo "train data: "
cat $2/data

echo "Trainning..."

mkdir -p $3
echo "paddleflow model" > $3/model
echo "MODEL PATH: $3/model"
sleep 10

echo "ending..."
