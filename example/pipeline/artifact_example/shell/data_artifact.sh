#!/bin/bash

printenv

echo "$@"

echo "starting..."

mkdir -p $1
echo "hello paddleflow" > $1/data

mkdir -p $2
echo "hello paddleflow train" > $2/data

mkdir -p $3
echo "hello paddleflow validate" > $3/data

echo "ending..."