#!/bin/bash

printenv

echo "$@"

echo "starting..."

mkdir -p $1
echo "hello paddleflow\n" >> $1/data
sleep 5

echo "ending..."
