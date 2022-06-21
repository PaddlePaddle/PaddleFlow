#!/bin/bash

echo "$@"

echo "starting..."

echo "model contents: "
cat $1/model

echo "step validate: path of input artifact [data]: $PF_INPUT_ARTIFACT_DATA" >> $1/model
echo "step validate: path of input artifact [model]: $PF_INPUT_ARTIFACT_MODEL" >> $1/model

echo "ending..."
