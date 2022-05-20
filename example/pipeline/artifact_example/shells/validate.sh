#!/bin/bash

echo "$@"

echo "starting..."

echo "model contents: "
cat $1/model

echo "path of input artifact [data]: $PF_INPUT_ARTIFACT_DATA"
echo "path of input artifact [model]: $PF_INPUT_ARTIFACT_MODEL"

echo "ending..."