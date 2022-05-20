#!/bin/bash

echo "$@"

echo "starting..."

echo "model contents: "
cat $PF_INPUT_ARTIFACT_MODEL/model

echo "path of input artifact [data]: $PF_INPUT_ARTIFACT_DATA"
echo "path of input artifact [model]: $PF_INPUT_ARTIFACT_MODEL"

echo "ending..."
