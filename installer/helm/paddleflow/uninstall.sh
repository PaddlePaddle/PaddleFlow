#/bin/bash
set -x


echo "Delete the PaddleFlow [paddleflow-server] service···············"
${PADDLEFLOW_HELM_BIN} del  paddleflow-server -npaddleflow