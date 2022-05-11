#!/bin/sh
set -x 
shdir=$(cd "$(dirname "$0")";pwd)
paddleflow_home=$(dirname $shdir)
pipeline_home=$paddleflow_home/paddleflow/pipeline
pipeline_test_home=$shdir/test_pipeline
export PYTHONPATH=$paddleflow_home:$pipeline_test_home:$PYTHONPATH

echo 'Start Unit Test: '
pytest  --cov-branch --cache-clear --cov-report=xml --cov-report=html --cov=$pipeline_home/dsl ./test_pipeline/test_dsl
exit $?
