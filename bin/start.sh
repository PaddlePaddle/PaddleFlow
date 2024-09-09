#!/bin/bash

set -e

script_dir=$(dirname "${BASH_SOURCE[0]}")
chmod +x ${script_dir}/../bms_agent
${script_dir}/../bms_agent

workdir=`cd $(dirname $0); pwd`
root=$workdir/..
supervise_dir=$root/status/paddleflow

pushd $root
mkdir -p $supervise_dir
./bin/supervise -p $supervise_dir -f "./bin/paddleflow"
popd
