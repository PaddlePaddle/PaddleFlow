#!/bin/bash

set -e

workdir=`cd $(dirname $0); pwd`
root=$workdir/..
supervise_dir=$root/status/paddleflow

pushd $root
mkdir -p $supervise_dir
./bin/supervise -p $supervise_dir -f "./bin/paddleflow"
popd
