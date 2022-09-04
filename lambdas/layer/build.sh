#!/usr/bin/env bash

cd $(dirname $0)

# Python 3.8
docker build --pull --tag layers-py38 .

# DIR_NAME=$(dirname "$PWD")
docker run \
 --rm \
 --volume "$PWD":/finance/ \
 --workdir /finance/ \
 layers-py38 \
"./build-image-layer.sh"
