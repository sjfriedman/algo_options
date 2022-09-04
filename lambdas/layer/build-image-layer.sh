#!/usr/bin/env bash
set -ex

FILENAME="finance-layer.zip"

pushd /finance

rm -rf ./python
rm -f $FILENAME
pip install yahoo_fin html5lib awswrangler -t ./python
find python -wholename "*/tests/*" -type f -delete

zip -r9 "${FILENAME}" ./python
rm -rf ./python

popd
