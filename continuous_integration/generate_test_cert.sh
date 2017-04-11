#!/bin/bash

set -e
pushd "distributed/tests"

set +e
rm test.key
rm test.pem
set -e

openssl req \
    -x509 \
    -nodes \
    -newkey \
    rsa:2048 \
    -days 365 \
    -subj /O=dask/CN=localhost \
    -nodes \
    -passout pass: \
    -keyout test.key \
    -out test.pem

popd