#!/usr/bin/env bash
set -Eeuo pipefail

echo "URL: ${KBC_TEST_URL}"
echo "Token: ${KBC_TEST_TOKEN:0:8}"
if R CMD check "keboola.sapi.r.client_0.5.0.tar.gz" --as-cran --no-manual; then
    echo "Build log:"
    cat /code/keboola.sapi.r.client.Rcheck/tests/testthat.Rout
    echo "Build Succeeded"
    exit 0
else
    echo "Error Log:"
    cat /code/keboola.sapi.r.client.Rcheck/tests/testthat.Rout.fail
    echo "Build Failed"
    exit 1
fi
