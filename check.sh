#!/usr/bin/env bash

if R CMD check "keboola.sapi.r.client_0.4.0.tar.gz" --as-cran --no-manual; then
    echo "Build log:"
    cat /code/keboola.sapi.r.client.Rcheck/tests/testthat.Rout
    echo "Build Succeeded"
    return 0
else
    echo "Error Log:"
    cat /code/keboola.sapi.r.client.Rcheck/tests/testthat.Rout.fail
    echo "Build Failed"
    return 1
fi
