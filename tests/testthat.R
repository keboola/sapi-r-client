library(testthat)

var <- Sys.getenv('KBC_TEST_TOKEN')
if (var == '') {
    stop('KBC_TEST_TOKEN environment variable is empty')
}
var <- Sys.getenv('KBC_TEST_URL')
if (var == '') {
    stop('KBC_TEST_URL environment variable is empty')
}

test_check("keboola.sapi.r.client")

