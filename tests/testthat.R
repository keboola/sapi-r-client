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

#Sys.setenv(KBC_TEST_TOKEN='66-322-DyChVD3tyv6bzasIU8YCC9uCcFztnC5SGjBLqP7p')
#Sys.setenv(KBC_TEST_URL='https://connection.north-europe.azure.keboola.com/')
#
#source("R/client.R")
#client <- SapiClient$new(
#    token = Sys.getenv('KBC_TEST_TOKEN'),
#    url = Sys.getenv('KBC_TEST_URL')
#)
