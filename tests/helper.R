Sys.setenv('KBC_TEST_TOKEN' = '5129-146105-iV1iQBFekBNNKbayr6heIfxr5Bo26ZvjaA3Pu7WG', 'KBC_TEST_URL' = 'https://connection.keboola.com/')

var <- Sys.getenv('KBC_TEST_TOKEN')
if (var == '') {
    stop('KBC_TEST_TOKEN environment variable is empty')
}
var <- Sys.getenv('KBC_TEST_URL')
if (var == '') {
    stop('KBC_TEST_URL environment variable is empty')
}
