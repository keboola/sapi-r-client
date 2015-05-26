# replace "your_token" with your keboola connection project token
# or set environment variable TEST_TOKEN with your token string

apiUrl <- 'https://connection.keboola.com/v2/'
if (nchar(Sys.getenv("TEST_TOKEN")) > 0) {
  apitoken <- Sys.getenv("TEST_TOKEN")  
}else {
  apitoken <- 'your_token'  
}
userAgent <- "Keboola StorageApi R Client/v2"
