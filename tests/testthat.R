library(testthat)

KBC_TOKEN <- 'your_token'
KBC_URL <- 'https://connection.keboola.com/v2/'

# override with config if any
if (file.exists("config.R")) {
  source("config.R")
}

# override with environment if any
if (nchar(Sys.getenv("KBC_TOKEN")) > 0) {
  KBC_TOKEN <- Sys.getenv("KBC_TOKEN")  
}
if (nchar(Sys.getenv("KBC_URL")) > 0) {
  KBC_URL <- Sys.getenv("KBC_URL")  
}

test_check("keboola.sapi.r.client")
