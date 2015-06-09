
verifyBucketStructure <- function(bucket) {
  expect_false(is.null(bucket$uri))  
  expect_false(is.null(bucket$id))
  expect_false(is.null(bucket$name))
  expect_false(is.null(bucket$stage))
  expect_false(is.null(bucket$description))
  expect_false(is.null(bucket$tables))
  expect_false(is.null(bucket$created))
  expect_false(is.null(bucket$isReadOnly))
  expect_false(is.null(bucket$dataSizeBytes))
  expect_false(is.null(bucket$rowsCount))
  expect_false(is.null(bucket$isMaintenance))
  expect_false(is.null(bucket$backend))
  expect_false(is.null(bucket$attributes))
}

verifyTableStructure <- function(table) {
  expect_false(is.null(table$uri))  
  expect_false(is.null(table$id))
  expect_false(is.null(table$name))
  expect_false(is.null(table$transactional))
  expect_false(is.null(table$primaryKey))
  expect_false(is.null(table$indexedColumns))
  expect_false(is.null(table$created))
  expect_false(is.null(table$lastImportDate))
  expect_false(is.null(table$rowsCount))
  expect_false(is.null(table$isAlias)) 
}



test_that("verifyToken", {
    client <- SapiClient$new(
        token = apitoken,
        url = apiUrl
    )
    tokenDetails <- client$verifyToken()

    # verify credentials structure
    expect_false(is.null(tokenDetails$id))
    expect_false(is.null(tokenDetails$token))
    expect_false(is.null(tokenDetails$description))
    expect_false(is.null(tokenDetails$uri))
    expect_false(is.null(tokenDetails$isMasterToken))
    expect_false(is.null(tokenDetails$bucketPermissions))
    expect_false(is.null(tokenDetails$owner))
})

test_that("listBuckets", {
  client <- SapiClient$new(
    token = apitoken,
    url = apiUrl
  )
  bucketlist <- client$listBuckets()
  
  # verify buckets structure
  lapply(seq_along(bucketlist), function(x) {
    verifyBucketStructure(bucketlist[[x]])
  })
})


test_that("listTables", {
  client <- SapiClient$new(
    token = apitoken,
    url = apiUrl
  )
  tablelist <- client$listTables("sys.c-shiny")
  expect_equal(1,length(tablelist))
  
  lapply(seq_along(tablelist), function(x) {
    verifyTableStructure(tablelist[[x]])
  }) 
})

test_that("createAndDeleteMethods", {
  client <- SapiClient$new(
    token = apitoken,
    url = apiUrl
  )
  # check if our testing table and bucket exist, if so remove them
  if (client$bucketExists("in.c-r_client_testing")) {
    if (client$tableExists("in.c-r_client_testing.test_table")) {
      client$deleteTable("in.c-r_client_testing.test_table")
    }
    client$deleteBucket("in.c-r_client_testing")
  }
  # make the bucket we will use for testing
  result <- client$createBucket("r_client_testing","in","This bucket was created by the sapi R client test routine")
  verifyBucketStructure(result)
  
  # create a table in our new bucket
  
  colC <- c("99786952", "109597927.109599284", "109611185.109612267", "109783546.109790316", "110305160.110305730")
  df <- data.frame(colA = 1:5, colB = 5:1, colC = colC)
  tableId <- client$saveTable(df, result$id, "test_table", "tmpfile.csv")
  
  #successfully saved the table.  retrieve it
  tbl <- client$getTable(tableId)
  verifyTableStructure(tbl)
  
  #import the data back into R session
  df <- client$importTable(tbl$id, options = list(limit = 3))
  
  expect_equal(nrow(df), 3)
  expect_equal(c("colA", "colB", 'colC'), names(df))

  df <- client$importTable(tbl$id)
  expect_equal(sort(df$colC), sort(colC))
  
  # delete the table
  dt <- client$deleteTable(tbl$id)
  expect_false(client$tableExists(tbl$id))
  # delete the bucket
  dt <- client$deleteBucket(result$id)
  expect_false(client$bucketExists(result$id))
})


