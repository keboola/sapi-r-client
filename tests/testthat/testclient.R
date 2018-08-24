
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

createTestBucket <- function(client) {
    client$createBucket(
        "r_client_testing",
        "in",
        "This bucket was created by the sapi R client test routine",
        backend="snowflake"
    )
}

test_that("verifyToken", {
    client <- SapiClient$new(
        token = KBC_TOKEN,
        url = KBC_URL
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
    token = KBC_TOKEN,
    url = KBC_URL
  )
  bucketlist <- client$listBuckets()
  
  # verify buckets structure
  lapply(seq_along(bucketlist), function(x) {
    verifyBucketStructure(bucketlist[[x]])
  })
})

test_that("createAndDeleteMethods", {
  client <- SapiClient$new(
    token = KBC_TOKEN,
    url = KBC_URL
  )
  # check if our testing table and bucket exist, if so remove them
  if (client$bucketExists("in.c-r_client_testing")) {
    if (client$tableExists("in.c-r_client_testing.test_table")) {
      client$deleteTable("in.c-r_client_testing.test_table")
    }
    client$deleteBucket("in.c-r_client_testing")
  }
  # make the bucket we will use for testing
  result <- createTestBucket(client)
  verifyBucketStructure(result)
  
  # create a table in our new bucket
  
  colC <- c("99786952", "109597927.109599284", "109611185.109612267", "109783546.109790316", "110305160.110305730")
  dfOrig <- data.frame(colA = 1:5, colB = 5:1, colC = colC)
  tableId <- client$saveTable(dfOrig, result$id, "test_table", "tmpfile.csv")
  
  #successfully saved the table.  retrieve it
  tbl <- client$getTable(tableId)
  verifyTableStructure(tbl)
  
  #confirm listTables function
  tablelist <- client$listTables("in.c-r_client_testing")
  expect_equal(1,length(tablelist))
  
  #import the data back into R session, test multiple where values
  df <- client$importTable(tbl$id, options = list(whereColumn = "colA", whereValues = c("1","2","4")))
  
  expect_equal(length(unique(df$colA)), 3)
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


test_that("oneRowTable", {
  client <- SapiClient$new(
    token = KBC_TOKEN,
    url = KBC_URL
  )
  # check if our testing table and bucket exist, if so remove them
  if (client$bucketExists("in.c-r_client_testing")) {
    if (client$tableExists("in.c-r_client_testing.test_table")) {
      client$deleteTable("in.c-r_client_testing.test_table")
    }
    client$deleteBucket("in.c-r_client_testing")
  }
  # make the bucket we will use for testing
  result <- createTestBucket(client)
  verifyBucketStructure(result)
  
  # create a table in our new bucket
  df <- data.frame(ts_var = '201309', actual_value = '20348832.0000000000', expected_value = '15190371.0000000000', run_id = '130865113')
  tableId <- client$saveTable(df, 'in.c-r_client_testing', 'test_table', 'tmpfile.csv')

  #import the data back into R session, test multiple where values
  df <- client$importTable(
    tableId,
    options = list(whereColumn = "run_id", whereValues = '130865113')
  )
  
  expect_equal(length(df$ts_var), 1)
  expect_equal(as.integer(df$ts_var[1]), 201309)
  
  # delete the table
  dt <- client$deleteTable(tableId)
  expect_false(client$tableExists(tableId))
  # delete the bucket
  dt <- client$deleteBucket(result$id)
  expect_false(client$bucketExists(result$id))
})


test_that("emptyRowTable", {
  client <- SapiClient$new(
    token = KBC_TOKEN,
    url = KBC_URL
  )
  # check if our testing table and bucket exist, if so remove them
  if (client$bucketExists("in.c-r_client_testing")) {
    if (client$tableExists("in.c-r_client_testing.test_table")) {
      client$deleteTable("in.c-r_client_testing.test_table")
    }
    client$deleteBucket("in.c-r_client_testing")
  }
  # make the bucket we will use for testing
  result <- createTestBucket(client)
  verifyBucketStructure(result)
  
  # create a table in our new bucket
  df <- data.frame(ts_var = character(), actual_value = character(), run_id = character(), dummy = character())
  tableId <- client$saveTable(df, 'in.c-r_client_testing', 'test_table', 'tmpfile.csv')
  
  #import the data back into R session, test multiple where values
  df <- client$importTable(
    tableId,
    options = list(whereColumn = "run_id", whereValues = '130865113')
  )
  
  expect_equal(nrow(df), 0)
  expect_equal(ncol(df), 4)
  
  # delete the table
  dt <- client$deleteTable(tableId)
  expect_false(client$tableExists(tableId))
  # delete the bucket
  dt <- client$deleteBucket(result$id)
  expect_false(client$bucketExists(result$id))
})

test_that("writeToNonExisingBucket", {
  client <- SapiClient$new(
    token = KBC_TOKEN,
    url = KBC_URL
  )
  # check if our testing table and bucket exist, if so remove them
  if (client$bucketExists("in.c-r_client_testing")) {
    if (client$tableExists("in.c-r_client_testing.test_table")) {
      client$deleteTable("in.c-r_client_testing.test_table")
    }
    client$deleteBucket("in.c-r_client_testing")
  }
  # make the bucket we will use for testing
  result <- createTestBucket(client)
  verifyBucketStructure(result)
  
  # create a table in our new bucket
  df <- data.frame(ts_var = '201309', actual_value = '20348832.0000000000', expected_value = '15190371.0000000000', run_id = '130865113')
  expect_error(client$saveTable(df, 'in.c-not-existing', 'ANM__991', 'tmpfile.csv'), regexp = 'not found')
  
  # delete the bucket
  dt <- client$deleteBucket(result$id)
  expect_false(client$bucketExists(result$id))
})

test_that("componentConfiguration", {
    client <- SapiClient$new(
        token = KBC_TOKEN,
        url = KBC_URL
    )
    testComponent <- "shiny"
    testConfigId <- "sapi-r-client-test"
    testConfiguration <- list(atest="test1", btest="test2")
    
    # check if our test configuration exists, and if so, delete it
    tryCatch ({
        configResponse <- client$getComponentConfiguration(testComponent,testConfigId)
        client$deleteComponentConfiguration(testComponent,testConfigId)
    }, error = function(e) {
        # didn't find configuration, that's great.    
    })
    
    # create component config
    config <- client$newComponentConfiguration(testComponent,testConfigId,testConfigId, "Description of the test.") 
    expect_equal(config$id, testConfigId)
    expect_equal(config$name, testConfigId)
    expect_equal(config$description, "Description of the test.")
    
    # put the configuration property
    config <- client$putComponentConfiguration(testComponent, testConfigId, testConfiguration)
    expect_equal(config$configuration, testConfiguration)
    
    testRowId <- "test-row"
    # create a configuration row 
    row <- client$createConfigurationRow(testComponent, testConfigId, testRowId, list(foo="bar", baz = c("foo", "barr")))
    expect_equal(row$configuration$foo, "bar")
    
    # put a different configuration row
    client$updateConfigurationRow(testComponent, testConfigId, testRowId, list(foo="baz", baz = c("foo", "barr")))
    
    # get configuration row
    row <- client$getConfigurationRow(testComponent, testConfigId, testRowId)
    expect_equal(row[[1]]$configuration$foo, "baz")
    
    # delete the configuration row
    client$deleteConfigurationRow(testComponent, testConfigId, testRowId)
    
    # get a list of configuration rows
    rows <- client$listConfigurationRows(testComponent, testConfigId) 
    rowIds <- unlist(lapply(rows, function(row){ row$id }))
    expect_false(testRowId %in% rowIds)
    
    # delete the configuration
    client$deleteComponentConfiguration(testComponent, testConfigId)
    # check to see that we've made it this far (means delete was successful)
    expect_true(TRUE)
})

test_that("workspaces", {
    client <- SapiClient$new(
        token = KBC_TOKEN,
        url = KBC_URL
    )
    
    workspaces <- client$listWorkspaces()
    # CLEAN UP before start: Drop all workspaces that 
    lapply(workspaces,function(workspace) {
        client$dropWorkspace(workspace$id)
    })
    
    # create a snowflake workspace
    workspace <- client$createWorkspace(backend="snowflake")
    expect_false(is.null(workspace$id))  
    expect_false(is.null(workspace$created))
    expect_equal("snowflake",workspace$connection$backend)
    expect_false(is.null(workspace$connection$host))
    expect_false(is.null(workspace$connection$warehouse))
    expect_false(is.null(workspace$connection$database))
    expect_false(is.null(workspace$connection$schema))
    expect_false(is.null(workspace$connection$user))
    expect_false(is.null(workspace$connection$password))
    
    # get the just created workspace
    ws <- client$getWorkspace(workspace$id)
    expect_equal(ws$id, workspace$id)
    expect_equal(ws$created, workspace$created)
    expect_equal(ws$connection$host,  workspace$connection$host)
    expect_equal(ws$connection$warehouse,  workspace$connection$warehouse)
    expect_equal(ws$connection$user,  workspace$connection$user)
    # password should be only on create
    expect_true(is.null(ws$password))
    
    # drop the workspace
    res <- client$dropWorkspace(ws$id)
    expect_true(res)
    
    tryCatch({
        # workspace no longer exists, should throw 404
        client$getWorkspace(ws$id)
    }, error = function(e) {
        expect_false(is.null(e))
    })
})

test_that("incremental_load", {
    client <- SapiClient$new(
        token = KBC_TOKEN,
        url = KBC_URL
    )
    # check if our testing table and bucket exist, if so remove them
    if (client$bucketExists("in.c-r_client_testing")) {
        if (client$tableExists("in.c-r_client_testing.test_table")) {
            client$deleteTable("in.c-r_client_testing.test_table")
        }
        client$deleteBucket("in.c-r_client_testing")
    }
    # make the bucket we will use for testing
    result <- createTestBucket(client)
    verifyBucketStructure(result)
    
    # create a table in our new bucket
    df <- data.frame(ts_var = '201309', actual_value = '20348832.0000000000', expected_value = '15190371.0000000000', run_id = '130865113')
    tableId <- client$saveTable(df, 'in.c-r_client_testing', 'test_table', 'tmpfile.csv', list(primaryKey = c("ts_var"), incremental = 1)) 
    
    retrieveTable <- client$importTable(tableId)
    expect_equal(1, nrow(df))
    
    df2 <- data.frame(ts_var = '201310', actual_value = '444.0000000000', expected_value = '152000.0000000000', run_id = '13086')
    tableId <- client$saveTable(df2, 'in.c-r_client_testing', 'test_table', 'tmpfile.csv', list(primaryKey = c("ts_var"), incremental = 1)) 
    
    retrieveTable <- client$importTable(tableId)
    expect_equal(2, nrow(retrieveTable))
})

test_that("fileLoad", {
    client <- SapiClient$new(
        token = KBC_TOKEN,
        url = KBC_URL
    )
    
    # create a file to put to our storage
    testFilePath <- "fileStorageTest.csv"
    df <- data.frame(ts_var = '201309', actual_value = '20348832.0000000000', expected_value = '15190371.0000000000', run_id = '130865113')
    write.csv(df, file=testFilePath)
    
    tags <- c("sapi-r-client", "bigFunTestFile")
    client$putFile(testFilePath, tags=tags)
    files <- client$listFiles(tags=tags)
    
    expect_equal(length(files), 1)
    
    data <- client$loadFile(files[[1]]$id)
    
    expect_equal(read.csv(textConnection(data, 'r'), row.names = FALSE), df)
    
    client$deleteFile(files[[1]]$id)
    files <- client$listFiles(tags=tags)
    expect_equal(0, length(files))
})