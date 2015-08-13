#' Client for working with Keboola Connection Storage API.
#'
#' @import httr methods aws.signature XML
#' @exportClass SapiClient
#' @export SapiClient
SapiClient <- setRefClass(
    'SapiClient',
    fields = list(
        token = 'character',
        url = 'character',
        userAgent = 'character'
    ),
    methods = list(
        #' Constructor.
        #'
        #' @param token KBC Storage API token.
        #' @param url Optional URL of the provisioning API.
        #' @exportMethod
        initialize = function(
                token,
                url = 'https://connection.keboola.com/v2/',
                userAgent = "Keboola StorageApi R Client/v2"
        ) {
            .self$token <<- token
            .self$url <<- url
            .self$userAgent <<- userAgent
            # check for token validity
            tryCatch(
            {  
              vf <- .self$verifyToken()
            }, error = function(e) {
              stop("Invalid Access Token")
            })
        },

        #' Internal method to process API response
        #' @param response List as returned from httr POST/GET method
        #' @return response body - either list or string in case the body cannot be parsed as JSON.
        decodeResponse = function(response) {
            # decode response
            content <- httr::content(response, as = "text")
            body <- NULL
            tryCatch({
              body <- jsonlite::fromJSON(content, simplifyVector = FALSE)
            }, error = function (e) {
              stop(paste("Error parsing JSON response:", e$message))
            })
            if (is.null(body)) {
                # failed to parse body as JSON
                body <- content
            }

            # handle errors
            if (!(response$status_code %in% c(200, 201))) {
                if ((class(body) == 'list') && !is.null(body$message)) {
                    stop(paste0("Error recieving response from sapi API: ", body$message))
                } else if (class(body) == 'list') {
                    str <- print(body)
                    stop(paste0("Error recieving response from sapi API: ", str, response$status_code))
                } else if (class(body) == 'character') {
                    stop(paste0("Error recieving response from sapi API: ", body))
                } else {
                    stop(paste0("Error recieving response from sapi API: unknown reason."))
                }
            }
            body
        },

        #' generic GET method
        #' 
        #' @param url
        #' @param query - list of query arguments ex. list(foo = bar)
        #' @return list - the response object
        get = function(urlG, query = NULL) {
            if ((class(query) == 'list') && (length(query) == 0)) {
                # if query is an empty list, convert it to NULL, otherwise httr::GET will botch the request
                query <- NULL
            }
            httr::GET(urlG, httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent), query = query)
        },
        
        #' generic POST method
        #' 
        #' @param urlP - url to post to
        #' @param data - body of the request
        #' @return body of the response
        #' @exportMethod
        genericPost = function(urlP, data = NULL) {
          .self$decodeResponse(  
            httr::POST(urlP,
                     httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent),
                     body = data)
          )
        },
        
        #' generic method for GET requests
        #'
        #' @param urlG - full URL 
        #' @param query - query parameters
        #' @return body of the response
        #' @exportMethod
        genericGet = function(urlG, query = NULL) {
          if ((class(query) == 'list') && (length(query) == 0)) {
            # if query is an empty list, convert it to NULL, otherwise httr::GET will botch the request
            query <- NULL
          }
          resp <- httr::GET(urlG, 
                            httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent), 
                            query = query)
          
          .self$decodeResponse(resp)
        },
        
        #' Generic method for DELETE requests
        #'
        #' @param urlD - the url to send the request to
        #' @return true for success, will throw error if status code returned not 204
        #' @exportMethod
        genericDelete = function(urlD) {
          resp <- httr::DELECT(urlD,
                       httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent)
                       )
          if (!(resp$status_code == 204)) {
            stop(paste0(resp$status_code, " Unnexpected Status code  ", .self$decodeResponse(resp)))
          } else {
            TRUE
          }
        },
        
        #' internal helper for parsing options
        #' 
        #' @param list options
        #' @return list options
        prepareOptions = function(options) {
          # which parameters aree allowed
          params <- c("limit","changedSince","changedUntil","whereColumn","whereValues")
          opts <- options[names(options) %in% params]
          if ("columns" %in% names(options)) {
            opts[["columns"]] <- paste(options[["columns"]], collapse=",")
          }
          if ("whereValues" %in% names(options)) {
            for (i in 1:length(options[["whereValues"]])) {
              opts[[paste0("whereValues[",i-1,"]")]] <- options[["whereValues"]][i]  
            } 
            opts[["whereValues"]] <- NULL
          }
          opts
        },
        
        #' get details of the token
        #'
        #' @return list object containing details of this client's token
        #' @exportMethod
        verifyToken = function() {
            .self$decodeResponse(.self$get(paste0(.self$url,"storage/tokens/verify")))
        },
        
        #' make a status request to an async syrup job
        #' 
        #' @param url - this will normally be the url returned from the createTableAsync/importTableAsync methods
        #' @return list - job details body 
        getJobStatus = function(url) {
            .self$decodeResponse(.self$get(url))
        },
        
        #' return info about the file, including credentials
        #' 
        #' @param string fileId
        #' @return list fileInfo object
        getFileInfo = function(fileId, federationToken = TRUE) {
            .self$decodeResponse(.self$get(paste0(url,"storage/files/", fileId), query=list(federationToken=federationToken)))
        },
        
        #' get a file from the s3 storage
        #' 
        #' @param list fileInfo object
        #' @return file contents
        getFileData = function(fileInfo) {
          if (fileInfo$isSliced) {
            response <- httr::GET(fileInfo$url)
            manifest <- .self$decodeResponse(response)
            df <- 
              lapply(seq_along(manifest$entries), function(x) {
                fullPath <- manifest$entries[[x]]$url
                splittedPath <- strsplit(fullPath,"/")
                fileKey <-  paste(splittedPath[[1]][4:length(splittedPath[[1]])], collapse="/")
                bucket <- fileInfo$s3Path$bucket
                # get the chunk from S3
                .self$s3GET(paste0("https://",bucket,".s3.amazonaws.com/",fileKey), fileInfo$credentials)
            })
            df <- do.call("rbind", df)
          } else {
            # single file, so just get it
            bucket <- fileInfo$s3Path$bucket
            key <- fileInfo$s3Path$key
            df <- .self$s3GET(paste0("https://",bucket,".s3.amazonaws.com/",key), fileInfo$credentials, header=TRUE)
          }
          df
        },
        
        #' Upload a file to AWS S3 bucket 
        #' (compression is not yet supported by this client)
        #' 
        #' @param string file to upload
        #' @param list options - for a full list of options please see the api docs 
        #' @return int fileId of the uploaded file
        uploadFile = function(dataFile, options = list()) {
          if (!("name" %in% names(options))) {
            options$name = basename(dataFile)
          }
          resp <- tryCatch(
            {  
                .self$decodeResponse(
                      httr::POST(paste0(.self$url,"storage/files/prepare"),
                        httr::add_headers("X-StorageApi-Token" = .self$token),
                        body = options)
              )
            }, error = function(e) {
              stop(paste("error preparing file upload", e))
            }, warning = function(w) {
              stop(paste("preparing file upload warning recieved:", w$message))
            }
          )
          uploadParams <- resp$uploadParams
          body <- list(
            key = uploadParams$key,
            acl = uploadParams$acl,
            signature = uploadParams$signature,
            policy = uploadParams$policy,
            AWSAccessKeyId = uploadParams$AWSAccessKeyId,
            file = httr::upload_file(dataFile)
          )
          if ("isEncrypted" %in% names(options)) {
            body$x-amz-server-side-encryption <- uploadParams$x-amz-server-side-encryption
          }
          res <- 
            tryCatch(
              {
                httr::POST(uploadParams$url, body=body)
              }, error = function(e) {
                stop(paste("error uploading file", e))
              }, warning = function(w) {
                stop(paste("file upload warning recieved:", w$message))
              }
            )
          # return the file id of the uploaded file
          resp$id
        },
        
        #' Create/Overwrite a new table in a bucket ascynchronously
        #'
        #' @param bucket
        #' @param tableName
        #' @param fileId - id of file received from the uploadFile method
        #' @param (optional) character 
        #' @param (optional) list - additional parameters
        #' @return string - URL to ping for table creation status check
        saveTableAsync = function(bucket, tableName, fileId, opts = list()) {
          posturl <- paste(.self$url,"storage/buckets/", bucket, "/tables-async", sep="")
          
          #prepare our options
          options = list(
            bucketId = bucket,
            name = tableName,
            delimiter = if ("delimiter" %in% names(opts)) opts$delimeter else ",",
            enclosure = if ("enclosure" %in% names(opts)) opts$enclosure else '"',
            escapedBy = if ("escapedBy" %in% names(opts)) opts$delimeter else "",
            primaryKey = if ("primaryKey" %in% names(opts)) opts$primaryKey else NULL,
            transactional = if ("transactional" %in% names(opts)) ? opts$transactional else FALSE,
            dataFileId = fileId
          )
          
          resp <-
            tryCatch( 
              {
                httr::POST(posturl,httr::add_headers("X-StorageApi-Token" = .self$token),
                     body = options)
              }, error = function(e) {
                stop(paste("error creating table", e))
              }, warning = function(w) {
                stop(paste("warning recieved creating table:", w$message))
              }
            )
          httr::content(resp)
        },
        
        #' Begin an export job of a table.
        #' 
        #' @param string table identifier
        #' @param options allowable query parameters
        #' @return list containing info of the job.  Contains url to check status
        importTableAsync = function(tableId, options=list()) {
          opts <- .self$prepareOptions(options)
          if (!("federationToken" %in% names(opts))) {
            opts$federationToken = TRUE
          }
          response <- 
            tryCatch(
              {
                  httr::POST(paste0(.self$url,"storage/tables/", tableId, "/export-async"),
                      httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent),
                     body = opts)
              }, error = function(e) {
                stop(paste("error posting file to sapi", e))
              }, warning = function(w) {
                stop(paste("Save file warning recieved: ", w$message))
              }
            )
          httr::content(response)
        },
        
        #' wrapper for the saveTableAsync function call
        #' 
        #' @param data.frame data to save
        #' @param string bucket
        #' @param string tableName
        #' @param string fileName - temporary file name to write to. will be deleted
        #' @return boolean
        #' @exportMethod
        saveTable = function(df, bucket, tableName, fileName="tmpfile.csv", options=list()) {
          write.csv(df, file=fileName, row.names=FALSE)
          fileId <- .self$uploadFile(fileName)
          # start writing job
          res <- .self$saveTableAsync(bucket, tableName, fileId, options)
          if (!is.null(res$error)) {
            stop(paste0('Cannot save table ', bucket, '.', tableName, ' error:', res$error, ' (', res$exceptionId, ')'))
          }
          repeat {
            job <- .self$getJobStatus(res$url)
            # check the job status
            if (job$status == "success") {
              break
            } else if (job$status != "waiting" && job$status != "processing") {
              stop(paste("Unexpected Job status:", job$status))
            }
            Sys.sleep(0.5)
          }
          # if we got this far the write was successful
          # remove temporary file
          file.remove(fileName)
          job$results$id
        },
        
        #' import table into your R session
        #' this is a wrapper function
        #' 
        #' @param string tableId
        #' @param list options
        #' @return dataframe
        #' @exportMethod
        importTable = function(tableId, options=list()) {
          tryCatch({
            res <- .self$importTableAsync(tableId, options=options)
            if (!is.null(res$error)) {
              stop(paste("Error retrieving table:",res$error))
            }      
            repeat {
              job <- .self$getJobStatus(res$url)
              if (job$status == "success") {
                break
              } else if (job$status != "waiting" && job$status != "processing") {
                stop(paste("Unexpected Job status:", job$status))
              }
              Sys.sleep(0.5)
            }
            table <- .self$getTable(tableId)
            if ("columns" %in% names(options)) {
              columns <- options$columns
            }else {
              columns <- table$columns
            }
            fileInfo <- .self$getFileInfo(job$result$file$id)
            df <- .self$getFileData(fileInfo)
            names(df) <- columns
            df
          }, error = function(e) {
            stop(e$message)
          })  
        },
        
        #' get a list of all buckets
        #' 
        #' @param list of query parameters
        #' @return list of buckets
        #' @exportMethod
        listBuckets = function(options = list()) {
            resp <- .self$get(paste(.self$url,"storage/buckets",sep=""),options)
            body <- .self$decodeResponse(resp)

            if (class(body) != 'list') {
                str <- print(body)
                stop(paste0("Malformed response from storage API: ", str))
            }
            body
        },
        
        #'  Get a list of all tables
        #'  
        #'  @param (optional) string bucket - the bucket id whose tables to get
        #'  @param (optional) list options - query parameters
        #'  @return list of tables in bucket or list of all tables
        #'  @exportMethod
        listTables = function(bucket = NULL, options = list()) {
            if (is.null(bucket)) {
              body <- .self$decodeResponse(
                  .self$get(paste0(.self$url,"storage/tables"), options)  
                )
            } else {
              body <- .self$decodeResponse(
                  .self$get(paste0(.self$url,"storage/buckets/",bucket,"/tables"), options)  
                      )
            }
            if (class(body) != 'list') {
              str <- print(body)
              stop(paste0("Malformed response from storage API: ", str))
            }
            body
        },
        
        #' Get table information
        #' 
        #' @param string table identifier
        #' @return list table details
        #' @exportMethod
        getTable = function(tableId) {
            .self$decodeResponse(.self$get(paste0(.self$url,"storage/tables/",tableId)))
        },
        
        #' Get bucket information
        #' 
        #' @param string bucket identifier
        #' @return list bucket details
        #' @exportMethod
        getBucket = function(bucketId) {
            .self$decodeResponse(.self$get(paste0(.self$url,"storage/buckets/",bucketId)))
        },
        
        #' Create a new bucket
        #' 
        #' @param string name of the bucket
        #' @param string one of (in, out, sys)
        #' @param string description of the bucket
        #' @param string what database provider to use (mysql or redshift)
        #' @return list the newly created bucket object
        #' @exportMethod
        createBucket = function(name, stage, description, backend = "redshift") {
          options = list(
            name = name,
            stage = stage,
            description = description,
            backend = backend
          )
          resp <-
            tryCatch(
            {
              httr::POST(paste0(.self$url,"storage/buckets"), 
                    httr::add_headers("X-StorageApi-Token" = .self$token),
                   body = options)
            }, error = function(e) {
              stop(paste("error posting fle to sapi", e))
            }, warning = function(w) {
              stop(paste("attempting save file warning recieved:", w$message))
            }
            )
          .self$decodeResponse(resp)
        },
        
        #' delete a bucket
        #' 
        #' @param string the id of the bucket to delete
        #' @return TRUE on success
        #' @exportMethod
        deleteBucket = function(bucketId) {
          resp <- httr::DELETE(paste0(.self$url,"storage/buckets/",bucketId),
                httr::add_headers("X-StorageApi-Token" = .self$token))
          if (!(resp$status_code == 204)) {
            stop(paste0(resp$status_code, " Error deleting bucket ", bucketId))
          } else {
            TRUE
          }
        },
        
        #' delete a table
        #'
        #' @param string the id of the table
        #' @return TRUE on success
        #' @exportMethod
        deleteTable = function(tableId) {
          resp <- httr::DELETE(paste0(.self$url,"storage/tables/", tableId),
                    httr::add_headers("X-StorageApi-Token" = .self$token))
          
          if (!(resp$status_code == 204)) {
            stop(paste0(resp$status_code, " Error deleting table ", tableId))
          } else {
            TRUE
          }
        },
        
        #' AWS GET method (uses aws.signature package to compose the signature)
        #' 
        #' @param string url - the url to GET
        #' @param list credentials - the temporary AWS credentials
        #' @return request body (should be data.frame)
        s3GET = function(url, credentials, header = FALSE) {
          region <- "us-east-1"
          current <- Sys.time()
          d_timestamp <- format(current, "%Y%m%dT%H%M%SZ", tz = "UTC")
          p <- httr::parse_url(url)
          action <- if(p$path == "") "/" else paste0("/",p$path)
          headers <- list()
          Sig <- aws.signature::signature_v4_auth(
            datetime = d_timestamp,
            region = region,
            service = "s3",
            verb = "GET",
            action = action,
            query_args = p$query,
            canonical_headers = list(host = p$hostname,
                                     `x-amz-date` = d_timestamp,
                                     `x-amz-security-token` = credentials$SessionToken),
            request_body = "",
            key = credentials$AccessKeyId, 
            secret = credentials$SecretAccessKey
          )
          headers$`x-amz-date` <- d_timestamp
          headers$`x-amz-content-sha256` <- Sig$BodyHash
          headers$Authorization <- Sig$SignatureHeader
          headers$`x-amz-security-token` <- credentials$SessionToken
          
          H <- do.call(httr::add_headers, headers)
          
          r <- httr::GET(url, H)
          content <- httr::content(r, as="text")
          if (content != '') {
            read.csv(text = content, header=header, colClasses = "character")
          } else {
            data.frame()
          }
        },
        #' Check to see if a bucket exists
        #' 
        #' @param string bucketId
        #' @return boolean
        #' @exportMethod
        bucketExists = function(bucketId) {
          resp <- .self$get(paste0(.self$url,"storage/buckets/", bucketId))
          if (resp$status_code == 404) FALSE
          else TRUE
        },
        #' Check to see if a table exists
        #' 
        #' @param string tableId
        #' @return boolean
        #' @exportMethod
        tableExists = function(tableId) {
          resp <- .self$get(paste0(.self$url,"storage/tables/", tableId))
          if (resp$status_code == 404) FALSE
          else TRUE
        }
    )
)
