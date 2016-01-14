#' Client for working with Keboola Connection Storage API.
#'
#' @import httr methods aws.signature data.table
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
        initialize = function(
                token,
                url = 'https://connection.keboola.com/v2/',
                userAgent = "Keboola StorageApi R Client/v2"
        ) {
            "Constructor.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{token} KBC Storage API token.}
            \\item{\\code{url} Optional URL of the provisioning API.}
            }}
            \\subsection{Return Value}{Another return value}"          
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

        decodeResponse = function(response) {
            "Internal method to process API response.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{response} List as returned from httr POST/GET method.}
            }}
            \\subsection{Return Value}{Response body - either list or string in case the body cannot be parsed as JSON.}"
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

        get = function(urlG, query = NULL) {
            "Generic GET method.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{urlG} Target URL.}
            \\item{\\code{query} List of query arguments ex. list(foo = bar).}
            }}
            \\subsection{Return Value}{List with HTTP response.}"
            if ((class(query) == 'list') && (length(query) == 0)) {
                # if query is an empty list, convert it to NULL, otherwise httr::GET will botch the request
                query <- NULL
            }
            httr::GET(urlG, httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent), query = query)
        },

        genericPost = function(urlP, data = NULL) {
            "Generic POST method.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{urlP} Target URL.}
            \\item{\\code{data} Body of the request.}
            }}
            \\subsection{Return Value}{Decoded JSON body as a list of items.}"
            .self$decodeResponse(
                httr::POST(urlP,
                    httr::add_headers(
                        "X-StorageApi-Token" = .self$token, 
                        "User-Agent" = .self$userAgent
                    ),
                    body = data
                )
            )
        },
        
        genericGet = function(urlG, query = NULL) {
            "Generic POST method.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{urlG} Target URL.}
            \\item{\\code{query} Query parameters.}
            }}
            \\subsection{Return Value}{Decoded JSON body as a list of items.}"
            if ((class(query) == 'list') && (length(query) == 0)) {
                # if query is an empty list, convert it to NULL, otherwise httr::GET will botch the request
                query <- NULL
            }
            resp <- httr::GET(
                urlG, 
                httr::add_headers(
                    "X-StorageApi-Token" = .self$token, 
                    "User-Agent" = .self$userAgent
                ),
                query = query
            )
            .self$decodeResponse(resp)
        },
        
        genericDelete = function(urlD, query = NULL) {
            "Generic DELETE method.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{urlD} Target URL.}
            \\item{\\code{query} Query parameters.}
            }}
            \\subsection{Return Value}{TRUE for success. Will throw error if status code returned is not 204.}"
            resp <- httr::DELETE(
                urlD,
                httr::add_headers(
                    "X-StorageApi-Token" = .self$token,
                    "User-Agent" = .self$userAgent
                ),
                query = query
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(resp$status_code, " Unnexpected Status code  ", .self$decodeResponse(resp)))
            } else {
                TRUE
            }
        },
        
        prepareOptions = function(options) {
            "Internal helper for parsing options
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{options} List.}
            }}
            \\subsection{Return Value}{List with options}"
            # which parameters are allowed
            params <- c("limit", "changedSince", "changedUntil", "whereColumn", "whereValues")
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
            return(opts)
        },
        
        verifyToken = function() {
            "Get details of the current token
            \\subsection{Return Value}{List containing details of this client's token}"
            .self$decodeResponse(.self$get(paste0(.self$url, "storage/tokens/verify")))
        },
        
        getJobStatus = function(url) {
            "Make a status request to an async syrup job
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{url} This will normally be the url returned from the 
            createTableAsync/importTableAsync methods.}
            }}
            \\subsection{Return Value}{List with job details}"
            .self$decodeResponse(.self$get(url))
        },
        
        getFileInfo = function(fileId, federationToken = TRUE) {
            "Get information about a file, including credentials.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{fileId} Storage file ID.}
            \\item{\\code{federationToken} Use federation token?.}
            }}
            \\subsection{Return Value}{List with file information}"
            .self$decodeResponse(
                .self$get(
                    paste0(url, "storage/files/", fileId), 
                    query = list(federationToken = federationToken)
                )
            )
        },
        
        getFileData = function(fileInfo) {
            "Get a file from the S3 storage
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{list} File info list object (see \\code{getFileInfo())}.}
            }}
            \\subsection{Return Value}{Data frame with file contents}"
            if (fileInfo$isSliced) {
                response <- httr::GET(fileInfo$url)
                manifest <- .self$decodeResponse(response)
                target <- tempfile('s3dld-')
                for (i in seq_along(manifest$entries)) {
                    fullPath <- manifest$entries[[i]]$url
                    splittedPath <- strsplit(fullPath, "/")
                    fileKey <-  paste(splittedPath[[1]][4:length(splittedPath[[1]])], collapse = "/")
                    bucket <- fileInfo$s3Path$bucket
                    # get the chunk from S3 and store it in temporary file (target)
                    .self$s3GET(
                        fileInfo$region, 
                        paste0("https://", bucket, ".s3.amazonaws.com/", fileKey), 
                        fileInfo$credentials, 
                        target
                    )
                }
            } else {
                # single file, so just get it
                bucket <- fileInfo$s3Path$bucket
                key <- fileInfo$s3Path$key
                .self$s3GET(
                    fileInfo$region, 
                    paste0("https://", bucket, ".s3.amazonaws.com/", key), 
                    fileInfo$credentials, 
                    target
                )
            }
            df <- data.frame()
            tryCatch(
                {
                    df <- data.table::fread(target, header = FALSE)
                    # in case of empty file, fread causes error, silence it and continue with empty df
                }, error = function(e) {}
            )
            return(df)
        },
        
        uploadFile = function(dataFile, options = list()) {
            "Upload a file to AWS S3 bucket.
            (compression is not yet supported by this client)
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{string} File to upload.}
            \\item{\\code{options} List options. (For a full list of options please see the api docs).}
            }}
            \\subsection{Return Value}{Integer file ID of the uploaded file.}"
            if (!("name" %in% names(options))) {
                options$name = basename(dataFile)
            }
            resp <- tryCatch(
                {
                .self$decodeResponse(
                    httr::POST(paste0(.self$url,"storage/files/prepare"),
                        httr::add_headers("X-StorageApi-Token" = .self$token),
                        body = options
                    )
                )}, error = function(e) {
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
            res <- tryCatch(
                {
                    httr::POST(uploadParams$url, body=body)
                }, error = function(e) {
                    stop(paste("error uploading file", e))
                }, warning = function(w) {
                    stop(paste("file upload warning recieved:", w$message))
                }
            )
            # return the file id of the uploaded file
            return(resp$id)
        },
        
        saveTableAsync = function(bucket, tableName, fileId, opts = list()) {
            "Create/Overwrite a new table in a bucket ascynchronously.
            Generally use the \\code{saveTable} method
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucket} Storage bucket ID.}
            \\item{\\code{tableName} Table name.}
            \\item{\\code{fileId} Id of file received from the \\code{uploadFile} method.}
            \\item{\\code{opts} List with additional parameters.}
            }}
            \\subsection{Return Value}{URL to ping for table creation status check.}"
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
          
            resp <- tryCatch( 
                {
                    httr::POST(
                        posturl, 
                        httr::add_headers("X-StorageApi-Token" = .self$token),
                        body = options
                    )
                }, error = function(e) {
                    stop(paste("error creating table", e))
                }, warning = function(w) {
                    stop(paste("warning recieved creating table:", w$message))
                }
            )
            return(httr::content(resp))
        },
        
        importTableAsync = function(tableId, options=list()) {
            "Begin an export job of a table.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} String table Id (including bucket ID).}
            \\item{\\code{options} List with additional parameters.}
            }}
            \\subsection{Return Value}{URL to ping for table creation status check.}"
            opts <- .self$prepareOptions(options)
            if (!("federationToken" %in% names(opts))) {
                opts$federationToken = TRUE
            }
            response <- tryCatch(
                {
                    httr::POST(
                        paste0(.self$url,"storage/tables/", tableId, "/export-async"),
                        httr::add_headers("X-StorageApi-Token" = .self$token, "User-Agent" = .self$userAgent),
                        body = opts
                    )
                }, error = function(e) {
                    stop(paste("error posting file to sapi", e))
                }, warning = function(w) {
                    stop(paste("Save file warning recieved: ", w$message))
                }
            )
            return(httr::content(response))
        },
        
        saveTable = function(df, bucket, tableName, fileName="tmpfile.csv", options=list()) {
            "Save a table to Storage. Wrapper for the \\code{saveTableAsync} function.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{df} data.frame to save.}
            \\item{\\code{bucket} String bucket ID.}
            \\item{\\code{tableName} String table name.}
            \\item{\\code{fileName} String name of temporary file to use, will be deleted when done.}
            \\item{\\code{options} List with additional options.}
            }}
            \\subsection{Return Value}{String job ID}"
            write.csv(df, file = fileName, row.names = FALSE)
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
        
        importTable = function(tableId, options = list()) {
            "Import a table from Storage into R. Wrapper for the \\code{importTableAsync} function.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} String table ID (including bucket ID).}
            \\item{\\code{options} List with additional options.}
            }}
            \\subsection{Return Value}{data.frame with table contents.}"
            tryCatch(
                {
                    res <- .self$importTableAsync(tableId, options = options)
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
                    } else {
                        columns <- unlist(table$columns)
                    }
                    fileInfo <- .self$getFileInfo(job$result$file$id)
                    df <- .self$getFileData(fileInfo)
                    if (nrow(df) == 0) {
                        # data frame is empty and it has unfortunately already been truncated
                        # to have no columns either - make a new empty DF, but with the right columns
                        df <- as.data.frame(setNames(replicate(length(columns), character(0), simplify = FALSE), columns), stringsAsFactors = FALSE)
                    } else {
                        colnames(df) <- columns
                    }
                    return(df)
                }, error = function(e) {
                    stop(e$message)
                }
            )
        },
        
        listBuckets = function(options = list()) {
            "Get a list of all buckets.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{options} List with additional options.}
            }}
            \\subsection{Return Value}{List of buckets.}"
            resp <- .self$get(paste(.self$url,"storage/buckets",sep=""),options)
            body <- .self$decodeResponse(resp)

            if (class(body) != 'list') {
                str <- print(body)
                stop(paste0("Malformed response from storage API: ", str))
            }
            body
        },
        
        listTables = function(bucket = NULL, options = list()) {
            "Get a list of all tables.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucket} Bucket ID (if empty, list all tables).}
            \\item{\\code{options} List with additional options.}
            }}
            \\subsection{Return Value}{List of tables.}"
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
            return(body)
        },

        getTable = function(tableId) {
            "Get table information.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} Table ID (including bucket ID).}
            }}
            \\subsection{Return Value}{List with table details.}"
            .self$decodeResponse(.self$get(paste0(.self$url,"storage/tables/", tableId)))
        },

        getBucket = function(bucketId) {
            "Get bucket information.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucketId} Bucket ID.}
            }}
            \\subsection{Return Value}{List with bucket details.}"
            .self$decodeResponse(.self$get(paste0(.self$url,"storage/buckets/", bucketId)))
        },
        
        createBucket = function(name, stage, description, backend = "redshift") {
            "Create a new bucket.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{name} Name of the bucket.}
            \\item{\\code{stage} One of \\code{in}, \\code{out}, \\code{sys}.}
            \\item{\\code{description} Arbitrary description of the bucket.}
            \\item{\\code{name} Database backend - eithe \\code{mysql} or \\code{redshift}.}
            }}
            \\subsection{Return Value}{List with bucket details.}"
            options = list(
                name = name,
                stage = stage,
                description = description,
                backend = backend
            )
            resp <- tryCatch(
                {
                    httr::POST(
                        paste0(.self$url,"storage/buckets"), 
                        httr::add_headers("X-StorageApi-Token" = .self$token),
                        body = options
                    )
                }, error = function(e) {
                    stop(paste("error posting fle to sapi", e))
                }, warning = function(w) {
                    stop(paste("attempting save file warning recieved:", w$message))
                }
            )
            .self$decodeResponse(resp)
        },
        
        deleteBucket = function(bucketId) {
            "Delete a bucket.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucketId} String ID of the bucket.}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url, "storage/buckets/", bucketId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(resp$status_code, " Error deleting bucket ", bucketId))
            } else {
                TRUE
            }
        },
        
        deleteTable = function(tableId) {
            "Delete a table.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} String ID of the table (including bucket ID).}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url, "storage/tables/", tableId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(resp$status_code, " Error deleting table ", tableId))
            } else {
                TRUE
            }
        },
        
        s3GET = function(region, url, credentials, target) {
            "Get a file (or file chunk) from AWS S3 storage.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{region} AWS file region.}
            \\item{\\code{url} file URL to get.}
            \\item{\\code{credentials} List with file credentials (secret and access key).}
            \\item{\\code{target} String file to which contents will be appended.}
            }}
            \\subsection{Return Value}{TRUE}"
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
                canonical_headers = list(
                    host = p$hostname,
                    `x-amz-date` = d_timestamp,
                    `x-amz-security-token` = credentials$SessionToken
                ),
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
            cat(httr::content(r, as = "text"), file = target, append = TRUE)
            NULL
        },
        
        bucketExists = function(bucketId) {
            "Check that a bucket exists.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucketId} Bucket ID.}
            }}
            \\subsection{Return Value}{TRUE or FALSE}"
            resp <- .self$get(paste0(.self$url,"storage/buckets/", bucketId))
            if (resp$status_code == 404) {
                FALSE
            } else {
                TRUE
            }
        },
        
        tableExists = function(tableId) {
            "Check that a bucket exists.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} Table ID (including bucket ID).}
            }}
            \\subsection{Return Value}{TRUE or FALSE}"
            resp <- .self$get(paste0(.self$url,"storage/tables/", tableId))
            if (resp$status_code == 404) {
                FALSE
            } else {
                TRUE
            }
        },
        
        createCredentials = function(bucketId, credentialsName) {
            "Create read-only redshift credentials for a given bucket.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucketId} Bucket ID.}
            \\item{\\code{credentialsName} Descriptive name for the credentials.}
            }}
            \\subsection{Return Value}{List with redshift credentials}"
            resp <- tryCatch(
                {
                    httr::POST(
                        paste0(.self$url,"storage/buckets/", bucketId,"/credentials"),
                        httr::add_headers("X-StorageApi-Token" = .self$token),
                        body = list(name=credentialsName)
                    )
                }, error = function(e) {
                    stop(paste("error posting fle to sapi", e))
                }, warning = function(w) {
                    stop(paste("attempting save file warning recieved:", w$message))
                }
            )
            .self$decodeResponse(resp) 
        },
              
        listCredentials = function() {
            "List read-only redshift credentials.
            \\subsection{Return Value}{List with redshift credentials}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/credentials"))
            )
        },
        
        getCredentials = function(credentialsId) {
            "List read-only redshift credentials with a given name.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{credentialsId} Credentials name.}
            }}
            \\subsection{Return Value}{List with redshift credentials}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/credentials/",credentialsId))
            )
        },
              
        deleteCredentials = function(credentialsId) {
            "Delete redshift credentials.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{credentialsId} Credentials ID}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url,"storage/credentials/", credentialsId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(resp$status_code, " Error deleting credentials ", credentialsId))
            } else {
                TRUE
            }
        },
        
        newComponentConfiguration = function(componentId, configurationId, name, description="") {
            "Create a new component configuration.
            Note that the configuration property must be put in a subsequent PUT call.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{configurationId} Configuration ID.}
            \\item{\\code{name} Name of the cconfiguration}
            \\item{\\code{description} Descriptiion for the configuration.}
            }}
            \\subsection{Return Value}{List with component configuration}"
            resp <- tryCatch(
            {
                httr::POST(
                    paste0(.self$url,"storage/components/", componentId, "/configs"),
                    httr::add_headers("X-StorageApi-Token" = .self$token),
                    body = list(
                        configurationId=configurationId,
                        name=name,
                        description=description
                    )
                )
            }, error = function(e) {
                stop(paste("error posting fle to sapi", e))
            }, warning = function(w) {
                stop(paste("attempting save file warning recieved:", w$message))
            })
            .self$decodeResponse(resp)    
        },
            
        getComponentConfiguration = function(componentId, configId) {
            "Get KBC Component Configuration.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            }}
            \\subsection{Return Value}{List containing component configuration}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/components/",componentId,"/configs/",configId))
            )  
        },
        
        putComponentConfiguration = function(componentId, configId, configuration) {
            "PUT the configuration property of the KBC Component Configuration.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            \\item{\\code{configuration} the configuration property (should be of type list)}
            }}
            \\subsection{Return Value}{list of the new component configuration}"
            resp <- httr::PUT(
                paste0(.self$url,"storage/components/",componentId,"/configs/",configId),
                httr::add_headers("X-StorageApi-Token" = .self$token),
                body = list(configuration = jsonlite::toJSON(configuration, auto_unbox=TRUE))
            ) 
            if (!(resp$status_code == 200)) {
                stop(paste0(
                    resp$status_code, 
                    " Error putting component: ", component, 
                    " configuration: ", configId, 
                    ". Server Response: ", httr::content(resp, as = "text")))
            } else {
                .self$decodeResponse(resp)
            }
        },
        
        deleteComponentConfiguration = function(componentId, configId) {
            "DELETE the provided component configuration.  CAUTION: this action is irreversible.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url,"storage/components/", componentId, "/configs/", configId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(
                    resp$status_code, 
                    " Error deleting component: ", componentId, 
                    " configuration: ", configId, 
                    ". Server Response: ", .self$decodeResponse(resp)))
            } else {
                TRUE
            }
        },
        
        createConfigurationRow = function(componentId, configId, rowId, configuration = list()) {
            "Create a new component configuration row.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} Component ID.}
            \\item{\\code{configurationId} Configuration ID.}
            \\item{\\code{rowId} ID for the row}
            \\item{\\code{configuration} The configuration to be stored as the row.}
            }}
            \\subsection{Return Value}{The component configuration row}"
            tryCatch({
                resp <- httr::POST(
                    paste0(.self$url,"storage/components/", componentId, "/configs/", configId, "/rows"),
                    httr::add_headers("X-StorageApi-Token" = .self$token),
                    body = list(
                        rowId=rowId,
                        configuration=jsonlite::toJSON(configuration, auto_unbox=TRUE)
                    )
                )    
            }, error = function(e) {
                stop(paste("Error posting component", componentId, "configuration", configId, "row:", rowId," to sapi", e))
            })
            .self$decodeResponse(resp)
        },
        
        updateConfigurationRow = function(componentId, configId, rowId, configuration = list()) {
            "PUT the configuration property of the KBC Component Configuration Row.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            \\item{\\code{rowId} ID of the row}
            \\item{\\code{configuration} the configuration property (should be of type list)}
            }}
            \\subsection{Return Value}{list of the new component configuration}"
            resp <- httr::PUT(
                paste0(.self$url,"storage/components/", componentId, "/configs/", configId, "/rows/", rowId),    
                httr::add_headers("X-StorageApi-Token" = .self$token),
                body = list(
                    configuration=jsonlite::toJSON(configuration, auto_unbox=TRUE)
                )
            )    
        }, 
        
        getComponentConfigurationRow = function(componentId, configId, rowId) {
            "Get KBC Component Configuration Row.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            \\item{\\code{rowId} ID of the configuration row}
            }}
            \\subsection{Return Value}{List containing component configuration}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/components/",componentId,"/configs/",configId, "/rows/", rowId))
            )  
        },
        
        deleteConfigurationRow = function(componentId, configId, rowId, configuration = list()) {
            "DELETE the provided component configuration row.  
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            \\item{\\code{rowId} ID of the configurationRow}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url,"storage/components/", componentId, "/configs/", configId, "/rows/", rowId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(
                    resp$status_code, 
                    " Error deleting component: ", componentId, 
                    " configuration: ", configId, 
                    " row: ", rowId,
                    ". Server Response: ", .self$decodeResponse(resp)))
            } else {
                TRUE
            }
        }
    )
)

