#' Client for working with Keboola Connection Storage API.
#'
#' @import httr methods aws.s3 AzureStor
#' @exportClass SapiClient
#' @export SapiClient
SapiClient <- setRefClass(
    'SapiClient',
    fields = list(
        token = 'character',
        url = 'character',
        userAgent = 'character',
        backend = 'character'
    ),
    methods = list(
        initialize = function(
                token,
                url,
                userAgent = "Keboola StorageApi R Client/v0.5"
        ) {
            "Constructor.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{token} KBC Storage API token.}
            \\item{\\code{url} Optional URL of the provisioning API.}
            }}
            \\subsection{Return Value}{Another return value}"          
            .self$token <<- token
            .self$url <<- paste0(trimws(url, whitespace = '/'), '/v2/')
            .self$userAgent <<- userAgent
            options(azure_storage_progress_bar=FALSE)
            # check for token validity
            tryCatch(
            {  
                vf <- .self$verifyToken()
                .self$backend <<- vf$owner$fileStorageProvider
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
            content <- httr::content(response, as = "text", encoding = 'UTF-8')
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
            if (!(response$status_code %in% c(200, 201, 202))) {
                if ((class(body) == 'list') && !is.null(body$message)) {
                    stop(paste0("Error recieving response from sapi API: ", body$message))
                } else if (class(body) == 'list') {
                    stop(paste0("Error recieving response from sapi API: ", paste(body, collapse = ", "), "code: ", response$status_code))
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
        
        getFileInfo = function(fileId, federationToken = 1) {
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
        
        getFileDataIntoDataFrame = function(fileInfo)
        {
            data <- .self$getFileData(fileInfo)
            df <- data.frame()
            tryCatch(
                {
                    if (fileInfo$isSliced) {
                        df <- read.csv(textConnection(data, 'r'), header = FALSE, numerals = "no.loss")
                        # in case of empty file, fread causes error, silence it and continue with empty df
                    } else {
                        # header is included in unsliced downloads
                        df <- read.csv(textConnection(data, 'r'), header = TRUE, numerals = "no.loss")
                    }
                }, error = function(e) {
                    write(paste("error reading from", data, " most likely the file was empty: ", e), stderr())
                }
            )
            return(df)
        },
        
        getS3Object = function(key, fileInfo) {
            "Get a file from the S3 storage
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{key} Object Key to retrieve}
            \\item{\\code{list} File info list object (see \\code{getFileInfo())}.}
            }}
            \\subsection{Return Value}{Data frame with file contents}"
            objectExists <- aws.s3::object_exists(
                object = key,
                bucket = fileInfo$s3Path$bucket, 
                region = fileInfo$region,
                key = fileInfo$credentials$AccessKeyId,
                secret = fileInfo$credentials$SecretAccessKey,
                session_token = fileInfo$credentials$SessionToken
            )
            if (objectExists) {
                # get the chunk from S3 and store it in temporary file (target)
                rawOutput <- aws.s3::get_object(
                    object = key, 
                    bucket = fileInfo$s3Path$bucket, 
                    region = fileInfo$region,
                    key = fileInfo$credentials$AccessKeyId,
                    secret = fileInfo$credentials$SecretAccessKey,
                    session_token = fileInfo$credentials$SessionToken
                )
                rawToChar(rawOutput)
            } else {
                NULL
            }
        },
        
        getAbsObject = function(key, fileInfo) {
            "Get a file from the Abs storage
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{key} Object Key to retrieve}
            \\item{\\code{list} File info list object (see \\code{getFileInfo())}.}
            }}
            \\subsection{Return Value}{Data frame with file contents}"
            # Sasconnection string comes in the following format:
            # BlobEndpoint=https://kbcfshc7chguaeh2km.blob.core.windows.net;SharedAccessSignature=sv=2017-11-09&sr=c&st=2023-03-08T18:47:29Z&se=2023-03-09T06:47:29Z&sp=rwl&sig=aaaaaaaaaaa%3D
            # We need to set containerUrl to https://kbcfshc7chguaeh2km.blob.core.windows.net + containerName
            # and sas to the value of SharedAccessSignature, that is sv=2017-11-09&sr=c&...aaaaaaaaaaa%3D
            connectionString <- fileInfo$absCredentials$SASConnectionString;
            blobEndpoint <- strsplit(connectionString, ";")[[1]][1]
            blobEndpointValue <- strsplit(blobEndpoint, "=")[[1]][2]
            sharedAccessSignature <- strsplit(connectionString, ";")[[1]][2]
            sharedAccessSignatureValue <- gsub("SharedAccessSignature=", "", sharedAccessSignature)
            containerUrl = paste0(blobEndpointValue)
            
            container <- AzureStor::blob_container(
                containerUrl,
                sas=sharedAccessSignatureValue
            )
            
            rawOutput <- AzureStor::download_blob(container, src=key, dest=NULL)
            rawToChar(rawOutput)
        },
        
        getFileData = function(fileInfo) {
            "Get a file from the S3 storage
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{list} File info list object (see \\code{getFileInfo())}.}
            }}
            \\subsection{Return Value}{Data frame with file contents}"
            target <- tempfile('s3dld-')
            if (fileInfo$isSliced) {
                response <- httr::GET(fileInfo$url)
                manifest <- .self$decodeResponse(response)
                stringOutput <- ""
                for (i in seq_along(manifest$entries)) {
                    fullPath <- manifest$entries[[i]]$url
                    splittedPath <- strsplit(fullPath, "/")
                    fileKey <-  paste(splittedPath[[1]][4:length(splittedPath[[1]])], collapse = "/")
                    if (.self$backend == 'aws') {
                        stringData <- .self$getS3Object(fileKey, fileInfo)
                        stringOutput <- paste0(stringOutput, stringData)
                    } else if (.self$backend == 'azure') {
                        stringData <- .self$getAbsObject(fileKey, fileInfo)
                        stringOutput <- paste0(stringOutput, stringData)
                    } else {
                        stop(paste("Unkown backend:", .self$backend))
                    }
                }
                stringOutput
            } else {
                if (.self$backend == 'aws') {
                    .self$getS3Object(fileInfo$s3Path$key, fileInfo)
                } else if (.self$backend == 'azure') {
                    .self$getAbsObject(paste(fileInfo$absPath$container, fileInfo$absPath$name, sep='/'), fileInfo)
                } else {
                    stop(paste("Unkown backend:", .self$backend))
                }
            }    
        },
        
        uploadFile = function(dataFile, options = list()) {
            "Upload a file to Storage files.
            (compression is not yet supported by this client)
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{string} File to upload.}
            \\item{\\code{options} List options. (For a full list of options please see the api docs).}
            }}
            \\subsection{Return Value}{Integer file ID of the uploaded file.}"
            if (!("name" %in% names(options))) {
                options$name = basename(dataFile)
            }
            if (!("federationToken" %in% names(options))) {
                options$federationToken = 1
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
            if (.self$backend == 'azure') {
                .self$uploadFileAzure(resp, dataFile)
            } else if (.self$backend == 'aws') {
                .self$uploadFileAws(resp, dataFile)
            } else {
                stop(paste("Invalid backend:", .self$backend));
            }
            # return the file id of the uploaded file
            return(resp$id)
        },
        
        uploadFileAzure = function(preparedFile, dataFile) {
            # Sasconnection string comes in the following format:
            # BlobEndpoint=https://kbcfshc7chguaeh2km.blob.core.windows.net;SharedAccessSignature=sv=2017-11-09&sr=c&st=2023-03-08T18:47:29Z&se=2023-03-09T06:47:29Z&sp=rwl&sig=o89P1pW04xbgRB7wrUFOq%2BLZRJxSu%2FIgcWt32B1P1os%3D
            # We need to set containerUrl to https://kbcfshc7chguaeh2km.blob.core.windows.net + containerName
            # and sas to the value of SharedAccessSignature, that is sv=2017-11-09&sr=c&...2BLZRJxSu%2FIgcWt32B1P1os%3D
            connectionString <- preparedFile$absUploadParams$absCredentials$SASConnectionString;
            blobEndpoint <- strsplit(connectionString, ";")[[1]][1]
            blobEndpointValue <- strsplit(blobEndpoint, "=")[[1]][2]
            sharedAccessSignature <- strsplit(connectionString, ";")[[1]][2]
            sharedAccessSignatureValue <- gsub("SharedAccessSignature=", "", sharedAccessSignature)
            containerUrl = paste0(blobEndpointValue, '/', preparedFile$absUploadParams$container)
            
            container <- AzureStor::blob_container(
                containerUrl,
                sas=sharedAccessSignatureValue
            )
            
            result <- AzureStor::upload_blob(container, dataFile, dest=preparedFile$absUploadParams$blobName)
        },
        
        uploadFileAws = function(preparedFile, dataFile) {
            # put the file to AWS
            aws.s3::put_object(
                file = dataFile,
                object = preparedFile$uploadParams$key,
                bucket = preparedFile$uploadParams$bucket,
                region = preparedFile$region,
                key = preparedFile$uploadParams$credentials$AccessKeyId,
                secret = preparedFile$uploadParams$credentials$SecretAccessKey,
                session_token = preparedFile$uploadParams$credentials$SessionToken
            )
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
            tableId <- paste0(bucket,".",tableName)  
            if (.self$tableExists(tableId)) {
                posturl <- paste(.self$url,"storage/tables/", tableId, "/import-async", sep="")        
            }
            #prepare our options
            options = list(
                bucketId = bucket,
                name = tableName,
                delimiter = if ("delimiter" %in% names(opts)) opts$delimeter else ",",
                enclosure = if ("enclosure" %in% names(opts)) opts$enclosure else '"',
                escapedBy = if ("escapedBy" %in% names(opts)) opts$escapedBy else NULL,
                primaryKey = if ("primaryKey" %in% names(opts)) opts$primaryKey else NULL,
                incremental = if ("incremental" %in% names(opts)) opts$incremental else NULL,
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
            return(httr::content(resp, encoding = 'UTF-8'))
        },
        
        importTableAsync = function(tableId, options=list()) {
            "Begin an export job of a table.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{tableId} String table Id (including bucket ID).}
            \\item{\\code{options} List with additional parameters.}
            }}
            \\subsection{Return Value}{URL to ping for table creation status check.}"
            opts <- .self$prepareOptions(options)
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
            return(httr::content(response, encoding = 'UTF-8'))
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
            job <- .self$waitForJob(res)
            # if we got this far the write was successful
            # remove temporary file
            file.remove(fileName)
            if (!is.null(job$results$id)) {
                job$results$id
            } else {
                paste0(bucket, ".", tableName)
            }
        },
        
        waitForJob = function(jobResponse) {
            if (!is.null(jobResponse$error)) {
                stop(paste("Request failed:", jobResponse$error))
            }      
            repeat {
                job <- .self$getJobStatus(jobResponse$url)
                if (job$status == "success") {
                    break
                } else if (job$status != "waiting" && job$status != "processing") {
                    stop(paste0("Job status: ", job$status, ' - ', job$error$message, ' (', job$error$exceptionId, ')'))
                }
                Sys.sleep(0.5)
            }
            job
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
                    job <- .self$waitForJob(res)
                    table <- .self$getTable(tableId)
                    if ("columns" %in% names(options)) {
                        columns <- options$columns
                    } else {
                        columns <- unlist(table$columns)
                    }
                    fileInfo <- .self$getFileInfo(job$result$file$id)
                    df <- .self$getFileDataIntoDataFrame(fileInfo)
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
        
        deleteBucket = function(bucketId, force = FALSE) {
            "Delete a bucket.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{bucketId} String ID of the bucket.}
            \\item{\\code{force} Boolean to force deletion of tables in bucket.}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url, "storage/buckets/", bucketId, '?force=', as.integer(force), '&async=1'),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            .self$waitForJob(.self$decodeResponse(resp))
            TRUE
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
                encode="form",
                httr::add_headers("X-StorageApi-Token" = .self$token),
                body = list(configuration = jsonlite::toJSON(configuration, auto_unbox=TRUE))
            ) 
            if (!(resp$status_code == 200)) {
                stop(paste0(
                    resp$status_code, 
                    " Error putting component: ", component, 
                    " configuration: ", configId, 
                    ". Server Response: ", httr::content(resp, as = "text", encoding = 'UTF-8')))
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
        
        getConfigurationRow = function(componentId, configId, rowId) {
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
        
        listConfigurationRows = function(componentId, configId) {
            "Get KBC Component Configuration Rows.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{componentId} ID of the component}
            \\item{\\code{configId} ID of the configuration}
            }}
            \\subsection{Return Value}{List of component configuration rows}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/components/",componentId,"/configs/",configId, "/rows"))
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
        },
        
        createWorkspace = function(backend=NULL) {
            "Create a new workspace. If backend not specified, project default backend will be used.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{backend} Either 'redshift' or 'snowflake'.}
            }}
            \\subsection{Return Value}{The new workspace}"
            postBody <- NULL
            if (!is.null(backend)) {
                postBody <- list(backend=backend) 
            }
            resp <- httr::POST(
                paste0(.self$url,"storage/workspaces?async=1"),
                httr::add_headers("X-StorageApi-Token" = .self$token),
                body = postBody
            )   
            resp <- .self$waitForJob(.self$decodeResponse(resp))
            resp$results
        },
        
        dropWorkspace = function(workspaceId) {
            "DELETE the provided workspace.  
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{workspaceId} ID of the workspace}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url,"storage/workspaces/", workspaceId, '?async=1'),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            .self$waitForJob(.self$decodeResponse(resp))
            TRUE
        },
        
        getWorkspace = function(workspaceId) {
            "Get KBC Workspace.
            \\subsection{Parameters}{\\itemize{
            \\item{\\code{workspaceId} ID of the workspace}
            }}
            \\subsection{Return Value}{List containing workspace object}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/workspaces/",workspaceId))
            )
        },
        
        listWorkspaces = function() {
            "Get KBC Workspaces.
            \\subsection{Return Value}{List of workspace}"
            .self$decodeResponse(
                .self$get(paste0(.self$url,"storage/workspaces/"))
            )
        },
        
        listFiles = function(tags=NULL, limit=NULL, offset=NULL)
        {
            "Get list of fileinfo objects about files in sapi.
            \\subsection{Parameters}{\\itemize{
                \\item{\\code{tags} list of tags}
                \\item{\\code{limi} limit of # of files to return. SAPI default is 100}
                \\item{\\code{offset} which page of results to return, SAPI default is 0 (first page). }
            }}
            \\subsection{Return Value}{List of fileInfo objects}"
            options = list()
            if (!(is.null(tags))) {
                for (i in 1:length(tags)) {
                    options[[paste0("tags[",i-1,"]")]] <- tags[i]  
                }
            }
            if (!(is.null(limit))) {
                options[["limit"]] <- limit
            }
            if (!(is.null(offset))) {
                options[["offset"]] <- offset
            }
            .self$decodeResponse(
                .self$get(paste0(.self$url, "storage/files"), options)
            )
        },
        
        putFile = function(fileName, tags=NULL)
        {
            "Get list of fileinfo objects about files in sapi.
            \\subsection{Parameters}{\\itemize{
                \\item{\\code{fileName} fileName including path if file not in the current directory}
                \\item{\\code{tags} tags to describe the file}
            }}
            \\subsection{Return Value}{fileId of the created file}"
            options = list()
            if (!(is.null(tags))) {
                for (i in 1:length(tags)) {
                    options[[paste0("tags[",i-1,"]")]] <- tags[i]  
                }
            }
            .self$uploadFile(fileName, options=options)
        },
        
        loadFile = function(fileId, options)
        {
            "load a file from SAPI into your current R session.
            \\subsection{Parameters}{\\itemize{
                \\item{\\code{fileId} the id of the file to load (hint: use listFiles to find the id of the file you want to load)}
            }}
            \\subsection{Return Value}{the contents of the file}"
            
            fileInfo <- .self$getFileInfo(fileId)
            
            .self$getFileData(fileInfo)
        },
        
        deleteFile = function(fileId)
        {
            "load a file from SAPI into your current R session.
            \\subsection{Parameters}{\\itemize{
                \\item{\\code{fileId} the id of the file to delete}
            }}
            \\subsection{Return Value}{TRUE}"
            resp <- httr::DELETE(
                paste0(.self$url,"storage/files/", fileId),
                httr::add_headers("X-StorageApi-Token" = .self$token)
            )
            if (!(resp$status_code == 204)) {
                stop(paste0(
                    resp$status_code, 
                    " Error deleting file: ", fileId, 
                    ". Server Response: ", .self$decodeResponse(resp)))
            } else {
                TRUE
            }
        }
    )
)

