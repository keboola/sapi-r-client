# KBC Storage R Client

[![Build Status](https://travis-ci.org/keboola/sapi-r-client.svg?branch=master)](https://travis-ci.org/keboola/sapi-r-client)

Client for using [Keboola Connection Storage API](http://docs.keboola.apiary.io/). 
This API client provides client methods to get data from KBC and put data back to KBC.
See the [documentation](https://developers.keboola.com/integrate/storage/) for more information.

## Examples
```
# installation
# first need to install the devtools package if it isn't already installed
install.packages("devtools")
# load the library
library(devtools)

# this package relies on another github package 
# for aws requests
devtools::install_github("cloudyr/aws.s3")

# install the sapi client package
devtools::install_github("keboola/sapi-r-client")

# load the library (dependencies will be loaded automatically)
library("keboola.sapi.r.client")

# create client
client <- SapiClient$new(
    token = 'your-token'
)

# verify the token
tokenDetails <- client$verifyToken()

# create a bucket
bucket <- client$createBucket("new_bucket","in","A brand new Bucket!")

# create a table
table <- client$saveTable(myDataFrame, bucket$id, "new_table")
# note: as of version 0.2.0 if the table exists it will be over-written unless the incremental option is specified
table <- client$saveTable(myDataFrame, bucket$id, "existing_table", options = list(incremental = 1))

# import a table
mydata <- client$importTable('in.c-my_bucket.my_table')

# list buckets
buckets <- client$listBuckets()

# list all tables in a bucket
tables <- client$listTables(bucket = bucket$id)

# list all tables
tables <- client$listTables()

# delete table
client$deleteTable(table$id)

# delete bucket
client$deleteBucket(bucket$id)

# list files with tag "my-tag"
client$listFiles(tags=c("my-tag")

# upload a file to sapi with tags "my-tag" and "our-tag"
newFileId <- client$putFile("path/to/my/file.whatever", tags=c("my-tag", "our-tag"))

# download a file from sapi
myFilePath <- client$loadFile(newFileId)
# now you can read it however you like (ex csv)
myFileContents <- read.csv(myFilePath)

# delete a file 
client$deleteFile(newFileId)
```

The only required argument to create a client is a valid Storage API token and API URL.
