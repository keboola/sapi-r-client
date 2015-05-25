# KBC Storage R Client

Client for using [Keboola Connection Storage API](http://docs.keboola.apiary.io/). This API client 
provides client methods to get data from KBC and put data back to KBC.

## Examples
```
# install the package
library(devtools)
devtools::install_github("keboola/sapi-r-client")

# create client
client <- SapiClient$new(
    token = 'your-token'
)

# verify the token
tokenDetails <- client$verifyToken()

# create a bucket
bucket <- client$createBucket("new_bucket","in","A brand new Bucket!")

# create a table
table <- client$saveTable(bucket$id,"new_table","tmpfile.csv")

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

```

The only required argument to create a client is a valid Storage API token.

Please see the storage api documentation for further info:
http://docs.keboola.apiary.io/

