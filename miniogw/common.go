package miniogw

import "errors"

const ENDPOINT = "S3_ENDPOINT"
const REGION = "S3_REGION"
const ACCESSKEY = "S3_ACCESSKEY"
const SECRETKEY = "S3_SECRETKEY"
const BUCKETNAME = "S3_BUCKETNAME"
const MAXUPLOADEDBYTES = "S3_MAXUPLOADEDBYTES"

const USED_BYTES_KEY = "usedBytes"

var BucketName string
var MaxUploadedBytes uint64

var (
	errSpaceOverflow = errors.New("space overflow")
)
