package miniogw

import "errors"

const USED_BYTES_KEY = "usedBytes"

var BucketName string
var MaxUploadableBytes uint64

var (
	errSpaceOverflow = errors.New("space overflow")
)
