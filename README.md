# MEFS Gateway

## 1. Gateway

### 1.1 Current Situation

The following is the basic usage document:

Currently only supports uploading objects, downloading objects and listing objects, and requires a configuration file `config.toml`:

```toml
[common]
use_local = true
use_s3 = false
read_only = true
max_upload_bytes = 1099511627776 # 1TiB
bucketname = "mefstest" # set bucketname the gateway to use
root_dir = "~/.mefs_gw"
local_dir = ""
console = ":8081"
endpoint = "0.0.0.0:5081"

[s3]
endpoint = "s3-qos.my-sz-1.qiniu-solutions.com:29091"
region = "z0"
accesskey = "22taR9gKak3fXfDYT-GqSjxGc_uslfiNC0ocxPK_"
secretkey = "VHr2Q1q3dRfZ7nXZPUnmRtlM0Wh_hDAWbzniSIlZ"
```

+ `bucketname` is the specified `BucketName` for this instance.
+ `max_upload_bytes` is the upper limit of the data that a user can upload to a bucket through the gateway.

`use_local` can be used to configure whether to use the local file system to store a copy, `local_dir` can be used to configure its storage path, `root_dir` is the system configuration path, if the path option `local_dir` is not set, the local storage path is the `local` folder under the `root_dir` path, and `read_only` is configured to limit the gateway to read-only.

`use_s3` can be used to configure an s3 backend, including the following configuration items. If s3 is not used, the following configuration items can be left unconfigured. `console` and `endpoint` can be used to set the gateway's external interface.

+ `endpoint` is the access point of the dependent S3
+ `region` is the region of S3 access
+ `accesskey` is the access account of S3
+ `secretkey` is the access key of S3

The logic is that users can only upload and download data in the bucket specified by `BUCKETNAME`, and the upload data volume is limited to `MAXUPLOADEDBYTES`, in bytes.

At the same time, the Gateway only opens some interfaces: including `ListObjects`, `GetObject`, `GetObjectInfo`, `PutObject` and a few other interfaces.

When starting, you can select name and password, and specify the path where the configuration file is located. The startup effect is as follows:

```sh
$ ./mefs-gateway run -n=testuser -p=testpassword --config-path=./conf/
Use Bucket:  mefstest
Maximum uploadable bytes:  mefstest
Gateway root dir:  /home/xcshuan/.mefs_gw
Use local fs:  /home/xcshuan/.mefs_gw/local
API: http://0.0.0.0:5080
RootUser: testuser
RootPass: testpassword

Console: http://172.31.118.222:8080 http://127.0.0.1:8080
RootUser: testuser
RootPass: testpassword

Command-line: https://docs.min.io/docs/minio-client-quickstart-guide
   $ mc alias set mylfs http://0.0.0.0:5080 testuser testpassword

Documentation: https://docs.min.io
```

### 1.2 Further requirements

1. Integrate mefs-v2

First, after the mefs-v2 code is completed, the gateway needs to be integrated with mefs-v2 to use mefs as the backend storage.

2. Provide more external interfaces

Currently, the gateway only provides the s3 interface to the outside world. You can add the json api interface. You can refer to the `cmd` connection method of `go-mefs-v2` to design the api, so that the gateway can choose to enable multiple interfaces such as s3 or json-api.

3. Code optimization

localFs adds the truncation and segmentation function of `listobject`, as well as the etag calculation function.
