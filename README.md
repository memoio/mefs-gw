## 1. 网关

### 1.1 现状

代码在gitlab上，由于目前mefs-v2尚未实现，所以gateway还未对接mefs，仅只对接了s3与本地文件夹。

下面是基础的使用文档：

目前只支持上传对象、下载对象以及列举对象，需要配置一个配置文件`config.toml`：

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

+ `bucketname`为该实例的规定好的`BucketName`
+ `max_upload_bytes`为用户可通过该网关往Bucket上传数据的上限。

通过`use_local`可以配置是否使用本地文件系统存储一份，`local_dir`可配置其存储路径，`root_dir`为系统配置路径，若不设置路径选项`local_dir`，则本地存储路径为`root_dir`路径下面的`local`文件夹，配置`read_only`限定该gateway只读。

通过`use_s3`可以配置一个s3后端，包含以下配置项，如果不采用s3，则以下配置项可不配置，`console`和`endpoint`可以设置网关对外的接口。

+ `endpoint`为依赖的S3的接入点
+ `region`为S3接入的地区
+ `accesskey`为S3的访问账户
+ `secretkey`为S3的访问密钥

逻辑为，用户只能在`BUCKETNAME`指定的bucket上传数据以及下载数据，并且其上传数据量限制为`MAXUPLOADEDBYTES`，单位为字节。

同时，该Gateway仅开放了部分接口：包括，`ListObjects`，`GetObject`，`GetObjectInfo`,`PutObject`等少数接口。

启动时，可以选择name和password，并指定配置文件所在的路径，启动效果如下：

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

### 1.2 进一步需求

1. 整合mefs-v2

   首先，在mefs-v2代码完成后，需要将gateway与mefs-v2整合，以使用mefs作为后端存储。

2. 提供更多对外接口

   目前gateway对外仅只提供s3接口，可以添加json的api接口，可以参考`go-mefs-v2`的`cmd`连接方法来设计api，让gateway可选择启用s3或者json-api等多种接口。

3. 代码优化

   localFs加入`listobject`的截断分段功能，以及etag计算功能