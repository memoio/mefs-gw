package miniogw

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	minio "github.com/minio/minio/cmd"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/memoio/mefs-gateway/utils"
)

// Start gateway
func Start(addr, pwd, endPoint, consoleAddress string) error {
	minio.RegisterGatewayCommand(cli.Command{
		Name:            "lfs",
		Usage:           "Mefs Log File System Service (LFS)",
		Action:          mefsGatewayMain,
		HideHelpCommand: true,
	})

	err := os.Setenv("MINIO_ROOT_USER", addr)
	if err != nil {
		return err
	}
	err = os.Setenv("MINIO_ROOT_PASSWORD", pwd)
	if err != nil {
		return err
	}

	rootpath, err := utils.BestKnownPath()
	if err != nil {
		return err
	}

	gwConf := rootpath + "/gwConf"

	// ”memoriae“ is app name
	// "gateway" represents gatewat mode; respective, "server" represents server mode
	// "lfs" is subcommand, should equal to RegisterGatewayCommand{Name}
	go minio.Main([]string{"memoriae", "gateway", "lfs",
		"--address", endPoint, "--config-dir", gwConf, "--console-address", consoleAddress})

	return nil
}

// Handler for 'minio gateway oss' command line.
func mefsGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &Mefs{"lfs"})
}

// Mefs implements Lfs Gateway.
type Mefs struct {
	host string
}

// Name implements Gateway interface.
func (g *Mefs) Name() string {
	return "lfs"
}
func (g *Mefs) new(creds madmin.Credentials, transport http.RoundTripper) (*miniogo.Core, error) {
	// Override default params if the host is provided
	endpoint := os.Getenv(ENDPOINT)
	region := os.Getenv(REGION)
	accessKey := os.Getenv(ACCESSKEY)
	secretKey := os.Getenv(SECRETKEY)
	optionsStaticCreds := &miniogo.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       false,
		Region:       region,
		BucketLookup: miniogo.BucketLookupAuto,
		Transport:    transport,
	}

	clntStatic, err := miniogo.New(endpoint, optionsStaticCreds)
	if err != nil {
		return nil, err
	}

	// if static keys are valid always use static keys.
	return &miniogo.Core{Client: clntStatic}, nil
}

// NewGatewayLayer implements Gateway interface and returns LFS ObjectLayer.
func (g *Mefs) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	//uploads := NewMultipartUploads()

	metrics := minio.NewMetrics()

	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	var err error
	// 读取设定好的bucketName
	BucketName = os.Getenv(BUCKETNAME)
	if len(BucketName) == 0 {
		return nil, errors.New("bucketname not set")
	}

	// 读取设定好的可上传字节数
	maxBytes := os.Getenv(MAXUPLOADEDBYTES)
	if len(maxBytes) == 0 {
		return nil, errors.New("maxBytes not set")
	}

	MaxUploadedBytes, err = strconv.ParseUint(maxBytes, 10, 64)
	if err != nil {
		return nil, err
	}

	clnt, err := g.new(creds, t)
	if err != nil {
		return nil, err
	}

	rootpath, err := utils.BestKnownPath()
	if err != nil {
		return nil, err
	}

	// 检查是否有指定的bucket
	ctx := context.TODO()
	bs, err := clnt.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(bs); i++ {
		if bs[i].Name == BucketName {
			break
		}

		// 如果没有就创建这个bucket
		if i >= len(bs)-1 {
			if utils.CheckValidBucketName(BucketName) != nil {
				return nil, minio.BucketNameInvalid{Bucket: BucketName}
			}

			err = clnt.MakeBucket(ctx,
				BucketName,
				miniogo.MakeBucketOptions{Region: REGION})
			if err != nil {
				return nil, err
			}
		}
	}

	db, err := leveldb.OpenFile(path.Join(rootpath, "db"), &opt.Options{})
	if err != nil {
		return nil, err
	}

	gw := &lfsGateway{
		Client: clnt,
		db:     db,
	}

	err = gw.readUsedBytes()
	if err != nil {
		return nil, err
	}

	return gw, nil
}

// Production - oss is production ready.
func (g *Mefs) Production() bool {
	return false
}

// lfsGateway implements gateway.
type lfsGateway struct {
	minio.GatewayUnsupported
	// multipart *MultipartUploads
	sync.Mutex
	Client    *miniogo.Core
	db        *leveldb.DB
	usedBytes uint64
}

func (l *lfsGateway) readUsedBytes() error {
	l.Lock()
	defer l.Unlock()
	val, err := l.db.Get([]byte(USED_BYTES_KEY), &opt.ReadOptions{})
	if err == leveldb.ErrNotFound {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, 0)
		err = l.db.Put([]byte(USED_BYTES_KEY), buf, &opt.WriteOptions{})
		if err != nil {
			return err
		}

		l.usedBytes = 0

		return nil
	}

	l.usedBytes = binary.BigEndian.Uint64(val)

	return nil
}

func (l *lfsGateway) addUsedBytes(addedBytes uint64) error {
	l.Lock()
	defer l.Unlock()
	buf := make([]byte, 8)
	l.usedBytes += addedBytes
	binary.BigEndian.PutUint64(buf, l.usedBytes)
	err := l.db.Put([]byte(USED_BYTES_KEY), buf, &opt.WriteOptions{})
	if err != nil {
		return err
	}

	return nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *lfsGateway) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to LFS backend.
func (l *lfsGateway) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	si.Backend.Type = madmin.Gateway
	si.Backend.GatewayOnline = l.Client.IsOnline()

	si.Disks = make([]madmin.Disk, 1)
	si.Disks[0].DiskIndex = 0
	si.Disks[0].UsedSpace = l.usedBytes
	si.Disks[0].TotalSpace = MaxUploadedBytes

	return si, nil
}

// 不允许创建桶
// MakeBucketWithLocation creates a new container on LFS backend.
func (l *lfsGateway) MakeBucketWithLocation(ctx context.Context, bucket string, options minio.BucketOptions) error {
	return minio.NotImplemented{}
	// if options.LockEnabled || options.VersioningEnabled {
	// 	return minio.NotImplemented{}
	// }

	// if utils.CheckValidBucketName(bucket) != nil {
	// 	return minio.BucketNameInvalid{Bucket: bucket}
	// }

	// err := l.Client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{Region: options.Location})
	// if err != nil {
	// 	return minio.ErrorRespToObjectError(err, bucket)
	// }
	// return err
}

// GetBucketInfo gets bucket metadata.
func (l *lfsGateway) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	if bucket != BucketName {
		return bi, minio.BucketNotFound{Bucket: bucket}
	}

	buckets, err := l.Client.ListBuckets(ctx)
	if err != nil {
		// Listbuckets may be disallowed, proceed to check if
		// bucket indeed exists, if yes return success.
		var ok bool
		if ok, err = l.Client.BucketExists(ctx, bucket); err != nil {
			return bi, minio.ErrorRespToObjectError(err, bucket)
		}
		if !ok {
			return bi, minio.BucketNotFound{Bucket: bucket}
		}
		return minio.BucketInfo{
			Name:    bi.Name,
			Created: time.Now().UTC(),
		}, nil
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets lists all LFS buckets.
func (l *lfsGateway) ListBuckets(ctx context.Context) (bs []minio.BucketInfo, err error) {
	buckets, err := l.Client.ListBuckets(ctx)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err)
	}

	bs = make([]minio.BucketInfo, 0, 1)
	for _, bi := range buckets {
		if bi.Name == BucketName {
			bs = append(bs, minio.BucketInfo{
				Name:    bi.Name,
				Created: bi.CreationDate,
			})
		}
	}

	return bs, err
}

// DeleteBucket deletes a bucket on LFS.
func (l *lfsGateway) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	return minio.NotImplemented{}
	// err := l.Client.RemoveBucket(ctx, bucket)
	// if err != nil {
	// 	return minio.ErrorRespToObjectError(err, bucket)
	// }
	// return nil
}

// ListObjects lists all blobs in LFS bucket filtered by prefix.
func (l *lfsGateway) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	if bucket != BucketName {
		return loi, minio.BucketNotFound{Bucket: bucket}
	}
	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

// ListObjectsV2 lists all blobs in LFS bucket filtered by prefix
func (l *lfsGateway) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {
	if bucket != BucketName {
		return loiv2, minio.BucketNotFound{Bucket: bucket}
	}

	result, err := l.Client.ListObjectsV2(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys)
	if err != nil {
		return loiv2, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketV2Result(bucket, result), nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *lfsGateway) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	if bucket != BucketName {
		return nil, minio.BucketNotFound{Bucket: bucket}
	}
	var objInfo minio.ObjectInfo
	objInfo, err = l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	fn, off, length, err := minio.NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}

	pr, pw := io.Pipe()
	go func() {
		err := l.GetObject(ctx, bucket, object, off, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return fn(pr, h, pipeCloser)
}

// GetObject reads an object on LFS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *lfsGateway) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	if bucket != BucketName {
		return minio.BucketNotFound{Bucket: bucket}
	}
	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucket, key)
	}

	opts := miniogo.GetObjectOptions{}
	opts.ServerSideEncryption = o.ServerSideEncryption

	if startOffset >= 0 && length >= 0 {
		if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
			return minio.ErrorRespToObjectError(err, bucket, key)
		}
	}

	if etag != "" {
		opts.SetMatchETag(etag)
	}

	object, _, _, err := l.Client.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket, key)
	}
	defer object.Close()
	if _, err := io.Copy(writer, object); err != nil {
		return minio.ErrorRespToObjectError(err, bucket, key)
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *lfsGateway) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if bucket != BucketName {
		return objInfo, minio.BucketNotFound{Bucket: bucket}
	}
	//// for s3 fuse
	//ud := make(map[string]string)
	//ud["x-amz-meta-mode"] = "33204"
	//ud["x-amz-meta-mtime"] = strconv.FormatInt(time.Now().Unix(), 10)
	//// need handle ETag
	//objInfo = minio.ObjectInfo{
	//	Bucket:      bucket,
	//	Name:        object,
	//	UserDefined: ud,
	//}
	//
	//return objInfo, nil
	oi, err := l.Client.StatObject(ctx, bucket, object, miniogo.StatObjectOptions{
		ServerSideEncryption: opts.ServerSideEncryption,
	})
	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// PutObject creates a new object with the incoming data.
func (l *lfsGateway) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if bucket != BucketName {
		return objInfo, minio.BucketNotFound{Bucket: bucket}
	}

	data := r.Reader
	var tagMap map[string]string
	if tagstr, ok := opts.UserDefined["X-Amz-Tagging"]; ok && tagstr != "" {
		tagObj, err := tags.ParseObjectTags(tagstr)
		if err != nil {
			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}
		tagMap = tagObj.ToMap()
		delete(opts.UserDefined, "X-Amz-Tagging")
	}
	putOpts := miniogo.PutObjectOptions{
		UserMetadata:         opts.UserDefined,
		ServerSideEncryption: opts.ServerSideEncryption,
		UserTags:             tagMap,
		SendContentMd5:       true,
	}

	var reader io.Reader = data
	// 限制上传总量
	if data.Size() == -1 {
		reader = newLimitedReader(data, MaxUploadedBytes-l.usedBytes)
	} else if uint64(data.Size()) > MaxUploadedBytes-l.usedBytes {
		return objInfo, errSpaceOverflow
	}

	ui, err := l.Client.PutObject(ctx, bucket, object, reader, data.Size(), data.MD5Base64String(), data.SHA256HexString(), putOpts)
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
	}
	// On success, populate the key & metadata so they are present in the notification
	oi := miniogo.ObjectInfo{
		ETag:     ui.ETag,
		Size:     ui.Size,
		Key:      object,
		Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
	}

	// 记录新增的存储空间
	l.addUsedBytes(uint64(ui.Size))

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *lfsGateway) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return objInfo, minio.NotImplemented{}
	// if srcOpts.CheckPrecondFn != nil && srcOpts.CheckPrecondFn(srcInfo) {
	// 	return minio.ObjectInfo{}, minio.PreConditionFailed{}
	// }
	// // Set this header such that following CopyObject() always sets the right metadata on the destination.
	// // metadata input is already a trickled down value from interpreting x-amz-metadata-directive at
	// // handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// // So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	// srcInfo.UserDefined["x-amz-metadata-directive"] = "REPLACE"
	// srcInfo.UserDefined["x-amz-copy-source-if-match"] = srcInfo.ETag
	// header := make(http.Header)
	// if srcOpts.ServerSideEncryption != nil {
	// 	encrypt.SSECopy(srcOpts.ServerSideEncryption).Marshal(header)
	// }

	// if dstOpts.ServerSideEncryption != nil {
	// 	dstOpts.ServerSideEncryption.Marshal(header)
	// }

	// for k, v := range header {
	// 	srcInfo.UserDefined[k] = v[0]
	// }

	// if _, err = l.Client.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo.UserDefined, miniogo.CopySrcOptions{}, miniogo.PutObjectOptions{}); err != nil {
	// 	return objInfo, minio.ErrorRespToObjectError(err, srcBucket, srcObject)
	// }
	// return l.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts)
}

// DeleteObject deletes a blob in bucket.
func (l *lfsGateway) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, minio.NotImplemented{}
	// err := l.Client.RemoveObject(ctx, bucket, object, miniogo.RemoveObjectOptions{})
	// if err != nil {
	// 	return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	// }

	// return minio.ObjectInfo{
	// 	Bucket: bucket,
	// 	Name:   object,
	// }, nil
}

func (l *lfsGateway) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx := range objects {
		errs[idx] = minio.NotImplemented{}

	}

	return dobjects, errs
}

// // SetBucketPolicy sets policy on bucket.
// // LFS supports three types of bucket policies:
// // oss.ACLPublicReadWrite: readwrite in minio terminology
// // oss.ACLPublicRead: readonly in minio terminology
// // oss.ACLPrivate: none in minio terminology
// func (l *lfsGateway) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
// 	data, err := json.Marshal(bucketPolicy)
// 	if err != nil {
// 		// This should not happen.

// 		return minio.ErrorRespToObjectError(err, bucket)
// 	}

// 	if err := l.Client.SetBucketPolicy(ctx, bucket, string(data)); err != nil {
// 		return minio.ErrorRespToObjectError(err, bucket)
// 	}

// 	return nil
// }

// // GetBucketPolicy will get policy on bucket.
// func (l *lfsGateway) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
// 	data, err := l.Client.GetBucketPolicy(ctx, bucket)
// 	if err != nil {
// 		return nil, minio.ErrorRespToObjectError(err, bucket)
// 	}

// 	bucketPolicy, err := policy.ParseConfig(strings.NewReader(data), bucket)
// 	return bucketPolicy, minio.ErrorRespToObjectError(err, bucket)
// }

// // DeleteBucketPolicy deletes all policies on bucket.
// func (l *lfsGateway) DeleteBucketPolicy(ctx context.Context, bucket string) error {
// 	if err := l.Client.SetBucketPolicy(ctx, bucket, ""); err != nil {
// 		return minio.ErrorRespToObjectError(err, bucket, "")
// 	}
// 	return nil
// }

// IsCompressionSupported returns whether compression is applicable for this layer.
func (l *lfsGateway) IsCompressionSupported() bool {
	return false
}
