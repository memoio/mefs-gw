package miniogw

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"sync"

	minio "github.com/memoio/minio/cmd"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/mimedb"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	logging "github.com/memoio/go-mefs-v2/lib/log"
	"github.com/memoio/mefs-gateway/memo"
	"github.com/memoio/mefs-gateway/miniogw/mutipart"
	"github.com/memoio/mefs-gateway/utils"
)

const SlashSeparator = "/"

var logger = logging.Logger("mefs-gateway")

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
	logger.Debugf("root path", rootpath)

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

func (g *Mefs) newS3Client(creds madmin.Credentials, transport http.RoundTripper) (*miniogo.Core, error) {
	// Override default params if the host is provided
	endpoint := viper.GetString("s3.endpoint")
	region := viper.GetString("s3.region")
	accessKey := viper.GetString("s3.accesskey")
	secretKey := viper.GetString("s3.secretkey")
	useSsl := viper.GetBool("s3.use_ssl")

	optionsStaticCreds := &miniogo.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       useSsl,
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

	// 读取设定好的bucketName
	BucketName = viper.GetString("common.bucketname")
	if len(BucketName) == 0 {
		return nil, errors.New("bucketname not set")
	}

	fmt.Println("Use Bucket: ", BucketName)

	if err := s3utils.CheckValidBucketNameStrict(BucketName); err != nil {
		return nil, minio.BucketNameInvalid{Bucket: BucketName, Err: err}
	}

	// 读取设定好的可上传字节数
	MaxUploadableBytes = viper.GetUint64("common.max_upload_bytes")
	if MaxUploadableBytes == 0 {
		return nil, errors.New("upload bytes not set")
	}

	fmt.Println("Maximum uploadable bytes: ", MaxUploadableBytes)

	rootpath, err := utils.BestKnownPath()
	if err != nil {
		return nil, err
	}

	fmt.Println("Gateway root dir: ", rootpath)

	db, err := leveldb.OpenFile(path.Join(rootpath, "db"), &opt.Options{})
	if err != nil {
		return nil, err
	}

	gw := &lfsGateway{
		rootpath: rootpath,
		polices:  make(map[string]*policy.Policy),
		db:       db,
	}

	gw.useLocal = viper.GetBool("common.use_local")
	gw.useS3 = viper.GetBool("common.use_s3")
	gw.useMemo = viper.GetBool("common.use_memo")
	gw.useIpfs = viper.GetBool("common.use_ipfs")
	gw.readOnly = viper.GetBool("common.read_only")

	if !gw.useLocal && !gw.useS3 && !gw.useMemo {
		return nil, errors.New("must choose a backend")
	}

	// 是否使用本地路径
	if gw.useLocal {
		logger.Debug("Use Local backend")
		gw.localfs, err = utils.UserLocalFS(rootpath, BucketName)
		if err != nil {
			return nil, err
		}
	}

	// 是否需要连接上某个s3
	if gw.useS3 {
		logger.Debug("Use S3 backend")
		err := initS3(creds, g, gw)
		if err != nil {
			return nil, err
		}
	}

	if gw.useMemo {
		logger.Debug("use Memo backend")
	}

	if gw.useIpfs {
		host := viper.GetString("ipfs.ipfs_host")
		gw.ipfs = memo.NewIpfsClient(host)
	}

	// 读取已使用的空间大小
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
	rootpath string

	useS3    bool
	useLocal bool
	useMemo  bool
	useIpfs  bool

	readOnly bool

	usedBytes uint64

	mutipart *mutipart.MultipartUploads

	memofs  *memo.MemoFs
	localfs *utils.LocalFS
	Client  *miniogo.Core
	ipfs    *memo.IpFs
	polices map[string]*policy.Policy
	db      *leveldb.DB
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

// SetBucketPolicy will set policy on bucket.
func (l *lfsGateway) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	_, err := l.GetBucketInfo(ctx, bucket)
	if err != nil {
		return err
	}
	l.polices[bucket] = bucketPolicy
	return nil
}

// GetBucketPolicy will get policy on bucket.
func (l *lfsGateway) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {

	if bucket == "favicon.ico" || !viper.GetBool("common.url_allow") {
		return &policy.Policy{}, nil
	}
	if l.useMemo {
		return l.memoGetBucketPolicy(ctx, bucket)
	}

	if l.useS3 {
		return l.s3GetBucketPolicy(ctx, bucket)
	}
	return nil, minio.NotImplemented{}
}

// StorageInfo is not relevant to LFS backend.
func (l *lfsGateway) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	si.Backend.Type = madmin.Gateway
	if l.useS3 {
		si.Backend.GatewayOnline = l.Client.IsOnline()
	}

	si.Disks = make([]madmin.Disk, 1)
	si.Disks[0].DiskIndex = 0
	si.Disks[0].UsedSpace = 100
	si.Disks[0].TotalSpace = MaxUploadableBytes

	return si, nil
}

// MakeBucketWithLocation creates a new container on LFS backend.
func (l *lfsGateway) MakeBucketWithLocation(ctx context.Context, bucket string, options minio.BucketOptions) error {
	if l.readOnly {
		return minio.NotImplemented{}
	}

	if bucket == BucketName && l.useMemo {
		err := l.memoMakeBucketWithLocation(ctx, bucket)
		if err != nil {
			logger.Error("make bucket error", err)
			return err
		}
		return nil
	}

	return minio.NotImplemented{}
}

// GetBucketInfo gets bucket metadata.
func (l *lfsGateway) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	if l.useIpfs {
		bi.Name = "mefstest"
		return bi, nil
	}

	if l.useS3 {
		bi, err := l.s3GetBucketInfo(ctx, bucket)
		if err == nil {
			return bi, nil
		}
	}

	if l.useMemo {
		bi, err := l.memoGetBucketInfo(ctx, bucket)
		if err == nil {
			return bi, nil
		}
	}

	if l.useLocal {
		return minio.BucketInfo{
			Name: bucket,
		}, nil
	}

	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets lists all LFS buckets.
func (l *lfsGateway) ListBuckets(ctx context.Context) (bs []minio.BucketInfo, err error) {
	bs = make([]minio.BucketInfo, 0, 1)
	if l.useMemo {
		bs, err := l.memoListBuckets(ctx)
		if err == nil {
			return bs, nil
		}
	}

	if l.useS3 {
		bs, err := l.s3ListBuckets(ctx)
		if err == nil {
			return bs, nil
		}
	}

	bs = append(bs, minio.BucketInfo{
		Name: BucketName,
	})

	return bs, err
}

// DeleteBucket deletes a bucket on LFS.
func (l *lfsGateway) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	return minio.NotImplemented{}
}

// ListObjects lists all blobs in LFS bucket filtered by prefix.
func (l *lfsGateway) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	if maxKeys == 0 {
		return loi, nil
	}

	if l.useMemo {
		loi, err := l.memoListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
		if err == nil {
			return loi, nil
		}
	}

	if l.useS3 {
		loi, err := l.s3ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
		if err == nil {
			return loi, nil
		}
	}

	return l.localfs.ListObjects(bucket)

}

// ListObjectsV2 lists all blobs in LFS bucket filtered by prefix
func (l *lfsGateway) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {

	if l.useMemo {
		loiv2, err := l.memoListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
		if err == nil {
			return loiv2, nil
		}
	}

	if l.useS3 {
		loiv2, err := l.s3ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
		if err == nil {
			return loiv2, nil
		}
	}

	return l.localfs.ListObjectsV2(bucket)
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *lfsGateway) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}
	if objInfo.UserDefined["content-type"] == "" {
		objInfo.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
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

// InvalidRange - invalid range typed error.
type InvalidRange struct {
	OffsetBegin  int64
	OffsetEnd    int64
	ResourceSize int64
}

func (e InvalidRange) Error() string {
	return fmt.Sprintf("The requested range \"bytes %d -> %d of %d\" is not satisfiable.", e.OffsetBegin, e.OffsetEnd, e.ResourceSize)
}

// GetObject reads an object on LFS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *lfsGateway) GetObject(ctx context.Context, bucketName, objectName string, startOffset, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	// if length < 0 && length != -1 {
	// 	return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucketName, objectName)
	// }

	if l.useLocal {
		object, size, err := l.localfs.GetObject(bucketName, objectName, startOffset)
		if err == nil {
			defer object.Close()
			reader := io.LimitReader(object, length)

			// Check if range is valid
			if startOffset > size || startOffset+length > size {
				err = InvalidRange{startOffset, length, size}
				return err
			}

			if _, err := io.Copy(writer, reader); err != nil {
				return minio.ErrorRespToObjectError(err, bucketName, objectName)
			}

			return nil
		}
	}

	if l.useMemo {
		err := l.memoGetObject(ctx, bucketName, objectName, startOffset, length, writer, etag, o)
		if err == nil {
			return nil
		}
		logger.Errorf("Memo getobject error:", err, "bucket: ", bucketName, "object: ", objectName)
	}

	if l.useS3 {
		err := l.s3GetObject(ctx, bucketName, objectName, startOffset, length, writer, etag, o)
		if err == nil {
			return nil
		}
		logger.Errorf("S3 getobject error:", err, "bucket: ", bucketName, "object: ", objectName)
	}

	if l.useIpfs {
		err := l.ipfs.GetObject(objectName, writer)
		if err != nil {
			logger.Errorf("Ipfs getobject error:", err, "bucket: ", bucketName, "object: ", objectName)
			return err
		}
		return nil
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *lfsGateway) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	log.Println("GetObjectInfo", bucket, object)
	if l.useMemo {
		if l.useIpfs {
			bucket = ""
		}
		objInfo, err := l.memoGetObjectInfo(ctx, bucket, object, opts)
		if err == nil {
			return objInfo, nil
		}
	}

	if l.useS3 {
		objInfo, err := l.s3GetObjectInfo(ctx, bucket, object, opts)
		if err == nil {
			return objInfo, nil
		}
	}
	if l.useLocal {
		return l.localfs.GetObjectInfo(bucket, object)
	}

	return objInfo, nil
}

// spilt reader to qiniu and memo
func readerSpilt(reader io.Reader, reader1, reader2 *bytes.Buffer) error {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("readerspilt err: ", err)
	}
	reader1.Write(b)
	reader2.Write(b)
	return err
}

// PutObject creates a new object with the incoming data.
func (l *lfsGateway) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if l.readOnly {
		return objInfo, minio.PrefixAccessDenied{Bucket: bucket}
	}

	data := r.Reader

	var limitedReader *utils.LimitedReader

	// 限制上传总量
	if data.Size() == -1 {
		limitedReader = utils.LimitReader(data, MaxUploadableBytes-l.usedBytes)
		//大小符合预期
	} else if uint64(data.Size()) < MaxUploadableBytes-l.usedBytes {
		limitedReader = utils.LimitReader(data, uint64(data.Size()))
		// 大小超标
	} else {
		return objInfo, errSpaceOverflow
	}

	var reader io.Reader = limitedReader
	var closer io.Closer

	var oi miniogo.ObjectInfo
	// 如果要存到本地
	if l.useLocal {
		reader, closer, err = l.localfs.PutObject(bucket, object, reader)
		if err != nil {
			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}
	}
	reader1 := new(bytes.Buffer)
	reader2 := new(bytes.Buffer)
	err = readerSpilt(reader, reader1, reader2)
	if err != nil {
		return objInfo, err
	}

	if l.useIpfs && l.useS3 {
		cid, err := l.ipfs.Putobject(reader2)
		if err != nil {
			log.Println("put object to ipfs error:", err, " bucket: ", bucket, " obejct: ", object)
			return objInfo, err
		} else {
			log.Println("put obejct to ipfs Success!", cid)
		}
		opts.UserDefined["name"] = object
		oi, err := l.s3PutObject(ctx, bucket, cid, reader1, data.Size(), opts)
		if err != nil {
			return oi, err
		}
		oi.ETag = cid
		return oi, nil
	}

	if l.useIpfs && l.useMemo {
		logger.Debug("use ipfs and memo")

		oi, err := l.memoPutObject(ctx, bucket, object, reader2, opts)
		if err != nil {
			logger.Error("MEMOPUT: put obejct to memo error", err, " bucket: ", bucket, " obejct: ", object)
		}

		go func(reader io.Reader, bucket, object string) {
			cid, err := l.ipfs.Putobject(reader)
			if err != nil {
				log.Println("put object to ipfs error:", err, " bucket: ", bucket, " obejct: ", object)
			} else {
				log.Println("put obejct to ipfs Success!", cid)
			}
		}(reader1, bucket, object)

		l.addUsedBytes(limitedReader.ReadedBytes())
		return oi, nil
	}

	if l.useS3 && l.useMemo {
		oi, err := l.memoPutObject(ctx, bucket, object, reader2, opts)
		if err != nil {
			logger.Error("MEMOPUT: put obejct to memo error", err, " bucket: ", bucket, " obejct: ", object)
		}

		go func(reader1 *bytes.Buffer, bucket, object string, size int64, opts minio.ObjectOptions) {
			_, err := l.s3PutObject(context.TODO(), bucket, object, reader1, size, opts)
			if err != nil {
				logger.Error("put obejct to s3 error: ", err, "bucket: ", bucket, "obejct: ", object)
			} else {
				log.Println("put object to s3 Success!")
			}
		}(reader1, bucket, object, data.Size(), opts)

		l.addUsedBytes(limitedReader.ReadedBytes())
		return oi, nil
	}

	if l.useMemo {
		oi, err := l.memoPutObject(ctx, bucket, object, reader2, opts)
		if err != nil {
			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}
		l.addUsedBytes(limitedReader.ReadedBytes())
		return oi, nil
	}

	if l.useS3 {
		oi, err := l.s3PutObject(ctx, bucket, object, reader1, data.Size(), opts)
		if err != nil {
			return oi, err
		}
		return oi, nil
	}

	if l.useLocal {
		closer.Close()
		err = l.localfs.FinishPut(bucket, object, oi.Size, true)
		if err != nil {
			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}
	}

	// 记录新增的存储空间
	l.addUsedBytes(limitedReader.ReadedBytes())

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *lfsGateway) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return objInfo, minio.NotImplemented{}
}

// DeleteObject deletes a blob in bucket.
func (l *lfsGateway) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, minio.NotImplemented{}
}

func (l *lfsGateway) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx := range objects {
		errs[idx] = minio.NotImplemented{}

	}

	return dobjects, errs
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (l *lfsGateway) IsCompressionSupported() bool {
	return false
}

func (l *lfsGateway) StatObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	if l.useS3 {
		qBucketname := viper.GetString("common.bucketname")

		oi, err := l.Client.StatObject(ctx, qBucketname, object, miniogo.StatObjectOptions{
			ServerSideEncryption: opts.ServerSideEncryption,
		})

		if err != nil {
			return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
		}

		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}
	return minio.ObjectInfo{}, minio.NotImplemented{}
}
