package miniogw

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	minio "github.com/memoio/minio/cmd"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	metag "github.com/memoio/go-mefs-v2/lib/utils/etag"
	"github.com/memoio/mefs-gateway/memo"
	"github.com/memoio/mefs-gateway/utils"
)

const SlashSeparator = "/"

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

func (g *Mefs) newS3Client(creds madmin.Credentials, transport http.RoundTripper) (*miniogo.Core, error) {
	// Override default params if the host is provided
	endpoint := viper.GetString("s3.endpoint")
	region := viper.GetString("s3.region")
	accessKey := viper.GetString("s3.accesskey")
	secretKey := viper.GetString("s3.secretkey")

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

	fmt.Println("Maximum uploadable bytes: ", BucketName)

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
		db:       db,
	}

	gw.useLocal = viper.GetBool("common.use_local")
	gw.useS3 = viper.GetBool("common.use_s3")
	gw.useMemo = viper.GetBool("common.use_memo")
	gw.readOnly = viper.GetBool("common.read_only")
	gw.repoDir = viper.GetString("common.repo_dir")

	if !gw.useLocal && !gw.useS3 && !gw.useMemo {
		return nil, errors.New("must choose a backend")
	}

	// 是否使用本地路径
	if gw.useLocal {
		localDir := viper.GetString("common.local_dir")
		if localDir == "" {
			localDir = path.Join(rootpath, "local")
		}
		fmt.Println("Use local fs: ", localDir)
		localfs, err := utils.OpenLocalFS(localDir)
		if err != nil {
			return nil, err
		}

		err = localfs.CheckBucketExist(BucketName)
		if err != nil {
			err = localfs.MakeBucket(BucketName)
			if err != nil {
				return nil, err
			}

		}
		gw.localfs = localfs
	}

	// 是否需要连接上某个s3
	if gw.useS3 {
		fmt.Println("Use s3 backend")
		clnt, err := g.newS3Client(creds, t)
		if err != nil {
			return nil, err
		}

		// 检查是否有指定的bucket
		ctx := context.TODO()
		bs, err := clnt.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}

		// 扫描所有的Bucket进行判断
		for i := 0; i < len(bs); i++ {
			if bs[i].Name == BucketName {
				break
			}

			// 如果没有就创建这个bucket
			if i >= len(bs)-1 {
				region := viper.GetString("s3.region")
				err = clnt.MakeBucket(ctx,
					BucketName,
					miniogo.MakeBucketOptions{Region: region})
				if err != nil {
					return nil, err
				}
			}
		}

		// 设置s3 client
		gw.Client = clnt
	}

	if gw.useMemo {
		var repoDir string
		if gw.repoDir == "" {
			repoDir = gw.rootpath
		} else {
			repoDir = gw.repoDir
		}
		gw.memofs, err = memo.NewMemofs(repoDir)
		if err != nil {
			return nil, err
		}
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

	readOnly bool
	repoDir  string

	usedBytes uint64

	memofs  *memo.MemoFs
	localfs *utils.LocalFS
	Client  *miniogo.Core
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

	if l.useMemo {
		l.memofs.MakeBucketWithLocation(ctx, bucket)
		return nil
	}

	return minio.NotImplemented{}
}

// GetBucketInfo gets bucket metadata.
func (l *lfsGateway) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {

	if l.useMemo {
		bucketInfo, err := l.memofs.GetBucketInfo(ctx, bucket)
		if err != nil {
			return bi, err
		}
		bi.Name = bucket
		bi.Created = time.Unix(bucketInfo.GetCTime(), 0).UTC()
		return bi, nil
	}

	if l.useS3 {
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
		buckets, err := l.memofs.ListBuckets(ctx)
		if err != nil {
			return bs, err
		}
		for _, v := range buckets {
			bs = append(bs, minio.BucketInfo{
				Name:    v.Name,
				Created: time.Unix(v.GetCTime(), 0).UTC(),
			})
		}

		return bs, nil
	}

	if l.useS3 {
		buckets, err := l.Client.ListBuckets(ctx)
		if err != nil {
			return nil, minio.ErrorRespToObjectError(err)
		}

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
	// fmt.Println("bucket: ", bucket, " prefix: ", prefix, " marker: ", marker, " delimiter: ", delimiter, " maxKeys: ", maxKeys)
	// recursive := true
	// if delimiter == SlashSeparator {
	// 	recursive = false
	// }

	if l.useMemo {

		mloi, err := l.memofs.ListObjects(ctx, bucket)
		if err != nil {
			return loi, err
		}
		ud := make(map[string]string)
		ud["x-amz-meta-mode"] = "33204"
		// loi.Prefixes = []string{"work"}
		for _, oi := range mloi {
			etag, _ := metag.ToString(oi.ETag)
			fmt.Println(oi.GetName())
			ud["x-amz-meta-mtime"] = strconv.FormatInt(oi.GetTime(), 10)

			// if strings.Contains(objname, SlashSeparator) {
			// 	// obj := strings.Split(objname, "/")
			// 	// for _, o := range obj {

			// 	// }
			// }
			loi.Objects = append(loi.Objects, minio.ObjectInfo{
				Bucket:      bucket,
				Name:        oi.GetName(),
				ModTime:     time.Unix(oi.GetTime(), 0).UTC(),
				Size:        int64(oi.Size),
				IsDir:       false,
				ETag:        etag,
				UserDefined: ud,
			})
		}

		return loi, nil
	}

	if l.useS3 {
		result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
		if err != nil {
			return loi, minio.ErrorRespToObjectError(err, bucket)
		}

		return minio.FromMinioClientListBucketResult(bucket, result), nil
	}
	return l.localfs.ListObjects(bucket)

}

// ListObjectsV2 lists all blobs in LFS bucket filtered by prefix
func (l *lfsGateway) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {
	// if bucket != BucketName {
	// 	return loiv2, minio.BucketNotFound{Bucket: bucket}
	// }
	// log.Println("ListObjectsV2 ", bucket)
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	if l.useMemo {
		loi, err := l.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
		if err != nil {
			return loiv2, err
		}

		loiv2 = minio.ListObjectsV2Info{
			IsTruncated:           loi.IsTruncated,
			ContinuationToken:     continuationToken,
			NextContinuationToken: loi.NextMarker,
			Objects:               loi.Objects,
			Prefixes:              loi.Prefixes,
		}

		return loiv2, err
	}

	if l.useS3 {
		result, err := l.Client.ListObjectsV2(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys)
		// fmt.Println("result", result.CommonPrefixes)
		if err != nil {
			return loiv2, minio.ErrorRespToObjectError(err, bucket)
		}

		return minio.FromMinioClientListBucketV2Result(bucket, result), nil
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
	if l.readOnly {
		return minio.NotImplemented{}
	}

	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucketName, objectName)
	}

	qBucketname := viper.GetString("common.bucketname")

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

		if !l.useS3 {
			return minio.ObjectNotFound{Bucket: bucketName, Object: objectName}
		}
	}

	if l.useS3 && l.useMemo {
		err := l.memofs.GetObject(ctx, bucketName, objectName, writer)
		if err != nil {
			opts := miniogo.GetObjectOptions{}
			opts.ServerSideEncryption = o.ServerSideEncryption

			if startOffset >= 0 && length >= 0 {
				if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
					return minio.ErrorRespToObjectError(err, bucketName, objectName)
				}
			}

			if etag != "" {
				opts.SetMatchETag(etag)
			}

			object, _, _, err := l.Client.GetObject(ctx, qBucketname, objectName, opts)
			if err != nil {
				return minio.ErrorRespToObjectError(err, bucketName, objectName)
			}
			defer object.Close()
			if _, err := io.Copy(writer, object); err != nil {
				return minio.ErrorRespToObjectError(err, bucketName, objectName)
			}

			return nil
		}
		return nil
	}
	if l.useS3 {
		opts := miniogo.GetObjectOptions{}
		opts.ServerSideEncryption = o.ServerSideEncryption

		if startOffset >= 0 && length >= 0 {
			if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
				return minio.ErrorRespToObjectError(err, bucketName, objectName)
			}
		}

		if etag != "" {
			opts.SetMatchETag(etag)
		}

		object, _, _, err := l.Client.GetObject(ctx, bucketName, objectName, opts)
		if err != nil {
			return minio.ErrorRespToObjectError(err, bucketName, objectName)
		}
		defer object.Close()
		if _, err := io.Copy(writer, object); err != nil {
			return minio.ErrorRespToObjectError(err, bucketName, objectName)
		}

		return nil
	}

	if l.useMemo {
		err := l.memofs.GetObject(ctx, bucketName, objectName, writer)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *lfsGateway) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	// if bucket != BucketName {
	// 	return objInfo, minio.BucketNotFound{Bucket: bucket}
	// }
	// log.Println("GetObjectInfo ", object)

	if l.useMemo {
		moi, err := l.memofs.GetObjectInfo(ctx, bucket, object)
		if err != nil {
			return objInfo, err
		}
		ud := make(map[string]string)
		ud["x-amz-meta-mode"] = "33204"
		ud["x-amz-meta-mtime"] = strconv.FormatInt(moi.GetTime(), 10)
		// need handle ETag
		etag, _ := metag.ToString(moi.ETag)
		oi := miniogo.ObjectInfo{
			Key:  moi.Name,
			ETag: etag,
			Size: int64(moi.Size),
		}
		// log.Println("ETag ", hex.EncodeToString(moi.Etag))
		// log.Println("objectinfo ", minio.FromMinioClientObjectInfo(bucket, oi))
		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}

	if l.useS3 {
		oi, err := l.Client.StatObject(ctx, bucket, object, miniogo.StatObjectOptions{
			ServerSideEncryption: opts.ServerSideEncryption,
		})

		if err != nil {
			return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
		}
		// log.Println("objectinfo ", minio.FromMinioClientObjectInfo(bucket, oi))
		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}

	return l.localfs.GetObjectInfo(bucket, object)
}

// PutObject creates a new object with the incoming data.
func (l *lfsGateway) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	// if bucket != BucketName {
	// 	return objInfo, minio.BucketNotFound{Bucket: bucket}
	// }
	// log.Println("PutObject ", object)
	if l.readOnly {
		return objInfo, minio.PrefixAccessDenied{Bucket: bucket}
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

	if l.useS3 && l.useMemo {
		qBucketname := viper.GetString("common.bucketname")
		moi, err := l.memofs.PutObject(ctx, bucket, object, reader, opts.UserDefined)
		if err != nil {
			return objInfo, err
		}
		etag, _ := metag.ToString(moi.ETag)
		oi = miniogo.ObjectInfo{
			ETag:     etag,
			Size:     int64(moi.Size),
			Key:      object,
			Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
		}
		go l.Client.PutObject(ctx, qBucketname, object, reader, data.Size(), data.MD5Base64String(), data.SHA256HexString(), putOpts)
		l.addUsedBytes(limitedReader.ReadedBytes())
		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}

	if l.useMemo {
		moi, err := l.memofs.PutObject(ctx, bucket, object, reader, opts.UserDefined)
		if err != nil {
			return objInfo, err
		}
		etag, _ := metag.ToString(moi.ETag)
		oi = miniogo.ObjectInfo{
			ETag:     etag,
			Size:     int64(moi.Size),
			Key:      object,
			Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
		}
		l.addUsedBytes(limitedReader.ReadedBytes())
		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}

	if l.useS3 {
		ui, err := l.Client.PutObject(ctx, bucket, object, reader, data.Size(), data.MD5Base64String(), data.SHA256HexString(), putOpts)
		if err != nil {
			if l.useLocal {
				closer.Close()
				err = l.localfs.FinishPut(bucket, object, 0, false)
				if err != nil {
					return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
				}
			}
			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}

		// On success, populate the key & metadata so they are present in the notification
		oi = miniogo.ObjectInfo{
			ETag:     ui.ETag,
			Size:     ui.Size,
			Key:      object,
			Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
		}
	} else {
		w := &utils.EmptyWriter{}
		_, err := io.Copy(w, reader)
		if err != nil {
			if l.useLocal {
				closer.Close()
				err = l.localfs.FinishPut(bucket, object, 0, false)
				if err != nil {
					return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
				}
			}

			return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
		}

		// On success, populate the key & metadata so they are present in the notification
		oi = miniogo.ObjectInfo{
			Size:     w.Size(),
			Key:      object,
			Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
		}
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

func (l *lfsGateway) StatObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {

	if l.useS3 {
		if bucket != BucketName {
			return minio.ObjectInfo{}, minio.BucketNotFound{Bucket: bucket}
		}

		oi, err := l.Client.StatObject(ctx, bucket, object, miniogo.StatObjectOptions{
			ServerSideEncryption: opts.ServerSideEncryption,
		})

		if err != nil {
			return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
		}

		return minio.FromMinioClientObjectInfo(bucket, oi), nil
	}
	return minio.ObjectInfo{}, minio.NotImplemented{}
}
