package miniogw

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	minio "github.com/memoio/minio/cmd"
	"github.com/minio/madmin-go"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/bucket/policy/condition"
	"github.com/spf13/viper"
)

func initS3(creds madmin.Credentials, g *Mefs, gw *lfsGateway) error {
	metrics := minio.NewMetrics()

	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	clnt, err := g.newS3Client(creds, t)
	if err != nil {
		return err
	}

	// 检查是否有指定的bucket
	ctx := context.TODO()
	bs, err := clnt.ListBuckets(ctx)
	if err != nil {
		return err
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
				return err
			}
		}
	}

	// 设置s3 client
	gw.Client = clnt
	return nil
}

func (l *lfsGateway) s3GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	bi, err := l.s3GetBucketInfo(ctx, bucket)
	if err != nil {
		return nil, err
	}
	pb, ok := l.polices[bucket]
	if ok {
		return pb, nil
	}

	pp := &policy.Policy{
		ID:      policy.ID(fmt.Sprintf("Name: %s, Created: %s", bi.Name, bi.Created)),
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				"",
				policy.Allow,

				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetObjectAction,
					//policy.ListBucketAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}

	return pp, nil
}

func (l *lfsGateway) s3GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	buckets, err := l.Client.ListBuckets(ctx)
	if err != nil {
		// Listbuckets may be disallowed, proceed to check if
		// bucket indeed exists, if yes return success.
		var ok bool
		if ok, err = l.Client.BucketExists(ctx, bucket); err != nil {
			logger.Error("s3GetBucketInfo")
			return bi, minio.ErrorRespToObjectError(err, bucket)
		}
		if !ok {
			logger.Error("s3GetBucketInfo")
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
	logger.Error("s3GetBucketInfo")
	return bi, minio.BucketNotFound{Bucket: bucket}
}

func (l *lfsGateway) s3ListBuckets(ctx context.Context) (bs []minio.BucketInfo, err error) {
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

func (l *lfsGateway) s3ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

func (l *lfsGateway) s3ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {
	result, err := l.Client.ListObjectsV2(bucket, prefix, startAfter, continuationToken, delimiter, maxKeys)
	// fmt.Println("result", result.CommonPrefixes)
	if err != nil {
		return loiv2, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketV2Result(bucket, result), nil
}

func (l *lfsGateway) s3GetObject(ctx context.Context, bucketName, objectName string, startOffset, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	opts := miniogo.GetObjectOptions{}
	opts.ServerSideEncryption = o.ServerSideEncryption

	if startOffset >= 0 && length >= 0 {
		if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
			return minio.ErrorRespToObjectError(err, BucketName, objectName)
		}
	}

	if etag != "" {
		opts.SetMatchETag(etag)
	}
	if objectName == "buckets/.usage.json" {
		return nil
	}
	object, _, _, err := l.Client.GetObject(ctx, BucketName, objectName, opts)
	if err != nil {
		logger.Errorf("get S3 error: ", err)
		return minio.ErrorRespToObjectError(err, BucketName, objectName)
	}
	defer object.Close()
	if _, err := io.Copy(writer, object); err != nil {
		return minio.ErrorRespToObjectError(err, BucketName, objectName)
	}

	return nil
}

func (l *lfsGateway) s3GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	oi, err := l.Client.StatObject(ctx, bucket, object, miniogo.StatObjectOptions{
		ServerSideEncryption: opts.ServerSideEncryption,
	})

	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	}
	// log.Println("objectinfo ", minio.FromMinioClientObjectInfo(bucket, oi))
	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

func (l *lfsGateway) s3PutObject(ctx context.Context, bucket, object string, reader *bytes.Buffer, size int64, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {

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
	hashReader, err := minio.NewhashReader(reader, size, "", "", size)
	if err != nil {
		return objInfo, err
	}
	rawReader := hashReader
	pReader := minio.NewPutObjReader(rawReader)

	ui, err := l.Client.PutObject(ctx, bucket, object, pReader, size, "", "", putOpts)
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
	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}
