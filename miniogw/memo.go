package miniogw

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	metag "github.com/memoio/go-mefs-v2/lib/utils/etag"
	"github.com/memoio/mefs-gateway/memo"
	minio "github.com/memoio/minio/cmd"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/bucket/policy/condition"
	"github.com/spf13/viper"
)

func initMemo() error {
	repoDir := viper.GetString("memo.repo_dir")
	err := os.Setenv("MEFS_PATH", repoDir)
	if err != nil {
		return err
	}
	return nil
}

func (l *lfsGateway) checkMemofs() bool {
	if l.memofs == nil {
		logger.Debug("Get Memo Info")
		var err error
		l.memofs, err = memo.NewMemofs()
		if err != nil {
			logger.Error("Get memo error: ", err)
			return false
		}
		return true
	}
	return true
}

func (l *lfsGateway) memoGetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	if !l.checkMemofs() {
		return nil, minio.NotImplemented{}
	}
	bi, err := l.memofs.GetBucketInfo(ctx, bucket)
	if err != nil {
		return nil, err
	}

	pb, ok := l.polices[bucket]
	if ok {
		return pb, nil
	}

	pp := &policy.Policy{
		ID:      policy.ID(fmt.Sprintf("data: %d, parity: %d", bi.DataCount, bi.ParityCount)),
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

func (l *lfsGateway) memoMakeBucketWithLocation(ctx context.Context, bucket string) error {
	if !l.checkMemofs() {
		return minio.NotImplemented{}
	}
	err := l.memofs.MakeBucketWithLocation(ctx, bucket)
	return err
}

func (l *lfsGateway) memoGetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	if !l.checkMemofs() {
		logger.Error("memoGetBucketInfo")
		return bi, minio.NotImplemented{}
	}
	bucketInfo, err := l.memofs.GetBucketInfo(ctx, bucket)
	if err != nil {
		logger.Error("memoGetBucketInfo")
		return bi, err
	}
	bi.Name = bucket
	bi.Created = time.Unix(bucketInfo.GetCTime(), 0).UTC()
	return bi, nil
}

func (l *lfsGateway) memoListBuckets(ctx context.Context) (bs []minio.BucketInfo, err error) {
	if !l.checkMemofs() {
		return nil, minio.NotImplemented{}
	}
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

func (l *lfsGateway) memoListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	if !l.checkMemofs() {
		return loi, minio.NotImplemented{}
	}
	mloi, err := l.memofs.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}
	ud := make(map[string]string)
	ud["x-amz-meta-mode"] = "33204"
	for _, oi := range mloi.Objects {
		etag, _ := metag.ToString(oi.ETag)
		ud["x-amz-meta-mtime"] = strconv.FormatInt(oi.GetTime(), 10)
		loi.Objects = append(loi.Objects, minio.ObjectInfo{
			Bucket:      bucket,
			Name:        oi.GetName(),
			ModTime:     time.Unix(oi.GetTime(), 0).UTC(),
			Size:        int64(oi.Size),
			ETag:        etag,
			UserDefined: ud,
		})
	}

	loi.IsTruncated = mloi.IsTruncated
	loi.NextMarker = mloi.NextMarker
	loi.Prefixes = mloi.Prefixes

	return loi, nil
}

func (l *lfsGateway) memoListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loiv2 minio.ListObjectsV2Info, err error) {
	if !l.checkMemofs() {
		return loiv2, minio.NotImplemented{}
	}
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

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

func (l *lfsGateway) memoGetObject(ctx context.Context, bucketName, objectName string, startOffset, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	if !l.checkMemofs() {
		if objectName == "buckets/.usage.json" {
			return nil
		}
		return minio.NotImplemented{}
	}
	err := l.memofs.GetObject(ctx, bucketName, objectName, startOffset, length, writer, l.useIpfs)
	if objectName == "buckets/.usage.json" {
		return nil
	}
	return err
}

func (l *lfsGateway) memoGetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if !l.checkMemofs() {
		return objInfo, minio.NotImplemented{}
	}
	moi, err := l.memofs.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		return objInfo, err
	}
	// filter metadata
	userdefined := moi.UserDefined
	for k := range userdefined {
		if strings.Contains(strings.ToLower(k), "metis") {
			continue
		}
		delete(moi.UserDefined, k)
	}

	// need handle ETag
	etag, _ := metag.ToString(moi.ETag)
	oi := miniogo.ObjectInfo{
		Key:      moi.Name,
		ETag:     etag,
		Size:     int64(moi.Size),
		Metadata: minio.ToMinioClientObjectInfoMetadata(moi.UserDefined),
	}
	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

func (l *lfsGateway) memoPutObject(ctx context.Context, bucket, object string, reader io.Reader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if !l.checkMemofs() {
		return objInfo, minio.NotImplemented{}
	}
	moi, err := l.memofs.PutObject(ctx, bucket, object, reader, opts.UserDefined)
	if err != nil {
		return objInfo, err
	}
	etag, _ := metag.ToString(moi.ETag)
	oi := miniogo.ObjectInfo{
		ETag:     etag,
		Size:     int64(moi.Size),
		Key:      object,
		Metadata: minio.ToMinioClientObjectInfoMetadata(opts.UserDefined),
	}
	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}
