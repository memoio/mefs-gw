package memo

import (
	"context"
	"errors"
	"io"
	"net/http"

	mclient "github.com/memoio/go-mefs-v2/api/client"
	mcode "github.com/memoio/go-mefs-v2/lib/code"
	mtypes "github.com/memoio/go-mefs-v2/lib/types"
)

type MemoFs struct {
	addr    string
	headers http.Header
}

func NewMemofs(repoDir string) (*MemoFs, error) {
	addr, headers, err := mclient.GetMemoClientInfo(repoDir)
	if err != nil {
		return nil, err
	}
	return &MemoFs{
		addr:    addr,
		headers: headers,
	}, nil
}

func (m *MemoFs) MakeBucketWithLocation(ctx context.Context, bucket string) error {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return err
	}
	defer closer()
	if bucket == "" {
		return errors.New("bucketname is nil")
	}
	opts := mcode.DefaultBucketOptions()

	_, err = napi.CreateBucket(ctx, bucket, opts)
	if err != nil {
		return err
	}
	return nil
}

func (m *MemoFs) GetBucketInfo(ctx context.Context, bucket string) (bi *mtypes.BucketInfo, err error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return bi, err
	}
	defer closer()

	bi, err = napi.HeadBucket(ctx, bucket)
	if err != nil {
		return bi, err
	}
	return bi, nil
}

func (m *MemoFs) ListBuckets(ctx context.Context) ([]*mtypes.BucketInfo, error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return nil, err
	}
	defer closer()

	buckets, err := napi.ListBuckets(ctx, "")
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (m *MemoFs) ListObjects(ctx context.Context, bucket string) (mloi []*mtypes.ObjectInfo, err error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return mloi, err
	}
	defer closer()
	ops := mtypes.DefaultListOption()
	mloi, err = napi.ListObjects(ctx, bucket, ops)
	if err != nil {
		return mloi, err
	}
	return mloi, nil
}

func (m *MemoFs) GetObject(ctx context.Context, bucketName, objectName string) ([]byte, error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return nil, err
	}
	defer closer()

	doo := mtypes.DefaultDownloadOption()
	data, err := napi.GetObject(ctx, bucketName, objectName, doo)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *MemoFs) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo *mtypes.ObjectInfo, err error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return objInfo, err
	}
	defer closer()

	oi, err := napi.HeadObject(ctx, bucket, object)
	if err != nil {
		return objInfo, err
	}
	return oi, nil
}

func (m *MemoFs) PutObject(ctx context.Context, bucket, object string, r io.Reader, UserDefined map[string]string) (objInfo *mtypes.ObjectInfo, err error) {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return objInfo, err
	}
	defer closer()

	poo := mtypes.DefaultUploadOption()
	poo.UserDefined = UserDefined
	moi, err := napi.PutObject(ctx, bucket, object, r, poo)
	if err != nil {
		return objInfo, err
	}
	return moi, nil
}


// func (m *MemoFs) DeleteObject()