package memo

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"

	mclient "github.com/memoio/go-mefs-v2/api/client"
	"github.com/memoio/go-mefs-v2/build"
	mcode "github.com/memoio/go-mefs-v2/lib/code"
	"github.com/memoio/go-mefs-v2/lib/types"
	mtypes "github.com/memoio/go-mefs-v2/lib/types"
	"github.com/memoio/go-mefs-v2/lib/utils/etag"
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

func (m *MemoFs) GetObject(ctx context.Context, bucketName, objectName string, writer io.Writer) error {
	napi, closer, err := mclient.NewUserNode(ctx, m.addr, m.headers)
	if err != nil {
		return err
	}
	defer closer()

	objInfo, err := napi.HeadObject(ctx, bucketName, objectName)
	if err != nil {
		return err
	}

	buInfo, err := napi.HeadBucket(ctx, bucketName)
	if err != nil {
		return err
	}

	h := md5.New()
	if len(objInfo.ETag) != md5.Size {
		h = sha256.New()
	}

	stripeCnt := 4 * 64 / buInfo.DataCount
	stepLen := int64(build.DefaultSegSize * stripeCnt * buInfo.DataCount)
	start := int64(0)
	oSize := int64(objInfo.Size)

	for start < oSize {
		readLen := stepLen
		if oSize-start < stepLen {
			readLen = oSize - start
		}

		doo := &types.DownloadObjectOptions{
			Start:  start,
			Length: readLen,
		}

		data, err := napi.GetObject(ctx, bucketName, objectName, doo)
		if err != nil {
			return err
		}

		h.Write(data)
		writer.Write(data)

		start += readLen
	}

	var etagb []byte
	if len(objInfo.ETag) == md5.Size {
		etagb = h.Sum(nil)
	} else {
		mhtag, err := mh.Encode(h.Sum(nil), mh.SHA2_256)
		if err != nil {
			return err
		}

		cidEtag := cid.NewCidV1(cid.Raw, mhtag)
		etagb = cidEtag.Bytes()
	}

	gotEtag, err := etag.ToString(etagb)
	if err != nil {
		return err
	}

	origEtag, err := etag.ToString(objInfo.ETag)
	if err != nil {
		return err
	}

	if !bytes.Equal(etagb, objInfo.ETag) {
		return xerrors.Errorf("object content wrong, expect %s got %s", origEtag, gotEtag)
	}

	return nil
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

	poo := mtypes.CidUploadOption()
	poo.UserDefined = UserDefined
	moi, err := napi.PutObject(ctx, bucket, object, r, poo)
	if err != nil {
		return objInfo, err
	}
	return moi, nil
}

// func (m *MemoFs) DeleteObject()
