package memo

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	mtypes "github.com/memoio/go-mefs-v2/lib/types"
)

var (
	meeda_bucket = "da-bucket"
)

type MeedaFs struct {
	client *http.Client
	url    string
	mefs   *MemoFs
}

func NewMeedaFs(repo, url string) (*MeedaFs, error) {
	mefs, err := NewMemofs(repo)
	if err != nil {
		return nil, err
	}

	return &MeedaFs{
		url:    url,
		client: &http.Client{},
		mefs:   mefs,
	}, nil
}

func (m *MeedaFs) ListObjects(ctx context.Context, prefix, marker, delimiter string, maxKeys int) (mloi mtypes.ListObjectsInfo, err error) {
	return m.mefs.ListObjects(ctx, meeda_bucket, prefix, marker, delimiter, maxKeys)
}

func (m *MeedaFs) Putobject(data []byte) (string, error) {
	// Upload files via http request
	url := m.url + "/putObject"

	datas := hex.EncodeToString(data)
	requestData := map[string]string{
		"data": datas,
	}

	jsonData, err := json.Marshal(requestData)
	if err != nil {
		return "", err
	}

	// Set headers
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (m *MeedaFs) GetObject(ctx context.Context, objectName string, startOffset int64, length int64, writer io.Writer) error {
	return m.mefs.GetObject(ctx, meeda_bucket, objectName, startOffset, length, writer)
}

func (m *MeedaFs) GetObjectInfo(ctx context.Context, object string) (objInfo mtypes.ObjectInfo, err error) {
	return m.mefs.GetObjectInfo(ctx, meeda_bucket, object)
}

func (m *MeedaFs) GetBucketInfo(ctx context.Context) (mtypes.BucketInfo, error) {
	return m.mefs.GetBucketInfo(ctx, meeda_bucket)
}
