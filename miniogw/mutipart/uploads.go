package mutipart

import (
	"context"
	"errors"
	"strconv"
	"sync"

	minio "github.com/memoio/minio/cmd"
	// "github.com/memoio/minio/pkg/hash"
)

var (
	errAbortByAnother = errors.New("MultipartUpload abort by another")
	errUploadMissing  = errors.New("MultipartUpload missing")
	errMismatch       = errors.New("MultipartUpload mismatch")
)

type MultipartUploads struct {
	mu      sync.RWMutex
	LastID  int
	Pending map[string]*MultipartUpload
}

func NewMultipartUploads() *MultipartUploads {
	return &MultipartUploads{
		Pending: make(map[string]*MultipartUpload),
	}
}

func (m *MultipartUploads) Create(bucket, object string, cancel context.CancelFunc) (*MultipartUpload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, upload := range m.Pending {
		if upload.Bucket == bucket && upload.Object == object {
			upload.Stream.Abort(errAbortByAnother)
			delete(m.Pending, id)
		}
	}

	m.LastID++
	uploadID := "Upload" + strconv.Itoa(m.LastID)
	upload := NewMultipartUpload(uploadID, bucket, object, cancel)
	m.Pending[uploadID] = upload
	return upload, nil
}

func (m *MultipartUploads) Get(bucket, object, uploadID string) (*MultipartUpload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	upload, ok := m.Pending[uploadID]
	if !ok {
		return nil, errUploadMissing
	}
	if upload.Bucket != bucket || upload.Object != object {
		return nil, errMismatch
	}

	return upload, nil
}

// Remove returns and removes a pending upload
func (uploads *MultipartUploads) Remove(bucket, object, uploadID string) (*MultipartUpload, error) {
	uploads.mu.RLock()
	defer uploads.mu.RUnlock()

	upload, ok := uploads.Pending[uploadID]
	if !ok {
		return nil, nil
	}
	if upload.Bucket != bucket || upload.Object != object {
		return nil, errMismatch
	}

	delete(uploads.Pending, uploadID)

	return upload, nil
}

// RemoveByID removes pending upload by id
func (m *MultipartUploads) RemoveByID(uploadID string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	delete(m.Pending, uploadID)
}

// MultipartUpload is partial info about a pending upload
type MultipartUpload struct {
	ID     string
	Bucket string
	Object string
	Cancel context.CancelFunc

	Done   chan (*MultipartUploadResult)
	Stream *MultipartStream

	mu        sync.Mutex
	completed []minio.PartInfo
}

func NewMultipartUpload(uploadID string, bucket, object string, cancel context.CancelFunc) *MultipartUpload {
	upload := &MultipartUpload{
		ID:     uploadID,
		Bucket: bucket,
		Object: object,
		Cancel: cancel,
		Done:   make(chan *MultipartUploadResult, 1),
		Stream: NewMultipartStream(),
	}
	return upload
}

// addCompletedPart adds a completed part to the list
func (upload *MultipartUpload) AddCompletedPart(part minio.PartInfo) {
	upload.mu.Lock()
	defer upload.mu.Unlock()

	upload.completed = append(upload.completed, part)
}

func (upload *MultipartUpload) GetCompletedParts() []minio.PartInfo {
	upload.mu.Lock()
	defer upload.mu.Unlock()

	return append([]minio.PartInfo{}, upload.completed...)
}

// fail aborts the upload with an error
func (upload *MultipartUpload) Fail(err error) {
	upload.Done <- &MultipartUploadResult{Error: err}
	close(upload.Done)
}

// complete completes the upload
func (upload *MultipartUpload) Complete(info minio.ObjectInfo) {
	upload.Done <- &MultipartUploadResult{Info: info}
	close(upload.Done)
}

// MultipartUploadResult contains either an Error or the uploaded ObjectInfo
type MultipartUploadResult struct {
	Error error
	Info  minio.ObjectInfo
}
