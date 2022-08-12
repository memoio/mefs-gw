package mutipart

import (
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"golang.org/x/xerrors"
)

var (
	errPartAlreadyUploaded = xerrors.New("part already uploaded")
)

type MultipartStream struct {
	mu          sync.Mutex
	moreParts   sync.Cond
	err         error
	closed      bool
	finished    bool
	nextID      int
	nextNumber  int
	currentPart *StreamPart
	parts       []*StreamPart
}

func (stream *MultipartStream) Abort(err error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.finished {
		return
	}

	if stream.err == nil {
		stream.err = err
	}
	stream.finished = true
	stream.closed = true

	for _, part := range stream.parts {
		part.Done <- err
		close(part.Done)
	}
	stream.parts = nil

	stream.moreParts.Broadcast()
}

// NewMultipartStream creates a new MultipartStream
func NewMultipartStream() *MultipartStream {
	stream := &MultipartStream{}
	stream.moreParts.L = &stream.mu
	stream.nextID = 1
	return stream
}

// Close closes the stream, but lets it complete
func (stream *MultipartStream) Close() {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	stream.closed = true
	stream.moreParts.Broadcast()
}

// Read implements io.Reader interface, blocking when there's no part
func (stream *MultipartStream) Read(data []byte) (n int, err error) {
	stream.mu.Lock()
	for {
		// has an error occurred?
		if stream.err != nil {
			stream.mu.Unlock()
			return 0, stream.err
		}
		// still uploading the current part?
		if stream.currentPart != nil {
			break
		}
		// do we have the next part?
		if len(stream.parts) > 0 && stream.nextID == stream.parts[0].ID {
			stream.currentPart = stream.parts[0]
			stream.parts = stream.parts[1:]
			stream.nextID++
			break
		}
		// we don't have the next part and are closed, hence we are complete
		if stream.closed {
			stream.finished = true
			stream.mu.Unlock()
			return 0, io.EOF
		}

		stream.moreParts.Wait()
	}
	stream.mu.Unlock()

	// read as much as we can
	n, err = stream.currentPart.Reader.Read(data)
	atomic.AddInt64(&stream.currentPart.Size, int64(n))
	if err == io.EOF {
		// the part completed, hence advance to the next one
		err = nil
		close(stream.currentPart.Done)
		stream.currentPart = nil
	} else if err != nil {
		// something bad happened, abort the whole thing
		stream.Abort(err)
		return n, err
	}

	return n, err
}

// AddPart adds a new part to the stream to wait
func (stream *MultipartStream) AddPart(partID int, data io.Reader) (*StreamPart, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if partID < stream.nextID {
		return nil, errPartAlreadyUploaded
	}

	for _, p := range stream.parts {
		if p.ID == partID {
			// Replace the reader of this part with the new one.
			// This could happen if the read timeout for this part has expired
			// and the client tries to upload the part again.
			p.Reader = data
			return p, nil
		}
	}

	stream.nextNumber++
	part := &StreamPart{
		Number: stream.nextNumber - 1,
		ID:     partID,
		Size:   0,
		Reader: data,
		Done:   make(chan error, 1),
	}

	stream.parts = append(stream.parts, part)
	sort.Slice(stream.parts, func(i, k int) bool {
		return stream.parts[i].ID < stream.parts[k].ID
	})

	stream.moreParts.Broadcast()

	return part, nil
}

type StreamPart struct {
	Number int
	ID     int
	Size   int64
	Reader io.Reader
	Done   chan error
}
