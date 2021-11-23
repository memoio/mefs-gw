package miniogw

import (
	"io"
)

var _ io.Reader = (*limitedReader)(nil)

type limitedReader struct {
	reader   io.Reader
	maxBytes uint64
	readed   uint64
}

func newLimitedReader(r io.Reader, maxBytes uint64) *limitedReader {
	return &limitedReader{
		reader:   r,
		maxBytes: maxBytes,
		readed:   0,
	}
}

func (r *limitedReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if uint64(n)+r.readed > r.maxBytes {
		return n, errSpaceOverflow
	}

	return n, err
}
