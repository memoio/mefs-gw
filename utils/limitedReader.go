package utils

import (
	"io"
)

var _ io.Reader = (*LimitedReader)(nil)

type LimitedReader struct {
	reader   io.Reader
	maxBytes uint64
	readed   uint64
}

func LimitReader(r io.Reader, maxBytes uint64) *LimitedReader {
	return &LimitedReader{
		reader:   r,
		maxBytes: maxBytes,
		readed:   0,
	}
}

func (r *LimitedReader) Read(p []byte) (n int, err error) {
	if r.readed > r.maxBytes {
		return 0, io.ErrUnexpectedEOF
	}

	if uint64(len(p))+r.readed > r.maxBytes {
		p = p[:r.maxBytes-r.readed]
	}

	n, err = r.reader.Read(p)
	r.readed += uint64(n)

	return n, err
}

func (r *LimitedReader) MaxBytes() (n uint64) {
	return r.maxBytes
}

func (r *LimitedReader) ReadedBytes() (n uint64) {
	return r.readed
}
