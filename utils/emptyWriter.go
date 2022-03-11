package utils

import "io"

var _ io.Writer = (*EmptyWriter)(nil)

type EmptyWriter struct {
	size int64
}

func (ew *EmptyWriter) Write(p []byte) (int, error) {
	ew.size += int64(len(p))
	return len(p), nil
}

func (ew *EmptyWriter) Size() int64 {
	return ew.size
}
