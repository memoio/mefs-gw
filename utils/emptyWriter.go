package utils

import "io"

var _ io.Writer = (*EmptyWriter)(nil)

type EmptyWriter struct {
}

func (ew *EmptyWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
