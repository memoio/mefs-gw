package utils

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/mitchellh/go-homedir"
)

func TestSimplefs(t *testing.T) {
	root := "~/gw_test"
	root, err := homedir.Expand(root)
	if err != nil {
		t.Fatal(nil)
	}
	sfs, err := OpenLocalFS(root)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := "testbucket"
	objectName := "data"
	sfs.MakeBucket(bucketName)

	data := make([]byte, 20*1024*1024)
	fillRandom(data)

	buf := bytes.NewBuffer(data)
	r, c, err := sfs.PutObject(bucketName, objectName, buf)
	if err != nil {
		t.Fatal(err)
	}

	ew := &EmptyWriter{}
	io.Copy(ew, r)
	c.Close()

	sfs.FinishPut(bucketName, objectName)

	cid, err := sfs.CidOf(bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}

	t.Error(cid)
}

func fillRandom(p []byte) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < len(p); i += 7 {
		val := rand.Int63()
		for j := 0; i+j < len(p) && j < 7; j++ {
			p[i+j] = byte(val)
			val >>= 8
		}
	}
}
