package utils

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
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

	sfs.FinishPut(bucketName, objectName, int64(len(data)), true)

	cid, err := sfs.CidOf(bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}

	t.Error(cid)
}

func TestSimplefsMultiFile(t *testing.T) {
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
	sfs.MakeBucket(bucketName)

	info, err := sfs.GetBucketInfo(bucketName)
	log.Println("info:", info, "err:", err)

	data := make([]byte, 20*1024*1024)
	fillRandom(data)

	for i := 0; i < 100; i++ {
		objectName := "data" + strconv.Itoa(i)
		buf := bytes.NewBuffer(data)
		r, c, err := sfs.PutObject(bucketName, objectName, buf)
		if err != nil {
			t.Fatal(err)
		}

		ew1 := &EmptyWriter{}
		io.Copy(ew1, r)
		c.Close()

		sfs.FinishPut(bucketName, objectName, int64(len(data)), true)

		_, err = sfs.CidOf(bucketName, objectName)
		if err != nil {
			t.Fatal(err)
		}

		obinfo, err := sfs.GetObjectInfo(bucketName, objectName)
		log.Println("obinfo:", obinfo, "err:", err)

		rc, _, err := sfs.GetObject(bucketName, objectName, 0)
		if err != nil {
			t.Fatal(err)
		}
		ew2 := &EmptyWriter{}
		io.Copy(ew2, rc)
		rc.Close()
	}

	loi, err := sfs.ListObjects(bucketName)

	fmt.Println("loi:", loi, "err:", err)
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
