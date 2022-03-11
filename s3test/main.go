package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	accessKey := "testuser"
	secretKey := "testpassword"
	endpoint := "0.0.0.0:5080"

	ctx := context.Background()

	optionsStaticCreds := &miniogo.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       false,
		BucketLookup: miniogo.BucketLookupAuto,
	}

	client, err := miniogo.New(endpoint, optionsStaticCreds)
	if err != nil {
		fmt.Println("New s3 client err: ", err)
	}
	bucketName := "mefstest"
	objectNeme := "testdata1"
	data := make([]byte, 256*1024+32)
	rand.Read(data)
	r := bytes.NewBuffer(data)
	{
		info, err := client.PutObject(ctx, bucketName, objectNeme, r, 256*1024+32, miniogo.PutObjectOptions{})
		if err != nil {
			fmt.Println("PutObject err:", err)
		}
		fmt.Println("Upload Info:", info)
	}

	downloadData := make([]byte, 256*1024+32)

	f := bytes.NewBuffer(downloadData)

	{
		object, err := client.GetObject(ctx, bucketName, objectNeme, miniogo.GetObjectOptions{})
		if err != nil {
			fmt.Println("GetObject err:", err)
			panic("")
		}

		n, err := io.Copy(f, object)
		if err != nil && err != io.EOF {
			fmt.Println("GetObject err:", err, n)
		}
		if n != 256*1024+32 {
			fmt.Println("Length error")
		}
		if !bytes.Equal(data, downloadData) {
			fmt.Println("not equal")
		}
	}
	{
		objs := client.ListObjects(ctx, bucketName, miniogo.ListObjectsOptions{})
		for obj := range objs {
			fmt.Println("Object: ", obj)
		}
	}
}
