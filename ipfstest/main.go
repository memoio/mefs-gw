package main

import (
	"context"
	"fmt"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// ipfsserver := memo.NewIpfsClient("127.0.0.1", "8000")
	accessKey := "hekai"
	secretKey := "12345678"
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
	bucketName := "test"
	objectNeme := "testdata3"
	// data := make([]byte, 256*1024+32)
	// rand.Read(data)
	// r := bytes.NewBuffer(data)
	// {
	// 	metadata := make(map[string]string)
	// 	metadata["hekai"] = "hekai"
	// 	info, err := client.PutObject(ctx, bucketName, objectNeme, r, 256*1024+32, miniogo.PutObjectOptions{UserMetadata: metadata})
	// 	if err != nil {
	// 		fmt.Println("PutObject err:", err)
	// 	}
	// 	fmt.Println("Upload Info:", info)
	// }

	{
		objInfo, err := client.StatObject(ctx, bucketName, objectNeme, miniogo.StatObjectOptions{})
		if err != nil {
			fmt.Println("StatObject err:", err)
		}
		fmt.Println("stat Info:", objInfo.Metadata)
	}
}
