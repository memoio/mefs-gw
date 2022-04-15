package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

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
	// bucketName := "test"
	// // objectNeme := "testdata4"
	// f, err := os.Open("1.txt")
	// if err != nil {
	// 	log.Println(err)
	// }
	// defer f.Close()
	// fi, _ := f.Stat()

	// data := make([]byte, 256*1024+32)
	// rand.Read(data)
	// r := bytes.NewBuffer(data)
	// put memo and ipfs
	// {
	// 	metadata := make(map[string]string)
	// 	metadata["hekai"] = "hekai"
	// 	info, err := client.PutObject(ctx, bucketName, fi.Name(), f, fi.Size(), miniogo.PutObjectOptions{UserMetadata: metadata})
	// 	if err != nil {
	// 		fmt.Println("PutObject err:", err)
	// 	}
	// 	fmt.Println("cid Info:", info.ETag)
	// }

	// {
	// 	objInfo, err := client.StatObject(ctx, bucketName, objectNeme, miniogo.StatObjectOptions{})
	// 	if err != nil {
	// 		fmt.Println("StatObject err:", err)
	// 	}
	// 	fmt.Println("stat Info:", objInfo.Metadata)
	// }
	// bucketName := "metis-1088-prod"
	// {
	// 	objInfo, err := client.ListBuckets(ctx)
	// 	if err != nil {
	// 		fmt.Println("StatObject err:", err)
	// 	}
	// 	fmt.Println("stat Info:", objInfo)
	// }

	// get memo from cid
	{
		data, err := client.GetObject(ctx, "nft", "bafkreic3ypzvwied7qqcfshcyeufe7kyg3sf4xw5ioafhw2hviwlzd4grq", miniogo.GetObjectOptions{})
		if err != nil {
			log.Println(err)
		}
		buf, _ := ioutil.ReadAll(data)
		os.WriteFile("2.txt", buf, 0644)

	}
}
