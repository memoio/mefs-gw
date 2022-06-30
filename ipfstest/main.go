package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var FileSize int64 = 1024*1024*1024 + 32

func main() {
	// ipfsserver := memo.NewIpfsClient("127.0.0.1", "8000")
	accessKey := "memo"
	secretKey := "memoriae"
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
	objectNeme := "testdata1g.txt"
	// f, err := os.Open("/home/hekai/test/test.mp4")
	// if err != nil {
	// 	log.Println(err)
	// }
	// defer f.Close()
	// fi, _ := f.Stat()

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
	// {
	// 	data, err := client.GetObject(ctx, "nft", "bafybeia6mvfj26kvihqowmxzmkzluaui4eso27yezv3egjmokwjmodicau", miniogo.GetObjectOptions{})
	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// 	buf, _ := ioutil.ReadAll(data)
	// 	os.WriteFile("2.mp4", buf, 0644)

	// }

	// get memo bucket version
	// {
	// 	config, err := client.GetBucketVersioning(ctx, bucketName)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// 	log.Println(config)
	// }
	// get object for memo and s3
	{
		data, err := client.GetObject(ctx, bucketName, "3.txt", miniogo.GetObjectOptions{})
		if err != nil {
			log.Println(err)
		}
		buf, _ := ioutil.ReadAll(data)
		os.WriteFile("3.txt", buf, 0644)
	}

	{
		data := make([]byte, FileSize)
		rand.Read(data)
		r := bytes.NewBuffer(data)
		metadata := make(map[string]string)
		metadata["hekai"] = "hekai"
		info, err := client.PutObject(ctx, bucketName, objectNeme, r, FileSize, miniogo.PutObjectOptions{UserMetadata: metadata})
		if err != nil {
			fmt.Println("PutObject err:", err)
			return
		}
		fmt.Println("cid Info:", info.ETag)
	}

	// delete object
	// {
	// 	err := client.RemoveObject(ctx, bucketName, objectNeme, miniogo.RemoveObjectOptions{})
	// 	if err != nil {
	// 		log.Println("delete error: ", err)
	// 		return
	// 	}
	// 	log.Println("Delete success!")
	// }
}
