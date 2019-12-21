package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-whosonfirst-media/operations/gather"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"log"
)

func main() {

	flag.Parse()

	ctx := context.Background()

	cb := func(rsp gather.GatherImagesResponse) error {

		log.Println(rsp)
		return nil
	}

	for _, path := range flag.Args() {

		uri := fmt.Sprintf("file://%s", path)
		log.Println(uri)

		bucket, err := blob.OpenBucket(ctx, uri)

		if err != nil {
			log.Fatal(err)
		}

		err = gather.GatherImages(ctx, bucket, cb)

		if err != nil {
			log.Fatal(err)
		}
	}
}