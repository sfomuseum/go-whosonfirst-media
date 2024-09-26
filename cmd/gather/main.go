// The gather tool will gather images from one or more sources and emit JSON-encoded gather.GatherImagesResponse data structures to STDOUT.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	_ "gocloud.dev/blob/fileblob"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"github.com/sfomuseum/go-whosonfirst-media/operations/gather"
	"gocloud.dev/blob"
)

func main() {

	flag.Parse()

	ctx := context.Background()

	cb := func(rsp *gather.GatherImagesResponse) error {

		enc, err := json.Marshal(rsp)

		if err != nil {
			return fmt.Errorf("Failed to marshal gather image response, %w", err)
		}

		fmt.Println(string(enc))
		return nil
	}

	for _, uri := range flag.Args() {

		bucket, err := blob.OpenBucket(ctx, uri)

		if err != nil {
			log.Fatal(err)
		}

		err = gather.GatherImages(ctx, bucket, cb)

		if err != nil {
			log.Fatalf("Failed to gather images, %v", err)
		}
	}
}
