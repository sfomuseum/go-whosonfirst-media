# go-whosonfirst-media

Go package for common methods and operations to produce Who's On First (WOF) style GeoJSON Feature documents for media records.

## Documentation

[![Go Reference](https://pkg.go.dev/badge/github.com/sfomuseum/go-whosonfirst-media.svg)](https://pkg.go.dev/github.com/sfomuseum/go-whosonfirst-media)

## Usage

`go-whosonfirst-media` is a package providing _common_ methods and operations to produce Who's On First (WOF) style GeoJSON Feature documents for media records. Specifically it is designed to provide methods for "gathering" images from gocloud.dev/blob Bucket sources, generating one or more image hashes (for comparison and deduplication purposes), cloning (writing) images to a destination for processing and finally for updating WOF feature records after an image has been processed.

Image _processing_ is not handled by this package. As written it is assumed to be handled by the [go-iiif/go-iiif](https://github.com/go-iiif/go-iiif) package and its "process image" tools which will produce "reports" that this package will use to update WOF feature records.

### Gathering images to process

```
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	
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
		enc, _ := json.Marshal(rsp)
		fmt.Println(string(enc))
		return nil
	}

	for _, uri := range flag.Args() {
		bucket, _ := blob.OpenBucket(ctx, uri)
		gather.GatherImages(ctx, bucket, cb)
	}
}
```

For example:

```
$> go run -mod vendor cmd/gather/main.go file:///usr/local/images/

{"Path":"20210810_2020_17_37.63444_-122.39280.png","Fingerprint":"d015d7246843a87e86a0e2b75cd89a833148603b","MimeType":"image/png","ImageHashes":[{"Approach":"avg","Hash":"a:7f63f75e7c5cfc6c"},{"Approach":"diff","Hash":"d:d6ce6eacccb839d9"}]}

...and so on
```

For cloning records, and other common operations, please consult the [operations documentation](https://pkg.go.dev/github.com/sfomuseum/go-whosonfirst-media/operations).

## See also

* https://github.com/go-iiif/go-iiif