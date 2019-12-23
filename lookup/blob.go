package lookup

import (
	"bytes"
	"context"
	"gocloud.dev/blob"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
)

type BlobLookerUpper struct {
	LookerUpper
	bucket *blob.Bucket
}

func NewBlobLookerUpper(ctx context.Context, uri string) (LookerUpper, error) {

	bucket, err := blob.OpenBucket(ctx, uri)

	if err != nil {
		return nil, err
	}

	return NewBlobLookerUpperWithBucket(ctx, bucket)
}

func NewBlobLookerUpperWithBucket(ctx context.Context, bucket *blob.Bucket) (LookerUpper, error) {

	l := &BlobLookerUpper{
		bucket: bucket,
	}

	return l, nil
}

func (l *BlobLookerUpper) Append(ctx context.Context, lu *sync.Map, append_funcs ...AppendLookupFunc) error {

	bucket_iter := l.bucket.List(nil)

	for {
		obj, err := bucket_iter.Next(ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if filepath.Ext(obj.Key) != ".geojson" {
			continue
		}

		fh, err := l.bucket.NewReader(ctx, obj.Key, nil)

		if err != nil {
			return err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		for _, f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := f(ctx, lu, fh)

			if err != nil {
				return err
			}
		}

	}

	return nil
}
