package lookup

import (
	"bytes"
	"context"
	"gocloud.dev/blob"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
)

// this should be updated to take an arbitrary list of "lookup sources", as in repos, buckets, etc.
// (20191205/thisisaaronland)

func NewLookupMapFromRepoAndBucket(ctx context.Context, append_funcs []AppendLookupFunc, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lu := new(sync.Map)

	r, err := git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: repo_url,
	})

	if err != nil {
		return nil, err
	}

	it, err := r.BlobObjects()

	if err != nil {
		return nil, err
	}

	err = it.ForEach(func(bl *object.Blob) error {

		fh, err := bl.Reader()

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

		return nil
	})

	bucket_iter := bucket.List(nil)

	for {
		obj, err := bucket_iter.Next(ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if filepath.Ext(obj.Key) != ".geojson" {
			continue
		}

		fh, err := bucket.NewReader(ctx, obj.Key, nil)

		if err != nil {
			return nil, err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return nil, err
		}

		for _, f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := f(ctx, lu, fh)

			if err != nil {
				return nil, err
			}
		}

	}

	return lu, nil
}

func NewFingerprintMapFromRepoAndBucket(ctx context.Context, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {

	funcs := []AppendLookupFunc{FingerprintAppendLookupFunc}
	return NewLookupMapFromRepoAndBucket(ctx, funcs, repo_url, bucket)
}

func NewImageHashMapFromRepoAndBucket(ctx context.Context, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {
	funcs := []AppendLookupFunc{ImageHashAppendLookupFunc}
	return NewLookupMapFromRepoAndBucket(ctx, funcs, repo_url, bucket)
}
