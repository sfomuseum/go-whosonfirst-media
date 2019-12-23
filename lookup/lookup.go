package lookup

import (
	"context"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"gocloud.dev/blob"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
)

type AppendLookupFunc func(*sync.Map, io.ReadCloser) error

func FingerprintAppendLookupFunc(lu *sync.Map, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		log.Println("MISSING ID")
		return nil
	}

	fp_rsp := gjson.GetBytes(body, "properties.media:fingerprint")

	if !fp_rsp.Exists() {
		// log.Println("MISSING FINGERPRINT")
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	_, exists := lu.LoadOrStore(fp, id)

	if exists {
		msg := fmt.Sprintf("Existing fingerprint key for %s", fp)
		return errors.New(msg)
	}

	// log.Println(id_rsp.Int(), fp_rsp.String())
	return nil
}

func ImageHashAppendLookupFunc(lu *sync.Map, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		log.Println("MISSING ID")
		return nil
	}

	fp_rsp := gjson.GetBytes(body, "properties.media:imagehash_avg")

	if !fp_rsp.Exists() {
		// log.Println("MISSING IMAGE HASH", id_rsp.Int())
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	// log.Println("HASH", id, fp)

	_, exists := lu.LoadOrStore(fp, id)

	if exists {
		msg := fmt.Sprintf("Existing image hash key for %s", fp)
		return errors.New(msg)
	}

	// log.Println(id_rsp.Int(), fp_rsp.String())
	return nil
}

// this should be updated to take an arbitrary list of "lookup sources", as in repos, buckets, etc.
// (20191205/thisisaaronland)

func NewLookupMapFromRepoAndBucket(ctx context.Context, append_func AppendLookupFunc, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {

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

		return append_func(lu, fh)
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

		append_func(lu, fh)
	}

	return lu, nil
}

func NewFingerprintMapFromRepoAndBucket(ctx context.Context, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {
	return NewLookupMapFromRepoAndBucket(ctx, FingerprintAppendLookupFunc, repo_url, bucket)
}

func NewImageHashMapFromRepoAndBucket(ctx context.Context, repo_url string, bucket *blob.Bucket) (*sync.Map, error) {
	return NewLookupMapFromRepoAndBucket(ctx, ImageHashAppendLookupFunc, repo_url, bucket)
}
