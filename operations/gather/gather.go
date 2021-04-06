package gather

import (
	"context"
	"github.com/sfomuseum/go-whosonfirst-media/common"
	"gocloud.dev/blob"
	"io"
	"log"
	"mime"
	"path/filepath"
	"strings"
	"sync"
)

type GatherImagesResponse struct {
	Path        string
	Fingerprint string
	MimeType    string
	ImageHashes []*common.ImageHashRsp
}

type GatherImageCallbackFunc func(*GatherImagesResponse) error

type GatherImagesOptions struct {
	Callback   GatherImageCallbackFunc
	HashImages bool
}

func GatherImages(ctx context.Context, bucket *blob.Bucket, cb GatherImageCallbackFunc) error {

	opts := &GatherImagesOptions{
		Callback:   cb,
		HashImages: true,
	}

	return GatherImagesWithOptions(ctx, bucket, opts)
}

func GatherImagesWithOptions(ctx context.Context, bucket *blob.Bucket, opts *GatherImagesOptions) error {

	gather_ch := make(chan *GatherImagesResponse)

	done_ch := make(chan bool)
	err_ch := make(chan error)

	go func() {

		err := CrawlImages(ctx, bucket, gather_ch)

		if err != nil {
			err_ch <- err
		}

		done_ch <- true
	}()

	gathering := true
	wg := new(sync.WaitGroup)

	for {
		select {

		case <-done_ch:
			gathering = false
		case err := <-err_ch:
			return err
		case gather_rsp := <-gather_ch:

			wg.Add(1)

			go func(rsp *GatherImagesResponse) {

				defer wg.Done()

				err := opts.Callback(rsp)

				if err != nil {
					log.Printf("Failed to process %s, %s\n", rsp.Path, err)
				}

			}(gather_rsp)

		}

		if !gathering {
			break
		}
	}

	wg.Wait()
	return nil
}

// Iterate through all the items stored in a blob.Bucket instance, generate a GatherImagesResponse for things that are images
// and dispatch that response to a user-defined channel.
func CrawlImages(ctx context.Context, bucket *blob.Bucket, rsp_ch chan *GatherImagesResponse) error {

	var list func(context.Context, *blob.Bucket, string) error

	list = func(ctx context.Context, b *blob.Bucket, prefix string) error {

		iter := b.List(&blob.ListOptions{
			Delimiter: "/",
			Prefix:    prefix,
		})

		for {

			select {
			case <-ctx.Done():
				return nil
			default:
				// pass
			}

			obj, err := iter.Next(ctx)

			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if obj.IsDir {

				err := list(ctx, b, obj.Key)

				if err != nil {
					return err
				}

				continue
			}

			rsp, err := GatherImageResponseWithPath(ctx, bucket, obj.Key)

			if err != nil {
				return err
			}

			if rsp == nil {
				continue
			}

			rsp_ch <- rsp
		}

		return nil
	}

	return list(ctx, bucket, "")
}

func GatherImageResponseWithPath(ctx context.Context, bucket *blob.Bucket, path string) (*GatherImagesResponse, error) {

	ext := filepath.Ext(path)

	t := mime.TypeByExtension(ext)

	if t == "" {
		return nil, nil
	}

	if !strings.HasPrefix(t, "image/") {
		return nil, nil
	}

	fp, err := common.FingerprintFile(ctx, bucket, path)

	if err != nil {
		return nil, err
	}

	hashes, err := common.ImageHashes(ctx, bucket, path)

	if err != nil {
		return nil, err
	}

	rsp := &GatherImagesResponse{
		Path:        path,
		MimeType:    t,
		Fingerprint: fp,
		ImageHashes: hashes,
	}

	return rsp, nil
}
