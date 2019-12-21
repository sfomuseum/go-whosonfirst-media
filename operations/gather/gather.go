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
}

type GatherImageCallbackFunc func(GatherImagesResponse) error

func GatherImages(ctx context.Context, bucket *blob.Bucket, cb GatherImageCallbackFunc) error {

	gather_ch := make(chan GatherImagesResponse)

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

			go func(rsp GatherImagesResponse) {

				defer wg.Done()

				err := cb(rsp)

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

func CrawlImages(ctx context.Context, bucket *blob.Bucket, rsp_ch chan GatherImagesResponse) error {

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

			im_path := obj.Key
			ext := filepath.Ext(im_path)

			t := mime.TypeByExtension(ext)

			if t == "" {
				continue
			}

			if !strings.HasPrefix(t, "image/") {
				continue
			}

			fp, err := common.FingerprintFile(ctx, bucket, im_path)

			if err != nil {
				return err
			}

			rsp_ch <- GatherImagesResponse{
				Path:        im_path,
				Fingerprint: fp,
				MimeType:    t,
			}
		}

		return nil
	}

	return list(ctx, bucket, "")
}
