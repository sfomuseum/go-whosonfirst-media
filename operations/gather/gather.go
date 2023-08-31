// package gather provides methods for compiling (gathering) a list of images to be processed.
package gather

import (
	"context"
	"io"
	"log"
	"mime"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sfomuseum/go-text-emboss"
	"github.com/sfomuseum/go-whosonfirst-media/common"
	"gocloud.dev/blob"
)

// type GatherImagesResponse provides a struct containing basic information about a file.
type GatherImagesResponse struct {
	// The path to the image file being gathered
	Path string
	// The SHA-1 hash of the file (defined in Path)
	Fingerprint string
	// The mimetype of the image file being gathered
	MimeType string
	// The set of image hashes for the image file being gathered
	ImageHashes []*common.ImageHashRsp
	// Text extracted from the image using the `sfomuseum/go-text-emboss` package.
	Text []byte
}

// type GatherImageCallbackFunc provides a function signature for custom callbacks applied to gathered images.
type GatherImageCallbackFunc func(*GatherImagesResponse) error

// type GatherImagesOptions provides configuration options for gathering images
type GatherImagesOptions struct {
	Bucket *blob.Bucket
	// A custom callback function to be applied to each image that is gathered
	Callback GatherImageCallbackFunc
	// A boolean flag indicating whether image hashes should be calculated for gathered images
	HashImages bool
	// A valid sfomuseum/go-text-emboss.Embosser instance used to extract text from gathered images
	Embosser emboss.Embosser
}

// GatherImages will gather images from bucket enabling image hashing by default.
func GatherImages(ctx context.Context, bucket *blob.Bucket, cb GatherImageCallbackFunc) error {

	opts := &GatherImagesOptions{
		Callback:   cb,
		HashImages: true,
		Bucket:     bucket,
	}

	return GatherImagesWithOptions(ctx, opts)
}

// GatherImages will gather images from bucket with custom configuration options.
func GatherImagesWithOptions(ctx context.Context, opts *GatherImagesOptions) error {

	gather_ch := make(chan *GatherImagesResponse)

	done_ch := make(chan bool)
	err_ch := make(chan error)

	go func() {

		err := CrawlImages(ctx, opts, gather_ch)

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
func CrawlImages(ctx context.Context, opts *GatherImagesOptions, rsp_ch chan *GatherImagesResponse) error {

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

			rsp, err := GatherImageResponseWithPath(ctx, opts, obj.Key)

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

	return list(ctx, opts.Bucket, "")
}

// GatherImageResponseWithPath will generate a single GatherImagesResponse response for `path` (contained in `bucket`).
func GatherImageResponseWithPath(ctx context.Context, opts *GatherImagesOptions, path string) (*GatherImagesResponse, error) {

	ext := filepath.Ext(path)

	t := mime.TypeByExtension(ext)

	if t == "" {
		return nil, nil
	}

	if !strings.HasPrefix(t, "image/") {
		return nil, nil
	}

	fp, err := common.FingerprintFile(ctx, opts.Bucket, path)

	if err != nil {
		return nil, err
	}

	hashes, err := common.ImageHashes(ctx, opts.Bucket, path)

	if err != nil {
		return nil, err
	}

	im_text, err := common.ExtractText(ctx, opts.Embosser, opts.Bucket, path)

	if err != nil {
		return nil, err
	}

	// TO DO: Extract text here...

	rsp := &GatherImagesResponse{
		Path:        path,
		MimeType:    t,
		Fingerprint: fp,
		ImageHashes: hashes,
		Text:        im_text,
	}

	return rsp, nil
}
