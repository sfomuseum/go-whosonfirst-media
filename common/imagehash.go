package common

import (
	"context"
	"errors"
	"fmt"
	"image"
	"log/slog"

	"github.com/corona10/goimagehash"
	"gocloud.dev/blob"
)

// ImageHashRsp is a struct representing the results of an image hashing operation.
type ImageHashRsp struct {
	// String label describing the image hashing procedure used.
	Approach string
	// The hexidecimal hash of an image.
	Hash string
}

// Generate a list of ImageHashRsp instances for a file stored in a blob.Bucket instance
// using the corona10/goimagehash package.
func ImageHashes(ctx context.Context, bucket *blob.Bucket, im_path string) ([]*ImageHashRsp, error) {

	r, err := bucket.NewReader(ctx, im_path, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to create reader for %s, %w", im_path, err)
	}

	defer r.Close()

	im, _, err := image.Decode(r)

	if err != nil {
		return nil, fmt.Errorf("Failed to decode image from %s, %w", im_path, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	approaches := []string{
		"avg",
		"diff",
		// don't bother with this for now since it appears to return the same string hash as "avg" : "ext",
	}

	done_ch := make(chan bool)
	err_ch := make(chan error)
	rsp_ch := make(chan *ImageHashRsp)

	for _, a := range approaches {

		go func(ctx context.Context, im image.Image, a string) {

			defer func() {
				done_ch <- true
			}()

			rsp, err := imageHash(ctx, im, a)

			if err != nil {
				err_ch <- err
				return
			}

			rsp_ch <- rsp

		}(ctx, im, a)

	}

	remaining := len(approaches)
	hashes := make([]*ImageHashRsp, 0)

	for remaining > 0 {

		select {

		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			slog.Error("Image hash channel received error", "error", err)
		case rsp := <-rsp_ch:
			hashes = append(hashes, rsp)
		}
	}

	return hashes, nil
}

func imageHash(ctx context.Context, im image.Image, approach string) (*ImageHashRsp, error) {

	select {
	case <-ctx.Done():
		return nil, nil
	default:
		// pass
	}

	var i interface{}
	var err error

	switch approach {
	case "avg":
		i, err = goimagehash.AverageHash(im)
	case "diff":
		i, err = goimagehash.DifferenceHash(im)
	case "ext":
		i, err = goimagehash.ExtAverageHash(im, 8, 8)
	default:
		err = errors.New("Unknown approach")
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to process image hash appoach '%s', %w", approach, err)
	}

	switch approach {
	case "avg", "diff":

		h := i.(*goimagehash.ImageHash)

		rsp := &ImageHashRsp{
			Approach: approach,
			Hash:     h.ToString(),
		}

		return rsp, nil

	case "ext":

		h := i.(*goimagehash.ExtImageHash)

		rsp := &ImageHashRsp{
			Approach: approach,
			Hash:     h.ToString(),
		}

		return rsp, nil
	default:
		// pass
	}

	return nil, fmt.Errorf("Impossible condition")
}
