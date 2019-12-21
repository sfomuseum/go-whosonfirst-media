package common

import (
	"context"
	"errors"
	"github.com/corona10/goimagehash"
	"gocloud.dev/blob"
	"image"
	"log"
)

type ImageHashRsp struct {
	Approach string
	Hash     string
}

func ImageHashes(ctx context.Context, bucket *blob.Bucket, im_path string) ([]*ImageHashRsp, error) {

	fh, err := bucket.NewReader(ctx, im_path, nil)

	if err != nil {
		return nil, err
	}

	defer fh.Close()

	im, _, err := image.Decode(fh)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	approaches := []string{
		"avg",
		"diff",
		"ext",
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
			log.Println(err)
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
		return nil, err
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

	return nil, errors.New("Impossible condition")
}
