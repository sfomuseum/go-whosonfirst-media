package common

import (
	"context"
	"errors"
	"github.com/corona10/goimagehash"
	"image"
	"log"
	"gocloud.dev/blob"
)

func ImageHashes(ctx context.Context, bucket *blob.Bucket, im_path string) error {

	fh, err := bucket.NewReader(ctx, im_path, nil)

	if err != nil {
		return err
	}

	defer fh.Close()

	im, _, err := image.Decode(fh)

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	approaches := []string{
		"avg",
		"diff",
		"ext",
	}

	type HashRsp struct {
		Approach  string
		ImageHash *goimagehash.ImageHash
	}

	done_ch := make(chan bool)
	err_ch := make(chan error)
	rsp_ch := make(chan HashRsp)

	for _, a := range approaches {

		go func(ctx context.Context, im image.Image, a string) {

			defer func() {
				done_ch <- true
			}()

			h, err := imageHash(ctx, im, a)

			if err != nil {
				err_ch <- err
				return
			}

			if h != nil {

				rsp := HashRsp{
					Approach:  a,
					ImageHash: h,
				}

				rsp_ch <- rsp
			}

		}(ctx, im, a)

	}

	remaining := len(approaches)

	for remaining > 0 {

		select {

		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			log.Println(err)
		case rsp := <-rsp_ch:
			log.Println(rsp)
		}
	}

	return nil
}

func imageHash(ctx context.Context, im image.Image, approach string) (*goimagehash.ImageHash, error) {

	select {
	case <-ctx.Done():
		return nil, nil
	default:
		// pass
	}

	switch approach {
	case "avg":
		return goimagehash.AverageHash(im)
	case "diff":
		return goimagehash.DifferenceHash(im)
		//	case "ext":
		//		return goimagehash.ExtAverageHash(im, 8, 8)
	default:
		return nil, errors.New("Unknown approach")
	}
}
