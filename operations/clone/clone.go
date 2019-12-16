package clone

// move an image and it's corresponding feature to a place where it can be processed

import (
	"context"
	"errors"
	"fmt"
	"gocloud.dev/blob"
	"io"
	"path/filepath"
)

type CloneImageOptions struct {
	Source   *blob.Bucket
	Target   *blob.Bucket
	ID       int64  // WOF (or sfomuseum-data-media-* ID)
	ImageID  int64  // source image ID
	Filename string // source image filename
	Force    bool
	Feature  io.ReadCloser
}

func CloneImage(ctx context.Context, opts *CloneImageOptions) (string, error) {

	image_ext := filepath.Ext(opts.Filename)

	var target_path string

	switch opts.ImageID {
	case -1:
		target_path = fmt.Sprintf("%d_%s", opts.ID, opts.Filename)
	default:
		target_path = fmt.Sprintf("%d_%d%s", opts.ID, opts.ImageID, image_ext)
	}

	select {
	case <-ctx.Done():
		return "", nil
	default:
		// pass
	}

	if !opts.Force {

		exists, err := opts.Target.Exists(ctx, target_path)

		if err != nil {
			return target_path, err
		}

		if exists {
			return target_path, nil
		}
	}

	var image_path string

	switch opts.ImageID {

	case -1:
		image_path = opts.Filename
	default:

		// FIX ME...what
		// pass

		return target_path, errors.New("Please implement me")
	}

	source_fh, err := opts.Source.NewReader(ctx, image_path, nil)

	if err != nil {
		return target_path, err
	}

	defer source_fh.Close()

	// this is where we might transform a source image (scaling it, converting
	// image format, etc.) prior to processing (20191121/thisisaaronland)

	target_wr, err := opts.Target.NewWriter(ctx, target_path, nil)

	if err != nil {
		return target_path, err
	}

	_, err = io.Copy(target_wr, source_fh)

	if err != nil {
		opts.Target.Delete(ctx, target_path)
		return target_path, err
	}

	err = target_wr.Close()

	if err != nil {
		return target_path, err
	}

	feature_path := fmt.Sprintf("%d.geojson", opts.ID)
	feature_fh := opts.Feature

	feature_wr, err := opts.Target.NewWriter(ctx, feature_path, nil)

	if err != nil {
		return feature_path, err
	}

	_, err = io.Copy(feature_wr, feature_fh)

	if err != nil {
		opts.Target.Delete(ctx, feature_path)
		return feature_path, err
	}

	err = feature_wr.Close()

	if err != nil {
		return feature_path, err
	}

	return target_path, nil
}
