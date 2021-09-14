// package clone provides method move an image and it's corresponding feature to a place where it can be processed.
package clone

import (
	"context"
	"errors"
	"fmt"
	"gocloud.dev/blob"
	"io"
	"path/filepath"
)

// CloneImageOptions is a struct containing application-specific options
// and details related to cloning an image.
type CloneImageOptions struct {
	// A blob.Bucket instance where images are read from.
	Source *blob.Bucket
	// A blob.Bucket instance where images are written to.
	Target *blob.Bucket
	// WOF (or sfomuseum-data-media-* ) ID
	ID int64
	// Source FileMaker image ID
	ImageID int64
	// Source FileMaker image filename
	Filename string
	// Boolean flag to signal that an image should be cloned even if it already exists in the target location.
	Force bool
	// The (GeoJSON) Feature record associated with this image.
	Feature io.ReadCloser
}

// CloneImage will copy a file from a source bucket to a target bucket, defined in 'opts'.
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

	image_path := opts.Filename

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

	if opts.Feature != nil {

		if opts.ID == 0 {
			return "", errors.New("Missing feature ID")
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
	}

	return target_path, nil
}
