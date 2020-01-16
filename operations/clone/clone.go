package clone

// move an image and it's corresponding feature to a place where it can be processed

import (
	"context"
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

	// DUNNO YET, PUTTING THIS HERE SO IT DOESN'T GET LOST IN THE WEEDS
	// (20200116/thisisaaronland)

	/*

		// START OF maybe just make this part of the Clone operation

		rsp, err := wof_gather.GatherImageResponseWithPath(ctx, opts.Source, opts.Filename)

		if err != nil {
			return err
		}

		wof_fh := opts.Feature
		body, err := ioutil.ReadAll(wof_fh)

		if err != nil {
			return err
		}

		// START OF: common code with any.go (see above)

		body, err = sjson.SetBytes(body, "properties.media:mimetype", rsp.MimeType)

		if err != nil {
			return err
		}

		body, err = sjson.SetBytes(body, "properties.media:fingerprint", rsp.Fingerprint)

		if err != nil {
			return err
		}

		body, err = sjson.DeleteBytes(body, "properties.media:imagehash_avg")

		if err != nil {
			return err
		}

		body, err = sjson.DeleteBytes(body, "properties.media:imagehash_diff")

		if err != nil {
			return err
		}

		for _, h := range rsp.ImageHashes {

			k := fmt.Sprintf("properties.media:imagehash_%s", h.Approach)

			body, err = sjson.SetBytes(body, k, h.Hash)

			if err != nil {
				return err
			}
		}

		_, body, err = export.ExportBytes(ctx, body)

		if err != nil {
			return err
		}

		br := bytes.NewReader(body)
		wof_fh = ioutil.NopCloser(br)

	*/

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
