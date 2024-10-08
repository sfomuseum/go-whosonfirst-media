// package rotate provides common methods for rotating image files.
package rotate

// THIS NEEDS TO BE UPDATED TO USE aws-sdk-go-v2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/aaronland/go-image-tools/imaging"
	"github.com/aaronland/go-image-tools/util"
	"github.com/aaronland/go-string/random"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sfomuseum/go-whosonfirst-media/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-export/v2"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"gocloud.dev/blob"
)

// type Rotation provides a struct for rotating media files.
type Rotation struct {
	// DataSource is a valid gocloud.dev/blob Bucket URI where WOF feature records associated with media files are stored.
	DataSource string
	// MediaSource is a valid gocloud.dev/blob Bucket URI where media files are stored.
	MediaSource string
	// A valid whosonfirst/go-whosonfirst-export Exporter for exporting Who's On First feature records.
	Exporter export.Exporter
	// A boolean flag indicating whether to perform a removal in "dry run" mode.
	Dryrun bool
}

// type RotateRequest provides a struct encapsulating data for rotating a given media file.
type RotateRequest struct {
	// A valid Who's On First ID (to remove).
	Id int64 `json:"id"`
	// The number of degrees to rotate a media file.
	Degrees int `json:"degrees"`
	// The data repository where Id is stored.
	Repo string `json:"repo"`
	// A boolean flag indicating whether to remove the old file on completion.
	Prune bool `json:"prune"`
}

// type RotateResponse provides a struct containing metadata about a rotated media file.
type RotateResponse struct {
	// The Who's On First ID of the media file that was rotated.
	Id int64
	// The URI secret associated with the rotated media file.
	Secret string
	// The URI label associated with the rotated media file.
	Label string
	// The filename extension associated with the rotated media file.
	Extension string
	// An image.Image instance containing the body of the rotated media file.
	Image image.Image
	// The URI of the unrotated media file.
	OldPath string
	// The URI of the rotated media file.
	NewPath string
}

// NewRotation returns a new Rotation instance.
func NewRotation(ex export.Exporter) (*Rotation, error) {

	r := &Rotation{
		DataSource:  "",
		MediaSource: "",
		Exporter:    ex,
		Dryrun:      false,
	}

	return r, nil
}

// Rotate will rotate one or more media files defined in 'requests'.
func (r *Rotation) Rotate(ctx context.Context, requests ...*RotateRequest) error {

	for _, req := range requests {

		err := r.rotate(ctx, req)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Rotation) rotate(ctx context.Context, req *RotateRequest) error {

	select {
	case <-ctx.Done():
		return nil
	default:
		// pass
	}

	if req.Degrees == 0 {
		return errors.New("Nothing to rotate")
	}

	if req.Degrees > 360 {
		return errors.New("Invalid rotation")
	}

	rel_path, err := uri.Id2RelPath(req.Id)

	if err != nil {
		return err
	}

	reader_source := r.DataSource
	writer_source := r.DataSource

	if strings.Contains(r.DataSource, "%s") {
		reader_source = fmt.Sprintf(r.DataSource, req.Repo)
		writer_source = fmt.Sprintf(r.DataSource, req.Repo)
	}

	rdr, err := common.NewReader(ctx, reader_source)

	if err != nil {
		return err
	}

	wr, err := common.NewWriter(ctx, writer_source)

	if err != nil {
		return err
	}

	bucket, err := blob.OpenBucket(ctx, r.MediaSource)

	if err != nil {
		return err
	}

	defer bucket.Close()

	fh, err := rdr.Read(ctx, rel_path)

	if err != nil {
		return err
	}

	defer fh.Close()

	body, err := io.ReadAll(fh)

	if err != nil {
		return err
	}

	sizes := gjson.GetBytes(body, "properties.media:properties.sizes")

	if !sizes.Exists() {
		return errors.New("Missing properties.media:properties.sizes")
	}

	wof_id := req.Id

	remaining := 0

	done_ch := make(chan bool)
	err_ch := make(chan error)
	rsp_ch := make(chan *RotateResponse)

	rand_opts := random.DefaultOptions()
	rand_opts.AlphaNumeric = true

	new_secret, err := random.String(rand_opts)

	if err != nil {
		return err
	}

	new_secret_o, err := random.String(rand_opts)

	if err != nil {
		return err
	}

	for label, details := range sizes.Map() {

		secret_rsp := details.Get("secret")

		if !secret_rsp.Exists() {
			return errors.New("Missing secret")
		}

		extension_rsp := details.Get("extension")

		if !extension_rsp.Exists() {
			return errors.New("Missing extension")
		}

		secret := secret_rsp.String()
		extension := extension_rsp.String()

		local_new_secret := new_secret

		if label == "o" {
			local_new_secret = new_secret_o
		}

		remaining += 1

		go func(id int64, secret string, new_secret string, label string, extension string) {

			defer func() {
				done_ch <- true
			}()

			root, err := uri.Id2Path(id)

			if err != nil {
				err_ch <- err
				return
			}

			old_fname := fmt.Sprintf("%d_%s_%s.%s", wof_id, secret, label, extension)
			new_fname := fmt.Sprintf("%d_%s_%s.%s", wof_id, new_secret, label, extension)

			old_path := filepath.Join(root, old_fname)
			new_path := filepath.Join(root, new_fname)

			im, err := r.rotateImage(ctx, req, bucket, old_path, new_path)

			if err != nil {
				err_ch <- err
				return
			}

			rsp := &RotateResponse{
				Id:        id,
				Secret:    new_secret,
				Label:     label,
				Extension: extension,
				Image:     im,
				OldPath:   old_path,
				NewPath:   new_path,
			}

			rsp_ch <- rsp

		}(wof_id, secret, local_new_secret, label, extension)
	}

	new_paths := make([]string, 0)
	old_paths := make([]string, 0)

	scrub := func(paths []string) {

		for _, path := range paths {
			bucket.Delete(ctx, path)
		}

	}

	for remaining > 0 {

		select {
		case <-done_ch:
			remaining -= 1
		case rsp := <-rsp_ch:

			old_paths = append(old_paths, rsp.OldPath)
			new_paths = append(new_paths, rsp.NewPath)

			im := rsp.Image
			label := rsp.Label

			bounds := im.Bounds()
			dims := bounds.Max

			root_path := fmt.Sprintf("properties.media:properties.sizes.%s", label)

			secret_path := fmt.Sprintf("%s.secret", root_path)
			height_path := fmt.Sprintf("%s.height", root_path)
			width_path := fmt.Sprintf("%s.width", root_path)

			body, _ = sjson.SetBytes(body, secret_path, rsp.Secret)
			body, _ = sjson.SetBytes(body, width_path, dims.X)
			body, _ = sjson.SetBytes(body, height_path, dims.Y)

		case e := <-err_ch:

			scrub(new_paths)
			return e
		default:
			// pass
		}
	}

	body, err = r.Exporter.Export(ctx, body)

	if err != nil {
		scrub(new_paths)
		return err
	}

	br := bytes.NewReader(body)
	out, err := ioutil.NewReadSeekCloser(br)

	if err != nil {
		return err
	}

	if r.Dryrun {
		log.Printf("[dryrun] write '%s' here\n", rel_path)
	} else {

		_, err = wr.Write(ctx, rel_path, out)

		if err != nil {
			scrub(new_paths)
			return err
		}
	}

	if req.Prune {

		if r.Dryrun {
			log.Printf("[dryrun] scrub old files here\n")
		} else {
			scrub(old_paths)
		}
	}

	return nil
}

func (r *Rotation) rotateImage(ctx context.Context, req *RotateRequest, bucket *blob.Bucket, old_path string, new_path string) (image.Image, error) {

	if req.Degrees == 0 {
		return nil, errors.New("Nothing to rotate")
	}

	if req.Degrees > 360 {
		return nil, errors.New("Invalid rotation")
	}

	fh, err := bucket.NewReader(ctx, old_path, nil)

	if err != nil {
		return nil, err
	}

	defer fh.Close()

	im, format, err := util.DecodeImageFromReader(fh)

	if err != nil {
		return nil, err
	}

	im = imaging.Rotate(im, float64(req.Degrees), color.White)

	if r.Dryrun {
		log.Printf("[dryrun] write '%s' here\n", new_path)
	} else {

		before := func(asFunc func(interface{}) bool) error {

			s3_req := &s3manager.UploadInput{}
			ok := asFunc(&s3_req)

			if ok {
				s3_req.ACL = aws.String("public-read")
			}

			return nil
		}

		wr_opts := &blob.WriterOptions{
			BeforeWrite: before,
		}

		wr, err := bucket.NewWriter(ctx, new_path, wr_opts)

		if err != nil {
			return nil, err
		}

		err = util.EncodeImage(im, format, wr)

		if err != nil {
			return nil, err
		}

		err = wr.Close()

		if err != nil {
			return nil, err
		}
	}

	return im, nil
}
