package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/mknote"
	"github.com/sfomuseum/go-whosonfirst-media/operations/gather"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-id"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
	"gocloud.dev/blob"
	_ "log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Coordinates []float64

type Geometry struct {
	Type        string      `json:"type"`
	Coordinates Coordinates `json:"coordinates"`
}

type Properties map[string]interface{}

type Feature struct {
	Type       string     `json:"type"`
	Properties Properties `json:"properties"`
	Geometry   Geometry   `json:"geometry"`
}

type NewMediaFeatureNameFunc func(string) (string, error)

type NewMediaFeatureOptions struct {
	SourceBucket     *blob.Bucket
	Repo             string
	NameFunction     NewMediaFeatureNameFunc
	DepictsPlacetype string
	CustomProperties map[string]interface{}
}

func NewMediaFeature(ctx context.Context, rsp gather.GatherImagesResponse, depicts geojson.Feature, opts *NewMediaFeatureOptions) (geojson.Feature, error) {

	pr, err := id.NewProvider(ctx)

	if err != nil {
		return nil, err
	}

	return NewMediaFeatureWithProvider(ctx, pr, rsp, depicts, opts)
}

func NewMediaFeatureWithProvider(ctx context.Context, pr id.Provider, rsp gather.GatherImagesResponse, depicts geojson.Feature, opts *NewMediaFeatureOptions) (geojson.Feature, error) {

	if opts.Repo == "" {
		return nil, errors.New("Missing wof:repo")
	}

	centroid, err := whosonfirst.Centroid(depicts)

	if err != nil {
		return nil, err
	}

	depicts_coords := centroid.Coord()

	coords := []float64{
		depicts_coords.X,
		depicts_coords.Y,
	}

	geom := Geometry{
		Type:        "Point",
		Coordinates: coords,
	}

	depicts_id := whosonfirst.Id(depicts)
	depicts_name := whosonfirst.Name(depicts)
	hierarchies := whosonfirst.Hierarchies(depicts)

	inception := whosonfirst.Inception(depicts)
	cessation := whosonfirst.Cessation(depicts)
	country := whosonfirst.Country(depicts)
	source_geom := whosonfirst.Source(depicts)

	props := make(map[string]interface{})

	wof_id, err := pr.NewID()

	if err != nil {
		return nil, err
	}

	wof_name := depicts_name

	if opts.NameFunction != nil {

		name, err := opts.NameFunction(depicts_name)

		if err != nil {
			return nil, err
		}

		wof_name = name
	}

	depicts_ids := make([]int64, 0)

	depicts_map := new(sync.Map)
	depicts_map.Store(depicts_id, true)

	if opts.DepictsPlacetype != "" {

		pt, err := placetypes.GetPlacetypeByName(opts.DepictsPlacetype)

		if err != nil {
			return nil, err
		}

		roles := []string{
			"common",
			"optional",
			"common_optional",
		}

		for _, d := range placetypes.DescendantsForRoles(pt, roles) {

			k := fmt.Sprintf("%s_id", d.Name)

			for _, hier := range hierarchies {

				id, ok := hier[k]

				if ok {
					depicts_map.LoadOrStore(id, true)
				}
			}
		}
	}

	depicts_map.Range(func(k interface{}, v interface{}) bool {
		id := k.(int64)
		depicts_ids = append(depicts_ids, id)
		return true
	})

	props["wof:id"] = wof_id
	props["wof:name"] = wof_name
	props["wof:repo"] = opts.Repo

	props["edtf:inception"] = inception
	props["edtf:cessation"] = cessation

	props["wof:placetype"] = "media"
	props["wof:parent_id"] = depicts_id
	props["wof:country"] = country
	props["wof:depicts"] = depicts_ids
	props["wof:hierarchy"] = hierarchies

	props["iso:country"] = country
	props["src:geom"] = source_geom

	props["media:source"] = "unknown"
	props["media:medium"] = "image"
	props["media:mimetype"] = rsp.MimeType
	props["media:fingerprint"] = rsp.Fingerprint

	props["mz:is_approximate"] = 1

	/*
		props["mz:latitude"] = depicts_coords.Y
		props["mz:longitude"] = depicts_coords.X
		props["mz:min_latitude"] = depicts_coords.Y
		props["mz:min_longitude"] = depicts_coords.X
		props["mz:max_latitude"] = depicts_coords.Y
		props["mz:max_longitude"] = depicts_coords.X
	*/

	if opts.CustomProperties != nil {
		for k, v := range opts.CustomProperties {
			props[k] = v
		}
	}
	var exif_data *exif.Exif

	switch filepath.Ext(rsp.Path) {

	case ".jpg", ".jpeg":

		im_fname := filepath.Base(rsp.Path)
		im_fh, err := opts.SourceBucket.NewReader(ctx, im_fname, nil)

		if err != nil {
			return nil, err
		}

		exif.RegisterParsers(mknote.All...)
		im_exif, err := exif.Decode(im_fh)

		if err == nil {
			exif_data = im_exif
		}

		im_fh.Close()

	default:
		// pass
	}

	if exif_data != nil {

		/*
			> exiv2 -pa _Case_Automotive_Toys_and_Plasitic_Models.jpg | grep DateTime
			Exif.Image.DateTime                          Ascii      20  2018:12:26 09:30:09
			Exif.Photo.DateTimeOriginal                  Ascii      20  2018:12:20 12:22:42
			Exif.Photo.DateTimeDigitized                 Ascii      20  2018:12:20 12:22:42

		*/

		tag, err := exif_data.Get("DateTimeOriginal")

		if err == nil {

			str_dt := tag.String()

			str_dt = strings.Trim(str_dt, "\"")    // see this? it's important
			str_dt = fmt.Sprintf("%s PST", str_dt) // see this? we might regret it one day...

			// remember these datetime formats are Go's internal cray-cray
			// for working with time... (20190201/thisisaaronland)

			exif_fmt := "2006:01:02 15:04:05 MST"
			// iso_fmt := "2006-01-02T15:04:05-0700"

			t, err := time.Parse(exif_fmt, str_dt)

			if err == nil {

				ldn, _ := time.LoadLocation("Europe/London")
				t = t.In(ldn)

				props["media:created"] = t.Unix()
			}

		} else {
			// log.Printf("Failed to wrangle dates for 'DateTimeOriginal' tag, %s\n", err)
		}

		// geo stuff

		lat, lon, err := exif_data.LatLong()

		if err != nil && lat != 0.0 && lon != 0.0 {

			geom.Coordinates[0] = lon
			geom.Coordinates[1] = lat

			props["mz:is_approximate"] = 0
		}
	}

	f := &Feature{
		Type:       "Feature",
		Geometry:   geom,
		Properties: props,
	}

	enc_f, err := json.Marshal(f)

	if err != nil {
		return nil, err
	}

	// see the way we're not trying to return a WOF specific
	// feature? that is on purpose since we have no idea what
	// sort of properties (notably  wof:placetype) have been
	// set above (20191216/thisisaaronland)

	br := bytes.NewReader(enc_f)
	return feature.LoadGeoJSONFeatureFromReader(br)
}
