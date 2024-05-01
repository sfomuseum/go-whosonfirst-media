package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	_ "log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/mknote"
	"github.com/sfomuseum/go-whosonfirst-media/operations/gather"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-id"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
	"gocloud.dev/blob"
)

// type Coordinates stores a single longitude, latitude coordinate pair.
type Coordinates []float64

// type Geomerty stores a GeoJSON geometry dictionary.
type Geometry struct {
	Type        string      `json:"type"`
	Coordinates Coordinates `json:"coordinates"`
}

// type Geomerty stores a GeoJSON properties dictionary.
type Properties map[string]interface{}

// type Feature provides a GeoJSON struct.
type Feature struct {
	Type       string     `json:"type"`
	Properties Properties `json:"properties"`
	Geometry   Geometry   `json:"geometry"`
}

// NewMediaFeatureNameFunc is a function for manipulating an input name in to a final name to be assigned to a feature's wof:name  property.
type NewMediaFeatureNameFunc func(string) (string, error)

// NewMediaFeatureOptions is a struct containing application-specific options used in the create of new media-related GeoJSON Features.
type NewMediaFeatureOptions struct {
	// The gocloud.dev/blob.Bucket where media records are loaded from
	SourceBucket *blob.Bucket
	// The name of the repository that this feature will be stored in.
	Repo string
	// An optional NewMediaFeatureNameFunc for deriving the final wof:name property assigned to the new feature.
	NameFunction NewMediaFeatureNameFunc
	// An optional string label for a WOF placetype. If present it will be used to derive the set of WOF IDs for that placetype and its ancestors, associated with the feature being depicted by the new media feature, to be assigned to the new feature.
	DepictsPlacetype string
	// Custom properties to assign to the new Feature
	CustomProperties map[string]interface{}
}

// Create a new geojson.Feature instance with media:properties associated with a Feature instance it depicts.
func NewMediaFeature(ctx context.Context, rsp *gather.GatherImagesResponse, depicts []byte, opts *NewMediaFeatureOptions) ([]byte, error) {

	pr, err := id.NewProvider(ctx)

	if err != nil {
		return nil, err
	}

	return NewMediaFeatureWithProvider(ctx, pr, rsp, depicts, opts)
}

// Create a new geojson.Feature instance with media:properties associated with a Feature instance it depicts, using a custom id.Provider.
func NewMediaFeatureWithProvider(ctx context.Context, pr id.Provider, rsp *gather.GatherImagesResponse, depicts []byte, opts *NewMediaFeatureOptions) ([]byte, error) {

	if opts.Repo == "" {
		return nil, errors.New("Missing wof:repo")
	}

	centroid, _, err := properties.Centroid(depicts)

	if err != nil {
		return nil, err
	}

	coords := []float64{
		centroid.X(),
		centroid.Y(),
	}

	geom := Geometry{
		Type:        "Point",
		Coordinates: coords,
	}

	depicts_id, err := properties.Id(depicts)

	if err != nil {
		return nil, fmt.Errorf("Failed to derive ID, %w", err)
	}

	depicts_name, err := properties.Name(depicts)

	if err != nil {
		return nil, fmt.Errorf("Failed to derive name, %w", err)
	}

	hierarchies := properties.Hierarchies(depicts)

	inception := properties.Inception(depicts)
	cessation := properties.Cessation(depicts)
	country := properties.Country(depicts)

	source_geom, err := properties.Source(depicts)

	if err != nil {
		return nil, fmt.Errorf("Failed to derive source, %w", err)
	}

	props := make(map[string]interface{})

	wof_id, err := pr.NewID(ctx)

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

	for _, h := range rsp.ImageHashes {
		k := fmt.Sprintf("media:imagehash_%s", h.Approach)
		props[k] = h.Hash
	}

	if rsp.ImageText != nil {
		props["media:imagetext"] = string(rsp.ImageText)
	}

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

	return enc_f, nil
}
