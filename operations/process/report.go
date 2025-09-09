package process

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	iiifuri "github.com/go-iiif/go-iiif-uri"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-export/v3"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"github.com/whosonfirst/go-writer/v3"
	"gocloud.dev/blob"
)

// type MediaPropertiesSizes defines a struct containing properties about a media file
type MediaPropertiesSize struct {
	// The filename extension for the media file.
	Extension string `json:"extension"`
	// The pixel height of the media file.
	Height int `json:"height"`
	// The pixel width of the media file.
	Width int `json:"width"`
	// The mimetype of the media file.
	Mimetype string `json:"mimetype"`
	// A secret associated with the media file (typically appended to its URI).
	Secret string `json:"secret"`
}

// IIIFProcessReport provides a struct mapping to the reports generate by the go-iiif/go-iiif 'iiif-process' functionality.
// See also: https://github.com/go-iiif/go-iiif#report-files
type IIIFProcessReport struct {
	// A data structure containing labels (for image sizes) mapped to their x (width) and y (height) pixel values.
	Dimensions IIIFProcessReportDimensions `json:"dimensions"`
	// A data structure containing colour palette information about an image file.
	Palette []IIIFProcessReportPalette `json:"palette"`
	// ...
	URIs IIIFProcessReportURIs `json:"uris"`
	// ...
	Origin string `json:"origin"`
	// ...
	OriginURI string `json:"origin_uri"`
	// ...
	OriginFingerprint string `json:"origin_fingerprint"`
}

// IIIFProcessReportDimensions provides a data structure containing labels (for image sizes) mapped to their x (width) and y (height) pixel values.
type IIIFProcessReportDimensions map[string][]int

// IIIFProcessReportPalette provides a data structure containing colour palette information about an image file.
type IIIFProcessReportPalette struct {
	// The name (or label) for a colour.
	Name string `json:"name"`
	// The (6-character) hexidecimal value for a colour.
	Hex string `json:"hex"`
	// The reference (source) for a colour.
	Reference string `json:"reference"`
}

// IIIFProcessReportURIs ...
type IIIFProcessReportURIs map[string]string

// URITemplateFunc ...
type URITemplateFunc func([]byte) ([]byte, error)

// ProcessReportCallback is a custom function for processing a IIIFProcessReport
type ProcessReportCallback func(context.Context, *IIIFProcessReport, []byte, []byte) error

// ReportProcessor provides a struct for managing and processing reports (produced by the go-iiif/go-iiif 'iiif-process' functionality).
type ReportProcessor struct {
	// A valid gocloud.dev/blob Bucket where reports are stored.
	Reports *blob.Bucket
	// A valid gocloud.dev/blob Bucket where pending images are stored.
	Pending *blob.Bucket
	// A valid whosonfirst/go-writer Writer for publishing Who's On First feature records
	WriterURI string
	// A valid whosonfirst/go-whosonfirst-export Exporter for exporting Who's On First feature records
	Exporter export.Exporter
	// A boolean flag indicating whether to remove pending images on completin
	Prune bool
	// ...
	URITemplateFunc URITemplateFunc
	// ...
	Callback ProcessReportCallback
}

// ProcessReports will process zero or more report URIs
func (p *ReportProcessor) ProcessReports(ctx context.Context, reports ...string) error {

	type ReportError struct {
		Report string
		Error  error
	}

	done_ch := make(chan bool)
	err_ch := make(chan ReportError)

	for _, report_uri := range reports {

		go func(report_uri string) {

			defer func() {
				done_ch <- true
			}()

			err := p.ProcessReport(ctx, report_uri)

			if err != nil {
				err_ch <- ReportError{
					Report: report_uri,
					Error:  fmt.Errorf("Failed to process report '%s', %w", report_uri, err),
				}
			}

		}(report_uri)
	}

	remaining := len(reports)
	report_errors := make([]ReportError, 0)

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			report_errors = append(report_errors, err)
		default:
			// pass
		}
	}

	if len(report_errors) > 0 {

		error_msgs := make([]string, len(report_errors))

		for i, e := range report_errors {
			error_msgs[i] = fmt.Sprintf("%s: %v", e.Report, e.Error)
		}

		return fmt.Errorf("One or more report errors: %s", strings.Join(error_msgs, ";"))
	}

	return nil
}

// ProcessReport will process a single report URI.
func (p *ReportProcessor) ProcessReport(ctx context.Context, report_uri string) error {

	select {
	case <-ctx.Done():
		return nil
	default:
		// pass
	}

	r, err := p.Reports.NewReader(ctx, report_uri, nil)

	if err != nil {
		return fmt.Errorf("Failed to create new reader for '%s', %w", report_uri, err)
	}

	defer r.Close()

	body, err := io.ReadAll(r)

	if err != nil {
		return fmt.Errorf("Failed to read report for '%s', %w", report_uri, err)
	}

	var process_report *IIIFProcessReport

	err = json.Unmarshal(body, &process_report)

	if err != nil {
		return fmt.Errorf("Failed to unmarshal report for '%s', %w", report_uri, err)
	}

	if process_report.OriginURI == "" {
		return fmt.Errorf("Report (%s) is missing origin_uri. Not sure what to do with it...", report_uri)
	}

	ru, err := iiifuri.NewURI(ctx, process_report.OriginURI)

	if err != nil {
		return fmt.Errorf("Failed to create new IIIF URI for report '%s' from '%s', %w", report_uri, process_report.OriginURI, err)
	}

	var wof_id int64

	switch ru.Scheme() {
	case "idsecret":

		u, _ := url.Parse(process_report.OriginURI) // we've just parse
		q := u.Query()

		str_id := q.Get("id") // iiifuri.URI interface does not have an "ID" method
		id, err := strconv.ParseInt(str_id, 10, 64)

		if err != nil {
			return fmt.Errorf("Failed to parse string ID '%s', %w", str_id, err)
		}

		wof_id = id

	default:
		return fmt.Errorf("Unsupported URI driver in report: %s", ru.Scheme())
	}

	// START OF sudo wrap me in a function or something

	wof_path, err := uri.Id2RelPath(wof_id)

	if err != nil {
		return fmt.Errorf("Failed to derive rel path for ID %d, %w", wof_id, err)
	}

	wof_fname := filepath.Base(wof_path)

	// note that we are reading from a *blob.Bucket rather than a
	// reader.Reader because we need the bucket in order to prune
	// files below (20191125/thisisaaronland)

	wof_r, err := p.Pending.NewReader(ctx, wof_fname, nil)

	if err != nil {
		return fmt.Errorf("Failed to create new reader for %s, %w", wof_fname, err)
	}

	defer wof_r.Close()

	old_feature, err := io.ReadAll(wof_r)

	if err != nil {
		return fmt.Errorf("Failed to read feature %s, %w", wof_fname, err)
	}

	new_feature, err := p.appendReport(old_feature, process_report)

	if err != nil {
		return fmt.Errorf("Failed to append report to feature %s, %w", wof_fname, err)
	}

	_, new_feature, err = p.Exporter.Export(ctx, new_feature)

	if err != nil {
		return fmt.Errorf("Failed to re-export feature %s, %w", wof_fname, err)
	}

	repo_rsp := gjson.GetBytes(new_feature, "properties.wof:repo")

	if !repo_rsp.Exists() {
		return fmt.Errorf("Missing properties.wof:repo")
	}

	repo := repo_rsp.String()

	writer_uri, err := url.QueryUnescape(p.WriterURI)

	if err != nil {
		return fmt.Errorf("Failed to unescape writer URI, %w", err)
	}

	if strings.Contains(writer_uri, "{repo}") {
		writer_uri = strings.Replace(writer_uri, "{repo}", repo, 1)
	}

	wr, err := writer.NewWriter(ctx, writer_uri)

	if err != nil {
		return fmt.Errorf("Failed to create writer for report '%s' from writer '%s', %w", report_uri, writer_uri, err)
	}

	feature_reader := bytes.NewReader(new_feature)
	feature_readcloser, err := ioutil.NewReadSeekCloser(feature_reader)

	if err != nil {
		return fmt.Errorf("Failed to create ReadSeekCloser from new feature, %w", err)
	}

	_, err = wr.Write(ctx, wof_path, feature_readcloser)

	if err != nil {
		return fmt.Errorf("Failed to write report to %s, %w", wof_path, err)
	}

	// END OF sudo wrap me in a function or something

	if p.Callback != nil {

		err := p.Callback(ctx, process_report, old_feature, new_feature)

		if err != nil {
			return fmt.Errorf("Failed to execute report processing callback, %w", err)
		}
	}

	if p.Prune {

		wg := new(sync.WaitGroup)

		prune_func := func(ctx context.Context, wg *sync.WaitGroup, bucket *blob.Bucket, key string) {

			select {
			case <-ctx.Done():
				return
			default:
				//
			}

			defer wg.Done()

			logger := slog.Default()
			logger = logger.With("key", key)

			logger.Debug("Prune key")

			exists, err := bucket.Exists(ctx, key)

			if err != nil {
				logger.Error("Failed to determine if key exists", "error", err)
				return
			}

			if !exists {
				return
			}

			err = bucket.Delete(ctx, key)

			if err != nil {
				logger.Error("Failed to delete key", "error", err)
			}
		}

		wg.Add(3)

		go prune_func(ctx, wg, p.Reports, report_uri)            // the processing report
		go prune_func(ctx, wg, p.Pending, process_report.Origin) // the actual image that got processed
		go prune_func(ctx, wg, p.Pending, wof_fname)             // the corresponding image feature w/out image details

		wg.Wait()
	}

	return nil
}

// appendReport will append properties from `report` to `body`.
func (p *ReportProcessor) appendReport(body []byte, report *IIIFProcessReport) ([]byte, error) {

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil, fmt.Errorf("Missing properties.wof:id")
	}

	logger := slog.Default()
	logger = logger.With("wof id", id_rsp.Int())

	logger.Debug("Append report")

	updates := map[string]interface{}{
		"properties.media:fingerprint":        report.OriginFingerprint,
		"properties.media:properties.colours": report.Palette,
	}

	var err error

	for path, value := range updates {

		body, err = sjson.SetBytes(body, path, value)

		if err != nil {
			return nil, fmt.Errorf("Failed to update %s property for record, %w", path, err)
		}
	}

	sizes := make(map[string]MediaPropertiesSize)

	for k, dims := range report.Dimensions {

		width := dims[0]
		height := dims[1]

		u, ok := report.URIs[k]

		if !ok {
			logger.Debug("Report URIs missing key, skipping", "key", k)
			continue
		}

		fname := filepath.Base(u)
		ext := filepath.Ext(fname)

		mimetype := mime.TypeByExtension(ext)

		if mimetype == "" {
			logger.Debug("Unknown mimetype, skipping", "filename", fname, "ext", ext)
			continue
		}

		ext = strings.TrimLeft(ext, ".")

		// THIS IS DUMB... PLEASE STOP DOING THIS...
		// USE A PROPER REGEXP OR FIGURE OUT HOW/WHERE
		// TO STORE SECRETS IN THE _r FILE...
		// (20190206/thisisaaronland)

		parts := strings.Split(fname, "_")
		secret := parts[1]

		sz := MediaPropertiesSize{
			Mimetype:  mimetype,
			Extension: ext,
			Width:     int(width),
			Height:    int(height),
			Secret:    secret,
		}

		sizes[k] = sz
	}

	updates = map[string]interface{}{
		"properties.media:properties.sizes": sizes,
		"properties.media:status_id":        1,
	}

	for path, value := range updates {

		body, err = sjson.SetBytes(body, path, value)

		if err != nil {
			return nil, fmt.Errorf("Failed to assign %s property, %w", path, err)
		}
	}

	source_rsp := gjson.GetBytes(body, "properties.media:source")

	if !source_rsp.Exists() {

		body, err = sjson.SetBytes(body, "properties.media:source", "unknown")

		if err != nil {
			return nil, fmt.Errorf("Failed to assign properties.media:source property, %w", err)
		}
	}

	if p.URITemplateFunc != nil {

		body, err = p.URITemplateFunc(body)

		if err != nil {
			return nil, fmt.Errorf("Failed to apply URITemplateFunc, %w", err)
		}
	}

	logger.Debug("Finished appending report")
	return body, nil
}
