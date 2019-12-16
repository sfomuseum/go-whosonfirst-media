package process

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	iiifuri "github.com/go-iiif/go-iiif-uri"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	wof_exporter "github.com/whosonfirst/go-whosonfirst-export/exporter"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	wof_uri "github.com/whosonfirst/go-whosonfirst-uri"
	wof_writer "github.com/whosonfirst/go-writer"
	"gocloud.dev/blob"
	"io/ioutil"
	"log"
	"mime"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type MediaPropertiesSize struct {
	Extension string `json:"extension"`
	Height    int    `json:"height"`
	Width     int    `json:"width"`
	Mimetype  string `json:"mimetype"`
	Secret    string `json:"secret"`
}

type IIIFProcessReport struct {
	Dimensions        IIIFProcessReportDimensions `json:"dimensions"`
	Palette           []IIIFProcessReportPalette  `json:"palette"`
	URIs              IIIFProcessReportURIs       `json:"uris"`
	Origin            string                      `json:"origin"`
	OriginURI         string                      `json:"origin_uri"`
	OriginFingerprint string                      `json:"origin_fingerprint"`
}

type IIIFProcessReportDimensions map[string][]int

type IIIFProcessReportPalette struct {
	Name      string `json:"name"`
	Hex       string `json:"hex"`
	Reference string `json:"reference"`
}

type IIIFProcessReportURIs map[string]string

type ReportProcessor struct {
	Reports   *blob.Bucket
	Pending   *blob.Bucket
	WriterURI string
	Exporter  wof_exporter.Exporter
	Prune     bool
}

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
					Error:  err,
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

		msg := fmt.Sprintf("One or more report errors: %s", strings.Join(error_msgs, ";"))
		return errors.New(msg)
	}

	return nil
}

func (p *ReportProcessor) ProcessReport(ctx context.Context, report_uri string) error {

	select {
	case <-ctx.Done():
		return nil
	default:
		// pass
	}

	fh, err := p.Reports.NewReader(ctx, report_uri, nil)

	if err != nil {
		return err
	}

	defer fh.Close()

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	var process_report *IIIFProcessReport

	err = json.Unmarshal(body, &process_report)

	if err != nil {
		return err
	}

	if process_report.OriginURI == "" {
		return errors.New("Report is missing origin_uri. Not sure what to do with it...")
	}

	uri, err := iiifuri.NewURI(process_report.OriginURI)

	if err != nil {
		return err
	}

	var wof_id int64

	switch uri.Driver() {
	case "idsecret":

		u, _ := url.Parse(process_report.OriginURI) // we've just parse
		q := u.Query()

		str_id := q.Get("id") // iiifuri.URI interface does not have an "ID" method
		id, err := strconv.ParseInt(str_id, 10, 64)

		if err != nil {
			return err
		}

		wof_id = id

	default:
		return errors.New("Unsupported URI driver in report.")
	}

	// START OF sudo wrap me in a function or something

	wof_path, err := wof_uri.Id2RelPath(wof_id)

	if err != nil {
		return err
	}

	wof_fname := filepath.Base(wof_path)

	// note that we are reading from a *blob.Bucket rather than a
	// reader.Reader because we need the bucket in order to prune
	// files below (20191125/thisisaaronland)

	wof_fh, err := p.Pending.NewReader(ctx, wof_fname, nil)

	if err != nil {
		return err
	}

	defer wof_fh.Close()

	f, err := feature.LoadFeatureFromReader(wof_fh)

	if err != nil {
		return err
	}

	feature_body := f.Bytes()

	feature_body, err = p.appendReport(feature_body, process_report)

	if err != nil {
		return err
	}

	feature_body, err = p.Exporter.Export(feature_body)

	if err != nil {
		return err
	}

	repo_rsp := gjson.GetBytes(feature_body, "properties.wof:repo")

	if !repo_rsp.Exists() {
		return errors.New("Missing properties.wof:repo")
	}

	repo := repo_rsp.String()

	wr_uri := p.WriterURI

	if strings.Contains(p.WriterURI, "%s") {
		wr_uri = fmt.Sprintf(p.WriterURI, repo)
	}

	wr, err := wof_writer.NewWriter(ctx, wr_uri)

	if err != nil {
		return err
	}

	feature_reader := bytes.NewReader(feature_body)
	feature_readcloser := ioutil.NopCloser(feature_reader)

	err = wr.Write(ctx, wof_path, feature_readcloser)

	if err != nil {
		return err
	}

	// END OF sudo wrap me in a function or something

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

			exists, err := bucket.Exists(ctx, key)

			if err != nil {
				log.Printf("Failed to determine if '%s' exists, %s\n", key, err)
				return
			}

			if !exists {
				return
			}

			err = bucket.Delete(ctx, key)

			if err != nil {
				log.Printf("Failed to delete %s, %s\n", key, err)
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

func (p *ReportProcessor) appendReport(body []byte, report *IIIFProcessReport) ([]byte, error) {

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil, errors.New("Missing properties.wof:id")
	}

	body, err := sjson.SetBytes(body, "properties.media:fingerprint", report.OriginFingerprint)

	if err != nil {
		return nil, err
	}

	body, err = sjson.SetBytes(body, "properties.media:properties.colours", report.Palette)

	if err != nil {
		return nil, err
	}

	sizes := make(map[string]MediaPropertiesSize)

	for k, dims := range report.Dimensions {

		width := dims[0]
		height := dims[1]

		u, ok := report.URIs[k]

		if !ok {
			log.Println("Missing URI key", k)
			continue
		}

		fname := filepath.Base(u)
		ext := filepath.Ext(fname)

		mimetype := mime.TypeByExtension(ext)

		if mimetype == "" {
			msg := fmt.Sprintf("Unknown mimetype %s (%s)", ext, fname)
			log.Println(msg)
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

	return nil, errors.New("Fix URI template")
	uri_template := ""

	body, err = sjson.SetBytes(body, "properties.media:uri_template", uri_template)

	if err != nil {
		return nil, err
	}

	body, err = sjson.SetBytes(body, "properties.media:properties.sizes", sizes)

	if err != nil {
		return nil, err
	}

	body, err = sjson.SetBytes(body, "properties.media:status_id", 1)

	if err != nil {
		return nil, err
	}

	source_rsp := gjson.GetBytes(body, "properties.media:source")

	if !source_rsp.Exists() {

		body, err = sjson.SetBytes(body, "properties.media:source", "unknown")

		if err != nil {
			return nil, err
		}
	}

	return body, nil
}
