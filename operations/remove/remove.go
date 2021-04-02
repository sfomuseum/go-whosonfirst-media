package remove

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-whosonfirst-media/common"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-export/exporter"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"gocloud.dev/blob"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Removal struct {
	DataSource  string
	MediaSource string
	Exporter    exporter.Exporter
	Dryrun      bool
	mu          *sync.RWMutex
}

type RemovalRequest struct {
	Id   int64  `json:"id"`
	Repo string `json:"repo"`
}

func NewRemoval(ex exporter.Exporter) (*Removal, error) {

	mu := new(sync.RWMutex)

	c := &Removal{
		DataSource:  "",
		MediaSource: "",
		Exporter:    ex,
		Dryrun:      false,
		mu:          mu,
	}

	return c, nil
}

func (c *Removal) Remove(ctx context.Context, requests ...*RemovalRequest) error {

	remaining := len(requests)

	done_ch := make(chan bool)
	removed_ch := make(chan int64)
	err_ch := make(chan error)

	for _, req := range requests {

		go func(req *RemovalRequest) {

			defer func() {
				done_ch <- true
			}()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			err := c.remove(ctx, req)

			if err != nil {
				msg := fmt.Sprintf("failed to remove %d because %s\n", req.Id, err)
				err_ch <- errors.New(msg)
				return
			}

			removed_ch <- req.Id
		}(req)
	}

	for remaining > 0 {

		select {
		case <-done_ch:
			remaining -= 1
		case id := <-removed_ch:
			log.Printf("successfully removed ID %d\n", id)
		case err := <-err_ch:
			log.Println(err)
		default:
			// pass
		}
	}

	return nil
}

func (c *Removal) remove(ctx context.Context, req *RemovalRequest) error {

	err := c.deprecateMedia(ctx, req)

	if err != nil {
		return err
	}

	err = c.deleteMediaFiles(ctx, req)

	if err != nil {
		return err
	}

	return nil
}

func (c *Removal) deprecateMedia(ctx context.Context, req *RemovalRequest) error {

	id := req.Id

	rel_path, err := uri.Id2RelPath(id)

	if err != nil {
		return err
	}

	reader_source := c.DataSource
	writer_source := c.DataSource

	if strings.Contains(c.DataSource, "%s") {
		reader_source = fmt.Sprintf(c.DataSource, req.Repo)
		writer_source = fmt.Sprintf(c.DataSource, req.Repo)
	}

	rdr, err := common.NewReader(ctx, reader_source)

	if err != nil {
		return err
	}

	wr, err := common.NewWriter(ctx, writer_source)

	if err != nil {
		return err
	}

	// basically we have to block on (git) master
	// (20190222/thisisaaronland)

	c.mu.Lock()
	defer c.mu.Unlock()

	old, err := rdr.Read(ctx, rel_path)

	if err != nil {
		return err
	}

	new, err := c.deprecateFeature(old)

	if err != nil {
		return err
	}

	body, err := io.ReadAll(new)

	if err != nil {
		return err
	}

	body, err = c.Exporter.Export(body)

	if err != nil {
		return err
	}

	br := bytes.NewReader(body)
	fh, err := ioutil.NewReadSeekCloser(br)

	if err != nil {
		return err
	}

	if c.Dryrun {
		log.Printf("[dryrun] write '%s' here\n", rel_path)
	} else {
		_, err = wr.Write(ctx, rel_path, fh)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Removal) deleteMediaFiles(ctx context.Context, req *RemovalRequest) error {

	rel_path, err := uri.Id2RelPath(req.Id)

	if err != nil {
		return err
	}

	bucket, err := blob.OpenBucket(ctx, c.MediaSource)

	if err != nil {
		return err
	}

	defer bucket.Close()

	root := filepath.Dir(rel_path)

	list_opts := &blob.ListOptions{
		Prefix: root,
	}

	iter := bucket.List(list_opts)

	for {
		obj, err := iter.Next(ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if c.Dryrun {
			log.Printf("[dryrun] delete '%s' here\n", obj.Key)
		} else {
			err = bucket.Delete(ctx, obj.Key)

			if err != nil {
				return err
			}
		}
	}

	// maybe?
	// return c.Bucket.Delete(ctx, root)

	return nil
}

func (c *Removal) deprecateFeature(fh io.ReadCloser) (io.ReadCloser, error) {

	body, err := io.ReadAll(fh)

	if err != nil {
		return nil, err
	}

	now := time.Now()

	body, err = sjson.SetBytes(body, "properties.edtf:deprecated", now.Format("2006-01-02"))

	if err != nil {
		return nil, err
	}

	body, err = sjson.SetBytes(body, "properties.mz:is_current", 0)

	if err != nil {
		return nil, err
	}

	body, err = sjson.SetBytes(body, "properties.wof:lastmodified", now.Unix())

	if err != nil {
		return nil, err
	}

	body, err = sjson.DeleteBytes(body, "properties.media:properties.sizes")

	if err != nil {
		return nil, err
	}

	body, err = sjson.DeleteBytes(body, "properties.media:properties.colours")

	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(body)

	return ioutil.NewReadSeekCloser(r)
}
