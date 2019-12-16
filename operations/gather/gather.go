package gather

import (
	"context"
	"github.com/sfomuseum/go-whosonfirst-media/common"
	"github.com/whosonfirst/go-whosonfirst-crawl"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type GatherPhotosResponse struct {
	Path        string
	Fingerprint string
	MimeType    string
}

type GatherPhotoCallbackFunc func(GatherPhotosResponse) error

func GatherPhotos(ctx context.Context, root string, cb GatherPhotoCallbackFunc) error {

	gather_ch := make(chan GatherPhotosResponse)

	done_ch := make(chan bool)
	err_ch := make(chan error)

	go func() {

		err := CrawlPhotos(ctx, root, gather_ch)

		if err != nil {
			err_ch <- err
		}

		done_ch <- true
	}()

	gathering := true
	wg := new(sync.WaitGroup)

	for {
		select {

		case <-done_ch:
			gathering = false
		case err := <-err_ch:
			return err
		case gather_rsp := <-gather_ch:

			wg.Add(1)

			go func(rsp GatherPhotosResponse) {

				defer wg.Done()

				err := cb(rsp)

				if err != nil {
					log.Printf("Failed to process %s, %s\n", rsp.Path, err)
				}

			}(gather_rsp)

		}

		if !gathering {
			break
		}
	}

	wg.Wait()
	return nil
}

func CrawlPhotos(ctx context.Context, root string, rsp_ch chan GatherPhotosResponse) error {

	cb := func(im_path string, info os.FileInfo) error {

		select {
		case <-ctx.Done():
			return nil
		default:
			// pass
		}

		if info.IsDir() {
			return nil
		}

		ext := filepath.Ext(im_path)

		t := mime.TypeByExtension(ext)

		if t == "" {
			return nil
		}

		if !strings.HasPrefix(t, "image/") {
			return nil
		}

		im_hash, err := common.HashFile(im_path)

		if err != nil {
			return err
		}

		rsp_ch <- GatherPhotosResponse{
			Path:        im_path,
			Fingerprint: im_hash,
			MimeType:    t,
		}

		return nil
	}

	cr := crawl.NewCrawler(root)
	return cr.Crawl(cb)
}
