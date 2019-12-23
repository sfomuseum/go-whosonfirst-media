package lookup

import (
	"context"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	_ "log"
	"sync"
)

type AppendLookupFunc func(context.Context, *sync.Map, io.ReadCloser) error

func FingerprintAppendLookupFunc(ctx context.Context, lu *sync.Map, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		// log.Println("MISSING ID")
		return nil
	}

	fp_rsp := gjson.GetBytes(body, "properties.media:fingerprint")

	if !fp_rsp.Exists() {
		// log.Println("MISSING FINGERPRINT")
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(fp, id)

	if exists && id != has_id.(int64) {
		msg := fmt.Sprintf("Existing fingerprint key for %s (%d). Has ID: %d", fp, id, has_id.(int64))
		return errors.New(msg)
	}

	// log.Println(id, fp)
	return nil
}

func ImageHashAppendLookupFunc(ctx context.Context, lu *sync.Map, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		// log.Println("MISSING ID")
		return nil
	}

	fp_rsp := gjson.GetBytes(body, "properties.media:imagehash_avg")

	if !fp_rsp.Exists() {
		// log.Println("MISSING IMAGE HASH", id_rsp.Int())
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(fp, id)

	if exists && id != has_id.(int64) {
		msg := fmt.Sprintf("Existing image hash key for %s (%d). Has ID: %d", fp, id, has_id.(int64))
		return errors.New(msg)
	}

	// log.Println(id, fp)
	return nil
}
