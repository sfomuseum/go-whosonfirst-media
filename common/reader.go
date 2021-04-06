package common

import (
	"context"
	"github.com/whosonfirst/go-reader"
	"sync"
)

var readers = make(map[string]reader.Reader)
var readers_mu = new(sync.RWMutex)

// NewReader returns a whosonfirst/go-reader.Reader instance. Instances
// are cached in memory for repeat lookups.
func NewReader(ctx context.Context, uri string) (reader.Reader, error) {

	readers_mu.Lock()
	defer readers_mu.Unlock()

	r, ok := readers[uri]

	if ok {
		return r, nil
	}

	r, err := reader.NewReader(ctx, uri)

	if err != nil {
		return nil, err
	}

	readers[uri] = r
	return r, nil
}
