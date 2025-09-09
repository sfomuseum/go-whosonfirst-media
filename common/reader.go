package common

import (
	"context"
	"fmt"
	"sync"

	"github.com/whosonfirst/go-reader/v2"
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
		return nil, fmt.Errorf("Failed to create reader for '%s', %w", uri, err)
	}

	readers[uri] = r
	return r, nil
}
