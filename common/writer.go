package common

import (
	"context"
	"fmt"
	"sync"

	"github.com/whosonfirst/go-writer/v3"
)

var writers = make(map[string]writer.Writer)
var writers_mu = new(sync.RWMutex)

// NewWriter returns a whosonfirst/go-writer.Writer instance. Instances
// are cached in memory for repeat lookups.
func NewWriter(ctx context.Context, uri string) (writer.Writer, error) {

	writers_mu.Lock()
	defer writers_mu.Unlock()

	r, ok := writers[uri]

	if ok {
		return r, nil
	}

	r, err := writer.NewWriter(ctx, uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new writer for %s, %w", uri, err)
	}

	writers[uri] = r
	return r, nil
}
