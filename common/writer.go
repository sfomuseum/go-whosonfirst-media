package common

import (
	"context"
	"github.com/whosonfirst/go-writer"
	"sync"
)

var writers = make(map[string]writer.Writer)
var writers_mu = new(sync.RWMutex)

func NewWriter(ctx context.Context, uri string) (writer.Writer, error) {

	writers_mu.Lock()
	defer writers_mu.Unlock()

	r, ok := writers[uri]

	if ok {
		return r, nil
	}

	r, err := writer.NewWriter(ctx, uri)

	if err != nil {
		return nil, err
	}

	writers[uri] = r
	return r, nil
}
