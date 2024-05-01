package common

import (
	"context"
	"fmt"

	"github.com/sfomuseum/go-text-emboss"
	"gocloud.dev/blob"
)

func ExtractText(ctx context.Context, e emboss.Embosser, bucket *blob.Bucket, path string) ([]byte, error) {

	r, err := bucket.NewReader(ctx, path, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to open %s for reading, %w", path, err)
	}

	defer r.Close()

	rsp, err := e.EmbossTextWithReader(ctx, path, r)

	if err != nil {
		return nil, fmt.Errorf("Failed to emboss text for %s, %w", path, err)
	}

	return []byte(rsp.Text), nil
}
