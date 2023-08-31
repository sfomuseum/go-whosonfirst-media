package common

import (
	"context"
	"fmt"
	
	"gocloud.dev/blob"
	"github.com/sfomuseum/go-text-emboss"
)

func ExtractText(ctx context.Context, e emboss.Embosser, bucket *blob.Bucket, path string) ([]byte, error) {

	r, err := bucket.NewReader(ctx, path, nil)

	if err != nil {
		return nil, fmt.Errorf("Failed to open %s for reading, %w", path, err)
	}

	defer r.Close()

	return e.EmbossTextWithReader(ctx, path, r)
}
