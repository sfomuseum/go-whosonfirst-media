package common

import (
	"context"
	"fmt"

	"github.com/sfomuseum/go-text-emboss/v2"
	"gocloud.dev/blob"
)

// ExtractText will return the text contained in the body of 'path' derived using 'e'.
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
