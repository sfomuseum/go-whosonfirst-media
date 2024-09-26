package common

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"

	"gocloud.dev/blob"
)

// Generate a SHA-1 hash of a file stored in a blob.Bucket instance.
func FingerprintFile(ctx context.Context, bucket *blob.Bucket, path string) (string, error) {

	r, err := bucket.NewReader(ctx, path, nil)

	if err != nil {
		return "", fmt.Errorf("Failed to create new reader, %w", err)
	}

	defer r.Close()

	h := sha1.New()

	_, err = io.Copy(h, r)

	if err != nil {
		return "", fmt.Errorf("Failed to copy body to hash, %w", err)
	}

	hash := h.Sum(nil)
	str := hex.EncodeToString(hash[:])

	return str, nil
}
