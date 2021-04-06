package common

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"gocloud.dev/blob"
	"io"
)

// Generate a SHA-1 hash of a file stored in a blob.Bucket instance.
func FingerprintFile(ctx context.Context, bucket *blob.Bucket, path string) (string, error) {

	fh, err := bucket.NewReader(ctx, path, nil)

	if err != nil {
		return "", err
	}

	defer fh.Close()

	// h := sha256.New()
	h := sha1.New()

	_, err = io.Copy(h, fh)

	if err != nil {
		return "", err
	}

	hash := h.Sum(nil)
	str := hex.EncodeToString(hash[:])

	return str, nil
}
