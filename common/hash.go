package common

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os"
)

func HashFile(path string) (string, error) {

	fh, err := os.Open(path)

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
