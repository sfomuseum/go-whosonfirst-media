package lookup

import (
	"bytes"
	"context"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io/ioutil"
	"sync"
)

type GitLookerUpper struct {
	LookerUpper
	url string
}

func NewGitLookerUpper(ctx context.Context, url string) (LookerUpper, error) {

	l := &GitLookerUpper{
		url: url,
	}

	return l, nil
}

func (l *GitLookerUpper) Append(ctx context.Context, lu *sync.Map, append_funcs ...AppendLookupFunc) error {

	r, err := git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: l.url,
	})

	if err != nil {
		return err
	}

	it, err := r.BlobObjects()

	if err != nil {
		return err
	}

	err = it.ForEach(func(bl *object.Blob) error {

		fh, err := bl.Reader()

		if err != nil {
			return err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		for _, f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := f(ctx, lu, fh)

			if err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}
