package lookup

import (
	"bytes"
	"context"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io/ioutil"
	"log"
	"sync"
)

type GitLookerUpper struct {
	LookerUpper
	uri string
}

func NewGitLookerUpper(ctx context.Context) LookerUpper {

	l := &GitLookerUpper{}
	return l
}

func (l *GitLookerUpper) Open(ctx context.Context, uri string) error {
	l.uri = uri
	return nil
}

func (l *GitLookerUpper) Append(ctx context.Context, lu *sync.Map, append_funcs ...AppendLookupFunc) error {

	r, err := git.Clone(memory.NewStorage(), nil, &git.CloneOptions{
		URL: l.uri,
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
				log.Printf("GIT %s: %s\n", bl.Hash, err)
				// return err
			}
		}

		return nil
	})

	return err
}
