package lookup

import (
	"context"
	"sync"
)

type LookerUpper interface {
	Append(context.Context, *sync.Map, ...AppendLookupFunc) error
}

func NewLookupMap(ctx context.Context, looker_uppers []LookerUpper, append_funcs []AppendLookupFunc) (*sync.Map, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lu := new(sync.Map)

	done_ch := make(chan bool)
	err_ch := make(chan error)

	remaining := len(looker_uppers)

	for _, l := range looker_uppers {

		go func(l LookerUpper) {

			err := l.Append(ctx, lu, append_funcs...)

			if err != nil {
				err_ch <- err
			}

			done_ch <- true

		}(l)
	}

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}
	}

	return lu, nil
}
