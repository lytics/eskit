package streams

import (
	"context"
	"os"
	"time"

	"github.com/lytics/eskit"
)

type StdIO struct {
	docsread, docswritten int
}

func (s *StdIO) Open(_ map[string]interface{}) ([]*eskit.Doc, error) {

	return nil, nil
}

func (s *StdIO) Write(ctx context.Context, docs chan eskit.Doc) chan error {
	errs := make(chan error)

	go func() {
		outf := os.Stdout
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-docs:
				//d.Source = append(d.Source, '\n')
				_, err := outf.Write(d.Source)
				if err != nil {
					errs <- err
					time.Sleep(50 * time.Millisecond)
				} else {
					s.docswritten++
				}
			}
		}
	}()

	return errs
}
