package event

import (
	"context"

	"github.com/redaLaanait/storer/event"
)

type Persister interface {
	Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error
}
