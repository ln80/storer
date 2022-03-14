package event

import (
	"context"

	"github.com/redaLaanait/storer/event"
)

// Persister presents the service that persist chunks in the durable store e.g S3
type Persister interface {
	Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error
}
