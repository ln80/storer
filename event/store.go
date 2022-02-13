package event

import (
	"context"
	"time"
)

type Store interface {
	Append(ctx context.Context, id StreamID, events ...Envelope) error
	Load(ctx context.Context, id StreamID, trange ...time.Time) ([]Envelope, error)
}
