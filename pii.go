package storer

import (
	"context"
	"time"

	"github.com/ln80/pii"
	"github.com/ln80/storer/event"
	"github.com/ln80/storer/event/sourcing"
)

// ProtectPII returns wraps the given evetn store to ensure PII protection.
// It ensures client-side personal data encryption/decryption.
func ProtectPII(store EventStore, f pii.Factory) EventStore {
	return &piiProtectorWrapper{
		encryptor: f,
		store:     store,
	}
}

type piiProtectorWrapper struct {
	encryptor pii.Factory
	store     EventStore
}

// Append implements EventStore
func (s *piiProtectorWrapper) Append(ctx context.Context, id event.StreamID, events ...event.Envelope) error {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...interface{}) error {
		return p.Encrypt(ctx, ptrs...)
	}

	if err := event.Transform(ctx, events, fn); err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	return s.store.Append(ctx, id, events...)
}

// Load implements EventStore
func (s *piiProtectorWrapper) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...interface{}) error {
		return p.Decrypt(ctx, ptrs...)
	}

	events, err := s.store.Load(ctx, id, trange...)
	if err != nil {
		return nil, err
	}
	if err := event.Transform(ctx, events, fn); err != nil {
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	return events, nil
}

// Replay implements EventStore
func (s *piiProtectorWrapper) Replay(ctx context.Context, stmID event.StreamID, f event.StreamFilter, h event.StreamHandler) error {
	p, _ := s.encryptor.Instance(stmID.GlobalID())
	fn := func(ctx context.Context, ptrs ...interface{}) error {
		return p.Decrypt(ctx, ptrs...)
	}

	ph := func(ctx context.Context, ev event.Envelope) error {
		events := make([]event.Envelope, 1)
		events[0] = ev

		event.Transform(ctx, events, fn)
		return h(ctx, events[0])
	}

	return s.store.Replay(ctx, stmID, f, ph)
}

// AppendToStream implements EventStore
func (s *piiProtectorWrapper) AppendToStream(ctx context.Context, chunk sourcing.Stream) error {
	p, _ := s.encryptor.Instance(chunk.ID().GlobalID())
	fn := func(ctx context.Context, ptrs ...interface{}) error {
		return p.Encrypt(ctx, ptrs...)
	}

	if err := event.Transform(ctx, chunk.Unwrap(), fn); err != nil {
		return event.Err(event.ErrAppendEventsFailed, chunk.ID().GlobalID(), err)
	}

	return s.store.AppendToStream(ctx, chunk)
}

// LoadStream implements EventStore
func (s *piiProtectorWrapper) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...interface{}) error {
		return p.Decrypt(ctx, ptrs...)
	}

	stm, err := s.store.LoadStream(ctx, id, vrange...)
	if err != nil {
		return nil, err
	}
	if err := event.Transform(ctx, stm.Unwrap(), fn); err != nil {
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	return stm, nil
}

var _ EventStore = &piiProtectorWrapper{}
