package event

import (
	"context"
	"errors"
	"io"
)

type EventFormat string

const (
	EventFormatJSON = EventFormat("JSON")
)

var (
	ErrMarshalEventFailed   = errors.New("marshal event(s) failed")
	ErrMarshalEmptyEvent    = errors.New("event to marshal is empty")
	ErrUnmarshalEventFailed = errors.New("unmarshal event(s) failed")
)

// Serializer provides a standard encoding/decoding interface for events
type Serializer interface {
	// EventFormat returns serializer supported format
	EventFormat() EventFormat

	// MarshalEvent returns a binary version of the event and its size according to the supported format.
	// It fails if chunks is empty.
	MarshalEvent(event Envelope) ([]byte, int, error)

	// MarshalEventBatch returns a binary version of the chunks of events. It also returns a slice of events' size.
	// It fails if chunks is empty.
	MarshalEventBatch(event []Envelope) ([]byte, []int, error)

	// UnmarshalEvent returns an event envelope based on the binary/raw given event.
	// The returned envelope may not contain data (aka original event) if event type if not found in the event registry.
	UnmarshalEvent(b []byte) (Envelope, error)

	// UnmarshalEvent returns a slice of envelopes based on the bynary/raw given chunks of events.
	// Similar to UnmarshalEvent, events may be lazily unmarshaled, if event types are not found in registery.
	UnmarshalEventBatch(b []byte) ([]Envelope, error)

	// ContentType returns the equivalent MIME type of the serialized events ex: application/json
	ContentType() string

	// FileExt returns the extension to append to file containing the events ex: .json
	FileExt() string

	// Concat recieves chunk of raw events form the given channel and merge them into a single binary
	Concat(ctx context.Context, l int, chunks chan []byte, flush func(b []byte) error) error
	// ConcatSlice(chunks [][]byte) ([]byte, error)

	// Decode chunk of events from a reader and send unmarshaled events to a channel.
	Decode(ctx context.Context, r io.Reader, ch chan<- Envelope) error
}
