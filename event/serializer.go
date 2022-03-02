package event

import (
	"context"
	"errors"
	"io"
)

type EventFormat string

const (
	EventFormatJSON    = EventFormat("JSON")
	EventFormatCSV     = EventFormat("CSV")     // not yet supported
	EventFormatParquet = EventFormat("PARQUET") // not yet supported
	EventFormatPBuffer = EventFormat("PBuffer") // not yet supported
)

var (
	ErrMarshalEventFailed   = errors.New("marshal event(s) failed")
	ErrMarshalEmptyEvent    = errors.New("event to marshal is empty")
	ErrUnmarshalEventFailed = errors.New("unmarshal event(s) failed")
)

// Serializer provides a standard encoding/decoding interface for events chunks
type Serializer interface {
	EventFormat() EventFormat
	MarshalEvent(event Envelope) ([]byte, error)
	MarshalEventBatch(event []Envelope) ([]byte, error)
	UnmarshalEvent(b []byte) (Envelope, error)
	UnmarshalEventBatch(b []byte) ([]Envelope, error)
	ContentType() string
	FileExt() string
	Concat(ctx context.Context, l int, chunks chan []byte, flush func(b []byte) error) error
	ConcatSlice(chunks [][]byte) ([]byte, error)
	Decode(ctx context.Context, r io.Reader, ch chan<- Envelope) error
}
