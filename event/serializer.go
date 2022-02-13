package event

import (
	"context"
	"io"
)

type EventFormat string

const (
	EventFormatJSON    = EventFormat("JSON")
	EventFormatCSV     = EventFormat("CSV")     // not yet supported
	EventFormatParquet = EventFormat("PARQUET") // not yet supported
)

// Serializer provide a standard encoding/decoding interface for events
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
