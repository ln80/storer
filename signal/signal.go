package signal

import (
	"context"
	"errors"
	"time"
)

var (
	ErrSignalNotFound      = errors.New("signal not found")
	ErrSendSignalFailed    = errors.New("failed to send signal(s)")
	ErrReceiveSignalFailed = errors.New("failed to receive signal(s)")
)

// Processor presents a handler that receives a signal and apply a specific treatment
// which may fails and returns error.
type Processor func(ctx context.Context, sig Signal) error

// Sender presents the service responsible for sending signals (only used for IPC).
type Sender interface {
	Send(ctx context.Context, sig Signal) error
	FlushBuffer(ctx context.Context, err *error) error
}

// Reciever presents the service responsible for recieving signals in raw format,
// and process them using the givne processor.
type Receiver interface {
	Receive(ctx context.Context, data [][]byte, p Processor) error
}

// Manager combines Sender and Receiver interfaces in a single one.
type Manager interface {
	Sender
	Receiver
}

// Monitor presents an interface for genenating signals. It must be implemented by other package/services of the systems.
// WARNING this interface may be decomposed / removed later.
type Monitor interface {
	ActiveStreams(ctx context.Context, since time.Time) ([]*ActiveStream, error)
}

// Signal presents an internal event used for Interprocess communication (IPC).
// It's used in a light pub/sub to keep internal services decoupled, and ensure an unified language.
type Signal interface {
	// SignalName returns the signal name aka ID
	SignalName() string
	// StreamID returns the concerend stream which the signal is related.
	StreamID() string
}

// BaseSignal struct implements the common part of signals
type BaseSignal struct {
	Name string
}

func (sig *BaseSignal) SignalName() string {
	return sig.Name
}

const (
	SigActiveStream = "ActiveStream"
)

type ActiveStream struct {
	*BaseSignal
	GlobalStreamID string
	Since, Until   int64
}

func (sig *ActiveStream) StreamID() string {
	return sig.GlobalStreamID
}

// ActiveStreamSignal returns ActiveStream signal
func ActiveStreamSignal(gstmID string, since, until int64) *ActiveStream {
	return &ActiveStream{
		BaseSignal: &BaseSignal{
			Name: SigActiveStream,
		},
		GlobalStreamID: gstmID,
		Since:          since,
		Until:          until,
	}
}

// NewSignal returns an empty signal based on the given name.
// It returns nil if the equivalent signal is not found.
func NewSignal(name string) Signal {
	switch name {
	case SigActiveStream:
		return &ActiveStream{}
	}
	return nil
}
