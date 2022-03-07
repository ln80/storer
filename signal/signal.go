package signal

import (
	"context"
	"errors"
)

var (
	ErrSignalNotFound      = errors.New("signal not found")
	ErrSendSignalFailed    = errors.New("failed to send signal(s)")
	ErrReceiveSignalFailed = errors.New("failed to receive signal(s)")
)

type Processor func(ctx context.Context, sig Signal) error

type Sender interface {
	Send(ctx context.Context, sig Signal) error
	Flush(ctx context.Context, err *error) error
}

type Receiver interface {
	Receive(ctx context.Context, data [][]byte, p Processor) error
}

type Manager interface {
	Sender
	Receiver
}

type Signal interface {
	SignalName() string
	StreamID() string
}

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

func NewSignal(name string) Signal {
	switch name {
	case SigActiveStream:
		return &ActiveStream{}
	}
	return nil
}
