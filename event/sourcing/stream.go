package sourcing

import (
	"context"

	"github.com/redaLaanait/storer/event"
)

type Store interface {
	AppendToStream(ctx context.Context, chunk Stream) error
	LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*Stream, error)
}

func Envelop(ctx context.Context, stmID event.StreamID, curVer event.Version, evts []interface{}) Stream {
	stm := NewStream(stmID, event.Stream(
		event.Envelop(ctx, stmID, evts, event.WithVersionIncr(curVer.Incr(), event.VersionSeqDiffFrac10p1)),
	))
	if err := stm.Validate(); err != nil {
		panic(err)
	}
	return *stm
}

type Stream struct {
	events  event.Stream
	iD      event.StreamID
	version event.Version
}

func NewStream(id event.StreamID, evts event.Stream) *Stream {
	stm := &Stream{
		iD:      id,
		version: event.VersionZero,
		events:  evts,
	}
	if l := len(evts); l > 0 {
		stm.version = evts[l-1].Version()
	}
	return stm
}

func (s *Stream) Version() event.Version {
	return s.version
}

func (s *Stream) ID() event.StreamID {
	return s.iD
}

func (s *Stream) Empty() bool {
	return s.events.Empty()
}

func (s *Stream) Validate() error {
	return s.events.Validate(func(v *event.Validation) {
		v.GlobalStream = false
		v.SkipVersion = false
	})
}

func (s *Stream) Unwrap() event.Stream {
	return s.events
}
