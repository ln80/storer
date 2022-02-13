package sourcing

import (
	"context"

	"github.com/redaLaanait/storer/event"
)

type Stream struct {
	events  event.Stream
	iD      event.StreamID
	version event.Version
}

func NewStream(id event.StreamID, ver event.Version, evts event.Stream) *Stream {
	return &Stream{
		iD:      id,
		version: ver,
		events:  evts,
	}
}
func Envelop(ctx context.Context, stmID event.StreamID, ver event.Version, evts []interface{}, opts ...event.EnvelopeOption) Stream {
	evtVer := ver
	envs := event.Envelop(ctx, stmID, evts, func(env event.RWEnvelope) {
		env.SetVersion(evtVer)
		evtVer = evtVer.Incr(true, true)
	})
	return Stream{
		events:  event.Stream(envs),
		version: ver,
		iD:      stmID,
	}
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
	})
}

func (s *Stream) Unwrap() event.Stream {
	return s.events
}

type Store interface {
	AppendStream(ctx context.Context, chunk Stream) error
	LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*Stream, error)
}
