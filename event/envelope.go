package event

import (
	"context"
	"time"
)

//Envelope wraps and adds meta-data to events such us timestamp, stream ID, version
type Envelope interface {
	ID() string
	Type() string
	Event() interface{}
	At() time.Time

	StreamID() string
	Version() Version
	GlobalStreamID() string
	GlobalVersion() Version
	User() string
}

type RWEnvelope interface {
	Envelope

	SetAt(t time.Time) Envelope
	SetUser(userID string) Envelope
	SetVersion(v Version) Envelope
	SetGlobalVersion(v Version) Envelope
}

type EnvelopeOption func(env RWEnvelope)

func WithVersionIncr(startingVer Version, diff VersionSequenceDiff) EnvelopeOption {
	return func(env RWEnvelope) {
		env.SetVersion(startingVer)
		switch diff {
		case VersionSeqDiff10p0:
			startingVer = startingVer.doIncr(VersionSeqDiff10p0)
		case VersionSeqDiffFrac10p0:
			startingVer = startingVer.doIncr(VersionSeqDiffFrac10p0)
		case VersionSeqDiffFrac10p1:
			startingVer = startingVer.doIncr(VersionSeqDiffFrac10p1)
		}
	}
}

// Envelop wraps with options the given events
// by default:
// 		it creates a valid timestamp-based stream chunk
//		it does not set event's version aka sequence
func Envelop(ctx context.Context, stmID StreamID, evts []interface{}, opts ...EnvelopeOption) []Envelope {
	envs := make([]Envelope, len(evts))
	for i, evt := range evts {
		env := &envelope{
			globalStreamID: stmID.GlobalID(),
			streamID:       stmID.String(),
			event:          evt,
			eType:          TypeOfWithContext(ctx, evt),
			eID:            UID().String(),
			at:             time.Now().UTC(),
		}
		if ctx.Value(ContextUserKey) != nil {
			user := ctx.Value(ContextUserKey).(string)
			env.SetUser(user)
		}
		for _, opt := range opts {
			opt(env)
		}
		envs[i] = env
	}
	return envs
}

type envelope struct {
	streamID       string
	eID            string
	eType          string
	event          interface{}
	at             time.Time
	version        Version
	user           string
	globalStreamID string
	globalVersion  Version
}

var _ Envelope = &envelope{}
var _ RWEnvelope = &envelope{}

// ID implements the EventID method of the Envelope interface
func (e *envelope) ID() string {
	return e.eID
}

// Type implements the EventType method of the Envelope interface.
func (e *envelope) Type() string {
	return e.eType
}

// Event implements the Event method of the envelope interface.
func (e *envelope) Event() interface{} {
	return e.event
}

// At implements the Timestamp method of the Envelope interface.
func (e *envelope) At() time.Time {
	return e.at
}

// Version implements the Version method of the Envelope interface.
func (e *envelope) Version() Version {
	return e.version
}

// User implements the User method of the Envelope interface.
func (e *envelope) User() string {
	return e.user
}

// StreamID implements the StreamID method of the Envelope interface.
func (e *envelope) StreamID() string {
	return e.streamID
}

// GlobalStreamID implements the GlobalStreamID method of the Envelope interface.
func (e *envelope) GlobalStreamID() string {
	return e.globalStreamID
}

// GlobalVersion implements the GlobalVersion method of the Envelop interface
func (e *envelope) GlobalVersion() Version {
	return e.globalVersion
}

// SetAt implements the SetAt method of the RWEnvelope interface.
func (e *envelope) SetAt(t time.Time) Envelope {
	e.at = t
	return e
}

// SetUser implements the SetUser method of the RWEnvelope interface.
func (e *envelope) SetUser(userID string) Envelope {
	e.user = userID
	return e
}

// SetVersion implements the SetVersion method of the RWEnvelope interface.
func (e *envelope) SetVersion(v Version) Envelope {
	e.version = v
	return e
}

// SetGlobalVersion implements the SetGlobalVersion method of the RWEnvelope interface.
func (e *envelope) SetGlobalVersion(v Version) Envelope {
	e.globalVersion = v
	return e
}
