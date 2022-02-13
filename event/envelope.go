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

	SetGlobalVersion(v Version) Envelope
}

type RWEnvelope interface {
	Envelope

	SetAt(t time.Time) Envelope
	SetVersion(v Version) Envelope
	SetUser(userID string) Envelope
}

type EnvelopeOption func(env RWEnvelope)

// Envelop wraps and apply options to the given events
func Envelop(ctx context.Context, stmID StreamID, evts []interface{}, opts ...EnvelopeOption) []Envelope {
	envs := make([]Envelope, len(evts))
	for i, evt := range evts {
		env := &envelope{
			streamID:       stmID.String(),
			event:          evt,
			eType:          TypeOfWithContext(ctx, evt),
			eID:            UID().String(),
			at:             time.Now(),
			globalStreamID: stmID.Parts()[0], // by default the first part of the stream ID presents ti global one
		}
		// env := EnvelopEvent(ctx, stmID, UID().String(), evt, opts...)
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
	streamID string
	eID      string
	eType    string
	event    interface{}

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

// func (e *envelope) SetID(iD string) Envelope {
// 	e.eID = iD
// 	return e
// }

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

func (e *envelope) SetAt(t time.Time) Envelope {
	e.at = t
	return e
}

// Version implements the Version method of the Envelope interface.
func (e *envelope) Version() Version {
	return e.version
}

func (e *envelope) SetVersion(v Version) Envelope {
	e.version = v
	return e
}

func (e *envelope) User() string {
	return e.user
}

func (e *envelope) SetUser(userID string) Envelope {
	e.user = userID
	return e
}

func (e *envelope) StreamID() string {
	return e.streamID
}

// GlobalStreamID implements the GlobalStreamID method of the Envelope interface.
func (e *envelope) GlobalStreamID() string {
	return e.globalStreamID
}

// func (e *envelope) SetGlobalStreamID(gstmID string) Envelope {
// 	e.globalStreamID = gstmID
// 	return e
// }

// GlobalVersion implements the GlobalVersion method of the Envelop interface
func (e *envelope) GlobalVersion() Version {
	return e.globalVersion
}

func (e *envelope) SetGlobalVersion(v Version) Envelope {
	e.globalVersion = v
	return e
}
