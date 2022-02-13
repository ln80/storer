package event

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	StreamIDPartsDelimiter = "#"
)

var (
	ErrInvalidStream        = errors.New("invalid event stream")
	ErrAppendEventsConflict = errors.New("append events conflict")
	ErrAppendEventsFailed   = errors.New("append events failure")
)

// StreamID presents a composed event stream ID, the 1st part identifies the global stream ex: TenantID,
// While the last identify the service, bounded countext, or the root entity
type StreamID struct {
	parts []string
}

// String returns the ID of the stream
func (si StreamID) String() string {
	return strings.Join(si.parts[:], StreamIDPartsDelimiter)
}

// Parts returns the event stream ID parts
func (si StreamID) Parts() []string {
	return si.parts
}

// NewStreamID returns a composed event stream ID
func NewStreamID(id string, parts ...string) StreamID {
	si := StreamID{
		parts: append([]string{id}, parts...),
	}
	return si
}

// StreamOptions presents a functional configuration of streaming queries
type StreamOption func(f *StreamFilter)

type StreamHandler func(ctx context.Context, ev Envelope) error

type Persister interface {
	Persist(ctx context.Context, stmID StreamID, evts Stream) error
}

type Streamer interface {
	Persister
	Replay(ctx context.Context, stmID StreamID, f StreamFilter, h StreamHandler) error
}

// Filter presents an object
type StreamFilter struct {
	Since, Until time.Time
	From, To     Version
}

// Stream presents a collection of consecutive events
type Stream []Envelope

type Cursor struct {
	StmID string
	Ver   Version
	At    time.Time
}

func NewCursor(smtID string) *Cursor {
	return &Cursor{
		StmID: smtID,
	}
}

func resolveVer(env Envelope, isgstm bool) Version {
	if isgstm {
		return env.GlobalVersion()
	}
	return env.Version()
}

func resolveStmID(env Envelope, isgstm bool) string {
	if isgstm {
		return env.GlobalStreamID()
	}
	return env.StreamID()
}

type Validation struct {
	GlobalStream bool
	Filter       StreamFilter
}

func ValidateEvent(env Envelope, cur *Cursor, opts ...func(v *Validation)) (ignore bool, err error) {
	v := &Validation{}
	for _, opt := range opts {
		opt(v)
	}
	v.Filter.Build()

	if stmID := resolveStmID(env, v.GlobalStream); cur.StmID != stmID || stmID == "" {
		return false, Err(ErrInvalidStream, cur.StmID, "event stmID: "+stmID)
	}
	ver := resolveVer(env, v.GlobalStream)
	at := env.At()
	if ver.IsZero() {
		return false, Err(ErrInvalidStream, cur.StmID, "invalid evt version: "+ver.String())
	}
	if at.IsZero() || at.Equal(time.Unix(0, 0)) {
		return false, Err(ErrInvalidStream, cur.StmID, "invalid evt timstamp: "+at.String())
	}

	if ver.Before(v.Filter.From) || ver.After(v.Filter.To) || at.Before(v.Filter.Since) || at.After(v.Filter.Until) {
		return true, nil
	}

	if cur.Ver.IsZero() && ver.Equal(v.Filter.From) {
		cur.Ver = ver
	} else {
		if ver.Next(cur.Ver) {
			cur.Ver = ver
		} else {
			return false, Err(ErrInvalidStream, cur.StmID, "invalid version sequence: "+cur.Ver.String()+","+ver.String())
		}
	}

	if cur.At.IsZero() || cur.At.Equal(time.Unix(0, 0)) && at.Equal(v.Filter.Since) {
		cur.At = at
	} else {
		if at.After(cur.At) {
			cur.At = at
		} else {
			return false, Err(ErrInvalidStream, cur.StmID, "invalid timestamp sequence: "+cur.At.String()+","+at.String())
		}
	}

	return false, nil
}

// validate a chunk of stream
// filter opt will be ignored and replaced with a filter based on chunk boundaries
func (stm Stream) Validate(opts ...func(v *Validation)) error {
	var stml int
	if stml = len(stm); stml == 0 {
		return nil
	}
	cur := NewCursor(stm[0].StreamID())

	// reduce validation opts
	rv := &Validation{}
	for _, opt := range opts {
		opt(rv)
	}
	rv.Filter = StreamFilter{
		From: resolveVer(stm[0], rv.GlobalStream),
		To:   resolveVer(stm[stml-1], rv.GlobalStream),
	}
	rv.Filter.Build()
	for _, ev := range stm {
		if _, err := ValidateEvent(ev, cur, func(v *Validation) {
			v.GlobalStream = rv.GlobalStream
			v.Filter = rv.Filter
		}); err != nil {
			return err
		}

	}

	return nil
}

func (stm Stream) Empty() bool {
	return len(stm) == 0
}

func (stm Stream) EventIDs() []string {
	ids := make([]string, len(stm))
	for i, ev := range stm {
		ids[i] = ev.ID()
	}
	return ids
}

func (s Stream) Events() []interface{} {
	evts := make([]interface{}, len(s))
	for i, e := range s {
		evts[i] = e.Event()
	}
	return evts
}

func (f *StreamFilter) Build() {
	if f.Since.IsZero() {
		f.Since = time.Unix(0, 0)
	}
	if f.Until.IsZero() {
		f.Until = time.Now()
	}

	if f.To.IsZero() {
		f.To = VersionMax
	}
}
