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
	ErrCursorNotFound       = errors.New("cursor not found")
	ErrAppendEventsConflict = errors.New("append events conflict")
	ErrAppendEventsFailed   = errors.New("append events failure")
	ErrLoadEventFailed      = errors.New("load events failure")
)

// StreamID presents a composed event stream ID, the global field identifies the global stream ex: TenantID,
// While parts identify the service, bounded countext, or the root entity, etc.
type StreamID struct {
	global string
	parts  []string
}

// String returns the ID of the stream
func (si StreamID) String() string {
	return strings.Join(append([]string{si.global}, si.parts...), StreamIDPartsDelimiter)
}

// Global returns true if the given stream is a global one
// A global stream ID does not have parts
func (si StreamID) Global() bool {
	return len(si.parts) == 0
}

// GlobalID returns the first part of the streamID which represents the global stream ID, e.g TenantID
func (si StreamID) GlobalID() string {
	return si.global
}

// Parts returns the event stream ID parts others than the global ID
func (si StreamID) Parts() []string {
	return si.parts
}

// NewStreamID returns a composed event stream ID
func NewStreamID(global string, parts ...string) StreamID {
	si := StreamID{
		global: global,
		parts:  parts,
	}
	return si
}

// Store presents the transactional and strong-consistent side of the event store
// It guarantees the read consistency and may not necessarily support loading events from a global stream
type Store interface {
	Append(ctx context.Context, id StreamID, events ...Envelope) error
	Load(ctx context.Context, id StreamID, trange ...time.Time) ([]Envelope, error)
}

// Streamer mainly used to query global stream and replay events
type Streamer interface {
	Replay(ctx context.Context, stmID StreamID, f StreamFilter, h StreamHandler) error
}

// StreamOptions presents a functional configuration of streaming queries
// type StreamOption func(f *StreamFilter)

// StreamHandler process the given event in the replay stream process
type StreamHandler func(ctx context.Context, ev Envelope) error

// StreamFilter allows to filter stream based on version and/or timestamp
type StreamFilter struct {
	Since, Until time.Time
	From, To     Version
}

// Build applies filter default values if thry are empty
func (f *StreamFilter) Build() {
	if f.Since.IsZero() {
		f.Since = time.Unix(0, 0)
	}
	if f.Until.IsZero() {
		f.Until = time.Now()
	}
	if f.From.IsZero() {
		f.From = VersionMin
	}
	if f.To.IsZero() {
		f.To = VersionMax
	}
}

// Stream presents a collection of consecutive events
type Stream []Envelope

// Cursor of stream of events, mainly used to validate stream sequence
type Cursor struct {
	StmID string
	Ver   Version
	At    time.Time
}

// NewCursor returns Cursor
func NewCursor(smtID string) *Cursor {
	return &Cursor{
		StmID: smtID,
	}
}

// resolveVer returns the event's local or global version
func resolveVer(env Envelope, isgstm bool) Version {
	if isgstm {
		return env.GlobalVersion()
	}
	return env.Version()
}

// resolveVer returns the event's local or global streamID
func resolveStmID(env Envelope, isgstm bool) string {
	if isgstm {
		return env.GlobalStreamID()
	}
	return env.StreamID()
}

// Validation presents the stream validation options
type Validation struct {
	GlobalStream bool
	Filter       StreamFilter
	SkipVersion  bool
}

// ValidateEvent validates the event according to its sequence in the stream
// it returns an error, or an 'ignored' boolean flag if the event does not satisfy the filter
func ValidateEvent(env Envelope, cur *Cursor, opts ...func(v *Validation)) (ignore bool, err error) {
	if cur == nil {
		return false, ErrCursorNotFound
	}
	v := &Validation{}
	for _, opt := range opts {
		opt(v)
	}
	v.Filter.Build()

	if stmID := resolveStmID(env, v.GlobalStream); cur.StmID != stmID || stmID == "" {
		return false, Err(ErrInvalidStream, cur.StmID, "event stmID: "+stmID)
	}

	if !v.SkipVersion {
		ver := resolveVer(env, v.GlobalStream)
		if ver.IsZero() {
			return false, Err(ErrInvalidStream, cur.StmID, "invalid evt version: "+ver.String())
		}
		if ver.Before(v.Filter.From) || ver.After(v.Filter.To) {
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
	}

	at := env.At()
	if at.IsZero() || at.Equal(time.Unix(0, 0)) {
		return false, Err(ErrInvalidStream, cur.StmID, "invalid evt timstamp: "+at.String())
	}
	if at.Before(v.Filter.Since) || at.After(v.Filter.Until) {
		return true, nil
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

// Validate a chunk of stream
// Note that The Validation.filter option will be ignored and replaced with a filter based on the chunk boundaries
func (stm Stream) Validate(opts ...func(v *Validation)) error {
	var stml int
	if stml = len(stm); stml == 0 {
		return nil
	}

	// reduce validation opts to a single one
	rv := &Validation{}
	for _, opt := range opts {
		opt(rv)
	}

	cur := NewCursor(stm[0].StreamID())
	if rv.GlobalStream {
		cur = NewCursor(stm[0].GlobalStreamID())
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
			v.SkipVersion = rv.SkipVersion
		}); err != nil {
			return err
		}
	}
	return nil
}

// Empty return true if the stream chunk is empty
func (stm Stream) Empty() bool {
	return len(stm) == 0
}

// EventIDs returns the stream chunks events IDs
func (stm Stream) EventIDs() []string {
	ids := make([]string, len(stm))
	for i, ev := range stm {
		ids[i] = ev.ID()
	}
	return ids
}

// Events unwraps the event envelops and returns the domain events of the stream chunk
func (s Stream) Events() []interface{} {
	evts := make([]interface{}, len(s))
	for i, e := range s {
		evts[i] = e.Event()
	}
	return evts
}
