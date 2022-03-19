package json

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/redaLaanait/storer/event"
)

// convertEvent takes an event.Envelope an convert it to a jsonEvent type.
// It skips Versions zero values and replaces them with empty strings,
// this make the json more compact and small.
func convertEvent(evt event.Envelope) (to *jsonEvent, err error) {
	var data []byte
	data, err = json.Marshal(evt.Event())
	if err != nil {
		return
	}
	to = &jsonEvent{
		FStreamID: evt.StreamID(),
		FID:       evt.ID(),
		FType:     evt.Type(),
		FRawEvent: json.RawMessage(data),
		FAt:       evt.At().UnixNano(),
		FUser:     evt.User(),
		FDests:    evt.Dests(),
	}
	if !evt.Version().Equal(event.VersionZero) {
		to.fVersion = evt.Version()
		to.FRawVersion = to.fVersion.String()
	}
	if !evt.GlobalVersion().Equal(event.VersionZero) {
		to.fGlobalVersion = evt.GlobalVersion()
		to.FRawGlobalVersion = to.fGlobalVersion.String()
	}
	return
}

// eventSerializer implements event.Serializer interface.
// it uses json serialization, and it's based on event registry
// to unmarshal envelop data aka domain event.
type eventSerializer struct {
	eventRegistry event.Register
}

// NewEventSerializer returns a json event serializer
func NewEventSerializer(namespace string) event.Serializer {
	return &eventSerializer{
		eventRegistry: event.NewRegister(namespace),
	}
}

var _ event.Serializer = &eventSerializer{}

func (s *eventSerializer) EventFormat() event.EventFormat {
	return event.EventFormatJSON
}

func (s *eventSerializer) ContentType() string {
	return "application/json"
}

func (s *eventSerializer) FileExt() string {
	return "json"
}

func (s *eventSerializer) MarshalEvent(evt event.Envelope) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = event.Err(event.ErrMarshalEventFailed, evt.StreamID(), err)
		}
	}()
	if evt == nil {
		err = event.ErrMarshalEmptyEvent
		return
	}
	if jsonEvt, ok := evt.(*jsonEvent); ok {
		b, err = json.Marshal(jsonEvt)
	} else {
		jsonEvt, err = convertEvent(evt)
		if err != nil {
			return nil, err
		}
		b, err = json.Marshal(jsonEvt)
	}
	return
}

func (s *eventSerializer) MarshalEventBatch(evts []event.Envelope) (b []byte, err error) {
	defer func() {
		if err != nil {
			if len(evts) > 0 {
				err = event.Err(event.ErrMarshalEventFailed, evts[0].StreamID(), err)
			} else {
				err = event.Err(event.ErrMarshalEventFailed, "", err)
			}
		}
	}()
	jsonEvts := make([]jsonEvent, len(evts))
	for i, evt := range evts {
		if jsonEvt, ok := evt.(*jsonEvent); ok {
			jsonEvts[i] = *jsonEvt
		} else {
			jsonEvt, err = convertEvent(evt)
			if err != nil {
				return nil, err
			}
			jsonEvts[i] = *jsonEvt
		}
	}
	b, err = json.Marshal(jsonEvts)
	return
}

func (s *eventSerializer) UnmarshalEvent(b []byte) (event.Envelope, error) {
	jsonEvt := jsonEvent{
		reg: s.eventRegistry,
	}
	if err := json.Unmarshal(b, &jsonEvt); err != nil {
		return nil, err
	}
	return &jsonEvt, nil
}

func (s *eventSerializer) UnmarshalEventBatch(b []byte) ([]event.Envelope, error) {
	jsonEvts := []jsonEvent{}
	if err := json.Unmarshal(b, &jsonEvts); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(jsonEvts))
	for i, jsonEvt := range jsonEvts {
		jsonEvt := jsonEvt
		jsonEvt.reg = s.eventRegistry
		envs[i] = &jsonEvt
	}
	return envs, nil
}

func (s *eventSerializer) Decode(ctx context.Context, r io.Reader, ch chan<- event.Envelope) error {
	dec := json.NewDecoder(r)
	for loop := true; loop; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var evt jsonEvent
			if err := dec.Decode(&evt); err == io.EOF {
				loop = false
				break
			} else if err != nil {
				return err
			}
			ch <- &evt
		}
	}
	return nil
}

// func (s *eventSerializer) ConcatSlice(chunks [][]byte) ([]byte, error) {
// 	chunksLen := len(chunks)
// 	if chunksLen == 0 {
// 		return nil, nil
// 	}

// 	var buf bytes.Buffer
// 	buf.WriteString("[")
// 	for i, chunk := range chunks {
// 		fmtChunk := strings.TrimSpace(string(chunk))
// 		fmtChunk = fmtChunk[1 : len(fmtChunk)-1] // remove square brackets
// 		if _, err := buf.Write([]byte(fmtChunk)); err != nil {
// 			return nil, err
// 		}
// 		if i != chunksLen-1 {
// 			buf.WriteString(",")
// 		}
// 	}
// 	buf.WriteString("]")

// 	return buf.Bytes(), nil
// }

func (s *eventSerializer) Concat(ctx context.Context, chunksLen int, chunks chan []byte, flush func(b []byte) error) error {
	if chunksLen == 0 {
		return nil
	}

	var buf bytes.Buffer
	buf.WriteString("[")
	i := 0
	for loop := true; loop; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				loop = false
				break
			}
			fmtChunk := strings.TrimSpace(string(chunk))
			fmtChunk = fmtChunk[1 : len(fmtChunk)-1] // remove square brackets from chunk json
			if _, err := buf.Write([]byte(fmtChunk)); err != nil {
				return err
			}
			if i != chunksLen-1 {
				buf.WriteString(",")
			}
			i++

			if i%10 == 0 {
				if err := flush(buf.Bytes()); err != nil {
					return err
				}

				buf.Reset()
			}
		}
	}
	buf.WriteString("]")

	if err := flush(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

type jsonEvent struct {
	FStreamID         string          `json:"StmID"`
	fGlobalStreamID   string          `json:"-"`
	FRawGlobalVersion string          `json:"GVer,omitempty"`
	fGlobalVersion    event.Version   `json:"-"`
	FRawVersion       string          `json:"Ver,omitempty"`
	fVersion          event.Version   `json:"-"`
	FID               string          `json:"ID"`
	FType             string          `json:"Type"`
	FRawEvent         json.RawMessage `json:"Data"`
	fEvent            interface{}     `json:"-"`
	FAt               int64           `json:"At"`
	FUser             string          `json:"User,omitempty"`
	FDests            []string        `json:"Dests,omitempty"`
	reg               event.Register  `json:"-"`
}

var _ event.Envelope = &jsonEvent{}

func (e *jsonEvent) StreamID() string {
	return e.FStreamID
}

func (e *jsonEvent) ID() string {
	return e.FID
}

func (e *jsonEvent) Type() string {
	return e.FType
}

// Event return the orignal event wraped in json event envelope.
// It may returns nil if the event type is not found in the event registry,
// or the later is not configured in the event.
// It's up to the client/caller code to tolerate (or not) the enmpty value.
func (e *jsonEvent) Event() interface{} {
	if e.fEvent != nil {
		return e.fEvent
	}
	if e.reg == nil {
		return nil
	}
	evt, err := e.reg.Get(e.Type())
	if err != nil {
		log.Println(event.Err(err, e.StreamID()))
		return nil
	}
	if err := json.Unmarshal(e.FRawEvent, evt); err != nil {
		log.Println(event.Err(fmt.Errorf("unmarshal event data failed"), e.StreamID(), err))
		return nil
	}
	e.fEvent = evt

	return e.fEvent
}

func (e *jsonEvent) At() time.Time {
	return time.Unix(0, e.FAt)
}

func (e *jsonEvent) Version() event.Version {
	// return event version if already set in the event
	if !e.fVersion.IsZero() {
		return e.fVersion
	}
	// if not then resolve the version value from the raw value
	// in case the later is not empty
	if e.FRawVersion != "" {
		e.fVersion, _ = event.Ver(e.FRawVersion)
	}
	// return zero value as a fallback
	return e.fVersion
}

func (e *jsonEvent) User() string {
	return e.FUser
}

func (e *jsonEvent) GlobalStreamID() string {
	// the global stream ID not yet set in the event then
	// parse the stream value and get global ID part.
	if e.fGlobalStreamID == "" {
		stmID, _ := event.ParseStreamID(e.FStreamID)
		e.fGlobalStreamID = stmID.GlobalID()
	}
	return e.fGlobalStreamID
}

func (e *jsonEvent) GlobalVersion() event.Version {
	// return the global version if already set in the event
	if !e.fGlobalVersion.IsZero() {
		return e.fGlobalVersion
	}
	// resolve the global stream version from the raw value if the later is not empty
	if e.FRawGlobalVersion != "" {
		e.fGlobalVersion, _ = event.Ver(e.FRawGlobalVersion)
	}
	// otherwise, return  zero value
	return e.fGlobalVersion
}

func (e *jsonEvent) Dests() []string {
	return e.FDests
}

// SetGlobalVersion set the global version values. It's mainly used by the event forwarder, linked to DB stream.
// FYI the forwarder enriches the recieved events from the DB stream by setting the global version,
// and send them a permanent store e.g S3.
func (e *jsonEvent) SetGlobalVersion(v event.Version) event.Envelope {
	e.fGlobalVersion = v
	e.FRawGlobalVersion = e.fGlobalVersion.String()
	return e
}
