package json

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/redaLaanait/storer/event"
)

type Event struct {
	FGlobalStreamID string        `json:"GStreamID"`
	FGlobalVersion  string        `json:"GVersion"`
	FStreamID       string        `json:"StreamID"`
	FVersion        string        `json:"Version"`
	fVersion        event.Version `json:"-"`

	FID    string          `json:"ID"`
	FType  string          `json:"Type"`
	FRaw   json.RawMessage `json:"Data"`
	fEvent interface{}     `json:"-"`
	FAt    int64           `json:"At"`

	fGlobalVersion event.Version `json:"-"`

	FUser string `json:"User"`
}

var _ event.Envelope = &Event{}

// var eventRegistry = event.NewRegister("")

func (e *Event) StreamID() string {
	return e.FStreamID
}

func (e *Event) ID() string {
	return e.FID
}

func (e *Event) Type() string {
	return e.FType
}

func (e *Event) Event() interface{} {
	return e.fEvent
}

func (e *Event) SetEvent(evt interface{}) event.Envelope {
	e.fEvent = evt

	return e
}

func (e *Event) At() time.Time {
	return time.Unix(0, e.FAt)
}

func (e *Event) SetAt(t time.Time) event.Envelope {
	e.FAt = t.UnixNano()
	return e
}

func (e *Event) Version() event.Version {
	if !e.fVersion.IsZero() {
		return e.fVersion
	}
	if e.FVersion != "" {
		e.fVersion, _ = event.Ver(e.FVersion)
	}
	return e.fVersion
}

// func (e *Event) SetVersion(v event.Version) event.Envelope {
// 	if v.IsZero() {
// 		e.FVersion = ""
// 	} else {
// 		e.FVersion = v.String()
// 	}

// 	return e
// }

func (e *Event) User() string {
	return e.FUser
}

// func (e *Event) SetUser(userID string) event.Envelope {
// 	e.FUser = userID
// 	return e
// }

func (e *Event) GlobalStreamID() string {
	return e.FGlobalStreamID
}

// func (e *Event) SetGlobalStreamID(gstmID string) event.Envelope {
// 	e.FGlobalStreamID = gstmID
// 	return e
// }

func (e *Event) GlobalVersion() event.Version {
	if !e.fGlobalVersion.IsZero() {
		return e.fGlobalVersion
	}
	if e.FGlobalVersion != "" {
		e.fGlobalVersion, _ = event.Ver(e.FGlobalVersion)
	}
	return e.fGlobalVersion
}

func (e *Event) SetGlobalVersion(v event.Version) event.Envelope {
	e.fGlobalVersion = v
	return e
}

type eventSerializer struct {
	eventRegistry event.Register
}

func NewEventSerializer(context string) event.Serializer {
	return &eventSerializer{
		eventRegistry: event.NewRegister(context),
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

func (s *eventSerializer) MarshalEvent(evt event.Envelope) ([]byte, error) {
	data, err := json.Marshal(evt.Event())
	if err != nil {
		return nil, err
	}

	// log.Println("debug marshal events", evt)
	b, err := json.Marshal(Event{
		FStreamID:       evt.StreamID(),
		FID:             evt.ID(),
		FType:           evt.Type(),
		FRaw:            json.RawMessage(data),
		FAt:             evt.At().UnixNano(),
		FUser:           evt.User(),
		FVersion:        evt.Version().String(),
		FGlobalStreamID: evt.GlobalStreamID(),
		FGlobalVersion:  evt.GlobalVersion().String(),
	})
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *eventSerializer) MarshalEventBatch(evts []event.Envelope) ([]byte, error) {
	jsonEvts := make([]Event, len(evts))
	for i, evt := range evts {
		data, err := json.Marshal(evt.Event())
		if err != nil {
			return nil, err
		}
		jsonEvts[i] = Event{
			FStreamID:       evt.StreamID(),
			FID:             evt.ID(),
			FType:           evt.Type(),
			FRaw:            json.RawMessage(data),
			FAt:             evt.At().UnixNano(),
			FUser:           evt.User(),
			FVersion:        evt.Version().String(),
			FGlobalStreamID: evt.GlobalStreamID(),
			FGlobalVersion:  evt.GlobalVersion().String(),
		}
	}

	b, err := json.Marshal(jsonEvts)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *eventSerializer) UnmarshalEvent(b []byte) (event.Envelope, error) {
	jsonEv := Event{}
	if err := json.Unmarshal(b, &jsonEv); err != nil {
		return nil, err
	}

	evt, err := s.eventRegistry.Get(jsonEv.Type())
	if err != nil {
		return nil, fmt.Errorf("resolve event type faild %w", err)
		// panic(fmt.Errorf("unmarshal event faild %w", err))
	}
	if err := json.Unmarshal(jsonEv.FRaw, evt); err != nil {
		return nil, fmt.Errorf("unmarshal event faild %w", err)
		// panic(fmt.Errorf("unmarshal event faild %w", err))
	}

	// return event.Envelop(context.Background(), event.NewStreamID(jsonEv.GlobalStreamID()), []interface{}{ev}, func(env event.Envelope) {
	// 		env.
	// 			SetAt(jsonEv.At())

	// 	})[0],
	// 	nil

	jsonEv.SetEvent(evt)

	return &jsonEv, nil

}

func (s *eventSerializer) Decode(ctx context.Context, r io.Reader, ch chan<- event.Envelope) error {
	dec := json.NewDecoder(r)
	for loop := true; loop; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var evt Event
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

func (s *eventSerializer) UnmarshalEventBatch(b []byte) ([]event.Envelope, error) {
	jsonEvs := []Event{}
	if err := json.Unmarshal(b, &jsonEvs); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(jsonEvs))
	for i, jsonEv := range jsonEvs {
		jsonEv := jsonEv

		evt, err := s.eventRegistry.Get(jsonEv.Type())
		if err != nil {
			return nil, fmt.Errorf("resolve event type failed %w", err)
			// panic(fmt.Errorf("unmarshal event faild %w", err))
		}
		if err := json.Unmarshal(jsonEv.FRaw, evt); err != nil {
			return nil, fmt.Errorf("unmarshal event faild %w", err)
			// panic(fmt.Errorf("unmarshal event faild %w", err))
		}
		// evts[i] = evt

		// envs[i] = event.EnvelopEvent(context.Background(), event.NewStreamID(jsonEv.GlobalStreamID()), jsonEv.ID(), evt,
		// 	func(env event.RWEnvelope) {
		// 		env.
		// 			SetAt(jsonEv.At())
		// 	},
		// )
		jsonEv.SetEvent(evt)

		envs[i] = &jsonEv

	}

	// return event.Envelop(context.Background(), event.NewStreamID(jsonEvs[0].GlobalStreamID()), evts, func(env event.Envelope) {
	// 	env.
	// 		SetAt(jsonEvs[0].At()).
	// 		SetAt(env.ID)
	// }), nil

	// envs := make([]event.Envelope, len(jsonEvs))
	// for i, jsonEv := range jsonEvs {
	// 	envs[i] = &jsonEv
	// }

	return envs, nil
}

func (s *eventSerializer) ConcatSlice(chunks [][]byte) ([]byte, error) {
	chunksLen := len(chunks)
	if chunksLen == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.WriteString("[")
	for i, chunk := range chunks {
		fmtChunk := strings.TrimSpace(string(chunk))
		fmtChunk = fmtChunk[1 : len(fmtChunk)-1] // remove square brackets
		if _, err := buf.Write([]byte(fmtChunk)); err != nil {
			return nil, err
		}
		if i != chunksLen-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]")

	return buf.Bytes(), nil
}

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
