package json

import (
	"context"
	"reflect"
	"testing"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/internal/testutil"
)

func TestEventSerialiser(t *testing.T) {
	namespace := "service"
	ctx := context.Background()
	stmID := event.NewStreamID("tenantID")

	assertCmpEvt := func(t *testing.T, revt, evt event.Envelope, dataNotfound bool) {
		if dataNotfound {
			if data := revt.Event(); data != nil {
				t.Fatalf("expect event data be nil, got %v", data)
			}
		} else {
			if want, val := revt.Event(), evt.Event(); reflect.DeepEqual(revt, evt) {
				t.Fatalf("expect %v, %v  be equals", want, val)
			}
		}
		if want, val := revt.ID(), evt.ID(); want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.StreamID(), evt.StreamID(); want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.GlobalStreamID(), evt.GlobalStreamID(); want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.Type(), evt.Type(); want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.Version(), evt.Version(); !want.Equal(val) {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.GlobalVersion(), evt.GlobalVersion(); !want.Equal(val) {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := revt.At(), evt.At(); !want.Equal(val) {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
	}

	// test marshal/unmarshal events without being already registred
	// in this case envelope will be lazily unmarshaled:
	// 	event data (aka domain event) getter will return a nil value
	// 	the event raw data must be preserved
	// 	and marshalling the result a second time must give the same result
	ser := NewEventSerializer(namespace)
	evts := event.Envelop(ctx, stmID, testutil.GenEvts(4))
	b, err := ser.MarshalEventBatch(evts)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	revts, err := ser.UnmarshalEventBatch(b)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}

	if wantl, l := len(evts), len(revts); wantl != l {
		t.Fatalf("expect evts len %d, %d be equals", wantl, l)
	}
	for i, revt := range revts {
		assertCmpEvt(t, revt, evts[i], true)
	}

	// make sure we do not lose data even if we marshal the already unmarshaled batch
	b2, err := ser.MarshalEventBatch(revts)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, val := string(b), string(b2); !reflect.DeepEqual(want, val) {
		t.Fatalf("expect %v, %v  be equals", want, val)
	}

	// register events, so serializer can unmarshal event data
	event.NewRegister(namespace).
		Set(testutil.Event1{}).
		Set(testutil.Event2{})

	for i, revt := range revts {
		assertCmpEvt(t, revt, evts[i], false)
	}

	// TODO add impl test for Marshal/Unmarshal single event as well
}
