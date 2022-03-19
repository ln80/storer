package json

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"testing"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/testutil"
)

func assertCmpEvt(t *testing.T, revt, evt event.Envelope, dataNotfound bool) {
	if dataNotfound {
		if data := revt.Event(); data != nil {
			t.Fatalf("expect event data be nil, got %v", data)
		}
	} else {
		if want, val := revt.Event(), evt.Event(); !reflect.DeepEqual(want, val) {
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

func TestConvertEvent(t *testing.T) {
	ctx := context.Background()
	stmID := event.NewStreamID("tenantID")

	reg := testutil.RegisterEvent("")

	t.Run("convert a light envelope to json Event", func(t *testing.T) {
		// prepare a light envelope (without event versions & user)
		evt := event.Envelop(ctx, stmID, testutil.GenEvts(1))[0]

		jsonEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// make sure that events are equals except data value
		// which must be nil in the json event
		assertCmpEvt(t, jsonEvt, evt, true)

		// add event register to the json event
		jsonEvt.reg = reg

		// make sure that events are fully equals
		assertCmpEvt(t, jsonEvt, evt, false)

		// make sure that raw versions & user values are empty
		// ex: no need to have "00000000000000000000.0000000000" values in serialized json
		if want, val := "", jsonEvt.FRawVersion; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := "", jsonEvt.FRawGlobalVersion; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := "", jsonEvt.FUser; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}

		// make sure json event remains valid in case of marshal/unmarshal
		// and without lost of infos
		b, err := json.Marshal(jsonEvt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		log.Println(string(b))
		jsonEvt = &jsonEvent{
			reg: reg,
		}
		if err := json.Unmarshal(b, jsonEvt); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		// compare the new json event with the orignal envelope
		assertCmpEvt(t, jsonEvt, evt, false)

		jsonEvt.reg = reg

		// make sure that events are fully equals
		assertCmpEvt(t, jsonEvt, evt, false)
	})

	t.Run("convert enriched envelope to json Event", func(t *testing.T) {
		// prepare enriched event
		user, ver, gver := "userID", event.NewVersion().Add(4, 0), event.NewVersion().Add(10, 0)
		evt := event.Envelop(ctx, stmID, testutil.GenEvts(1), func(env event.RWEnvelope) {
			env.SetUser(user)
			env.SetVersion(ver)
			env.SetGlobalVersion(gver)
		})[0]

		jsonEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// make sure events are equals except data value
		// which must be nil in the json event
		assertCmpEvt(t, jsonEvt, evt, true)

		// make sure raw versions & user values are the same as the ones added to envelope
		if want, val := ver.String(), jsonEvt.FRawVersion; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := gver.String(), jsonEvt.FRawGlobalVersion; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}
		if want, val := user, jsonEvt.FUser; want != val {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}

		// make sure json event remains valid in case of marshal/unmarshal
		// and without lost of infos
		b, err := json.Marshal(jsonEvt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		log.Println(string(b))
		jsonEvt = &jsonEvent{
			reg: reg,
		}
		if err := json.Unmarshal(b, jsonEvt); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		// compare the new json event with the orignal envelope
		assertCmpEvt(t, jsonEvt, evt, false)

		jsonEvt.reg = reg

		// make sure that events are fully equals
		assertCmpEvt(t, jsonEvt, evt, false)
	})
}

func TestSerializerInfos(t *testing.T) {
	namespace := "service2"
	ser := NewEventSerializer(namespace)
	if want, val := event.EventFormatJSON, ser.EventFormat(); want != val {
		t.Fatalf("expect %v, %v  be equals", want, val)
	}
	if want, val := "json", ser.FileExt(); want != val {
		t.Fatalf("expect %v, %v  be equals", want, val)
	}
	if want, val := "application/json", ser.ContentType(); want != val {
		t.Fatalf("expect %v, %v  be equals", want, val)
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	ctx := context.Background()
	stmID := event.NewStreamID("tenantID")

	// first, test marshal/unmarshal non registred events
	// in this case envelope will be lazily unmarshaled:
	// the envelope Event() method will return a nil event;
	// the domain event raw data must be preserved;
	// marshalling the unmarshaled result must be the same as the orignal envelope;
	// the envelope Event() will returns the exact domain event once events are registred.

	t.Run("marshal/unmarshal single event case", func(t *testing.T) {
		namespace := "service1"
		ser := NewEventSerializer(namespace)
		evt := event.Envelop(ctx, stmID, testutil.GenEvts(1))[0]
		b, err := ser.MarshalEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		revt, err := ser.UnmarshalEvent(b)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		assertCmpEvt(t, revt, evt, true)

		// make sure we do not lose data even if we marshal x2
		b2, err := ser.MarshalEvent(revt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if want, val := string(b), string(b2); !reflect.DeepEqual(want, val) {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}

		// register events, so serializer can unmarshal event data
		testutil.RegisterEvent(namespace)

		assertCmpEvt(t, revt, evt, false)
	})

	t.Run("marshal/unmarshal batch case", func(t *testing.T) {
		namespace := "service2"
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

		// make sure we do not lose data even if we marshal x2
		b2, err := ser.MarshalEventBatch(revts)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if want, val := string(b), string(b2); !reflect.DeepEqual(want, val) {
			t.Fatalf("expect %v, %v  be equals", want, val)
		}

		// register events, so serializer can unmarshal event data
		testutil.RegisterEvent(namespace)
		revts, err = ser.UnmarshalEventBatch(b2)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		for i, revt := range revts {
			assertCmpEvt(t, revt, evts[i], false)
		}
	})
}
