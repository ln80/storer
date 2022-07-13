package event

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ln80/storer/event/testutil"
)

func TestStreamID(t *testing.T) {
	stmID := NewStreamID("gstmID", "p1", "p2", "p2")

	if want, val := strings.Join([]string{"gstmID", "p1", "p2", "p2"}, StreamIDPartsDelimiter), stmID.String(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if want, val := "gstmID", stmID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if want, val := []string{"p1", "p2", "p2"}, stmID.Parts(); !reflect.DeepEqual(want, val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if ok := stmID.Global(); ok {
		t.Fatal("expect stream is global be false, got true")
	}

	if ok := NewStreamID("gstmID").Global(); !ok {
		t.Fatal("expect stream is global be true, got false")
	}

	stmID, err := ParseStreamID("gstmID#service")
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, val := "gstmID", stmID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if ok := stmID.Global(); ok {
		t.Fatal("expect stream is global be false, got true")
	}
	stmID, err = ParseStreamID("gstmID")
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, val := "gstmID", stmID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if ok := stmID.Global(); !ok {
		t.Fatal("expect stream is global, got false")
	}

	if _, err := ParseStreamID(""); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("expect err be %v, got %v", ErrInvalidStreamID, err)
	}
}

func TestStreamFilter(t *testing.T) {
	f1 := StreamFilter{}
	f1.Build()
	if want, val := VersionMin, f1.From; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := VersionMax, f1.To; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := time.Now().Add(-2*time.Millisecond), f1.Until; !val.After(want) {
		t.Fatalf("expect %v, %v be slightly equals", want, val)
	}
	if want, val := time.Unix(0, 0), f1.Since; want != val {
		t.Fatalf("expect %v, %v be slightly equals", want, val)
	}

	f2 := StreamFilter{
		From: NewVersion().Add(10, 0),
	}
	f2.Build()
	if want, val := NewVersion().Add(10, 0), f2.From; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := VersionMax, f1.To; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
}

func TestStreamValidation(t *testing.T) {

	evts := []interface{}{
		&testutil.Event{
			Val: "1",
		},
		&testutil.Event{
			Val: "2",
		},
	}
	stmID := NewStreamID("gstmID")

	t.Run("test validate events", func(t *testing.T) {
		envs := Envelop(context.Background(), stmID, evts)

		// ValidateEvent fail because cursor is nil
		ignored, err := ValidateEvent(envs[0], nil)
		if ignored {
			t.Fatal("expect event is not ignored, got true")
		}
		if !errors.Is(err, ErrCursorNotFound) {
			t.Fatalf("expect err be %v, got %v", ErrCursorNotFound, err)
		}

		// init empty cursor
		cur := NewCursor(stmID.String())

		// ValidateEvent failed due to sequence validation which is not set [by default] by Envelop call
		if _, err := ValidateEvent(envs[0], cur); !errors.Is(err, ErrInvalidStream) {
			t.Fatalf("expect err be %v, got %v", ErrInvalidStream, err)
		}

		// ValidateEvent success because we skipped the version check
		if _, err := ValidateEvent(envs[0], cur, func(v *Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// Set 1st evt version
		envs[0].(RWEnvelope).SetVersion(NewVersion())
		// change timestamp to avoid an expected failure i.e cursor position already in 1st evt timestamp
		envs[0].(RWEnvelope).SetAt(time.Now())

		// ValidateEvent success coz 1st evt has a version & initial cursor does not
		// ValidateEvent will mutate the cursor and asign 1st evt's version to it
		if _, err := ValidateEvent(envs[0], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, err := ValidateEvent(envs[1], cur); !errors.Is(err, ErrInvalidStream) {
			t.Fatalf("expect err be %v, got %v", ErrInvalidStream, err)
		}
		// keep moving and validate the 2nd evt after a slight edit..
		envs[1].(RWEnvelope).SetVersion(NewVersion().Incr())
		envs[1].(RWEnvelope).SetAt(time.Now())
		if _, err := ValidateEvent(envs[1], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// create a versioned chunk
		envs = Envelop(context.Background(), stmID, evts, WithVersionIncr(NewVersion(), VersionSeqDiff10p0))
		cur = NewCursor(stmID.String())

		// ValidateEvent success
		if _, err := ValidateEvent(envs[0], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
	})

	t.Run("test validate stream chunk", func(t *testing.T) {
		withTimeHack := func(env RWEnvelope) {
			time.Sleep(1 * time.Nanosecond)
		}
		envs := Envelop(context.Background(), stmID, evts, withTimeHack)
		if err := Stream(envs).Validate(func(v *Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		envs = Envelop(context.Background(), stmID, evts, WithVersionIncr(NewVersion(), VersionSeqDiff10p0), withTimeHack)
		if err := Stream(envs).Validate(); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
	})
}

func TestStreamState(t *testing.T) {
	evts := []interface{}{
		&testutil.Event{
			Val: "1",
		},
		&testutil.Event{
			Val: "2",
		},
	}
	stmID := NewStreamID("gstmID")

	if !Stream([]Envelope{}).Empty() {
		t.Fatal("expect stream is empty, got false")
	}

	envs := Envelop(context.Background(), stmID, evts, WithVersionIncr(NewVersion(), VersionSeqDiff10p0))
	stm := Stream(envs)

	if stm.Empty() {
		t.Fatal("expect stream not empty, got empty")
	}
	if wantl, l := len(evts), len(stm.EventIDs()); wantl != l {
		t.Fatalf("expect len %d, %d  be equals", wantl, l)
	}
	for i, evt := range stm.Events() {
		if want, val := evts[i], evt; want != val {
			t.Fatalf("expect %v, %v be equals", want, val)
		}
	}
}
