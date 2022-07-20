package event

import (
	"context"
	"errors"
	"testing"

	"github.com/ln80/storer/event/testutil"
)

func TestRegister(t *testing.T) {
	namespace := "foo"
	ctx := context.WithValue(context.Background(), ContextNamespaceKey, namespace)

	reg := NewRegister(namespace)
	defer reg.clear()
	// both events are registred whithin the given namespace
	reg.
		Set(&testutil.Event{}).
		Set(&testutil.Event2{})

	// get an unregistred event
	if _, err := reg.Get("foo.NoEvent"); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expect err be %v, got %v", ErrNotFoundInRegistry, err)
	}

	// succesfully find Event in reg
	e, err := reg.Get(TypeOf(&testutil.Event{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}
	if _, ok := e.(*testutil.Event); !ok {
		t.Fatalf("expected casting to %s is ok, got false", TypeOf(&testutil.Event{}))
	}
	if _, err = reg.Get(TypeOfWithContext(ctx, &testutil.Event{})); err != nil {
		t.Fatal("expected err be nil, got", err)
	}

	// only Event2 is registred in global register
	globReg := NewRegister("")
	globReg.
		Set(&testutil.Event2{})

	if _, err = globReg.Get(TypeOf(&testutil.Event{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expected err be %v, got %v", ErrNotFoundInRegistry, err)
	}
	_, err = reg.Get(TypeOf(&testutil.Event2{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}

	// the global registry, in contrast to reg with namespace, does not force it's namespace prefix in event name
	// thus, Event2 name in registry is {package name}.Event2 instead of {namespace}.Event2
	if _, err = globReg.Get(TypeOfWithContext(ctx, &testutil.Event2{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatal("Expected err be nil, got", err)
	}
}

func TestRegister_Convert(t *testing.T) {
	type Event struct{ Val string }
	type Event2 struct{ Val string }

	namespace := "testutil"

	// clear event registry before and after test
	NewRegister("").clear()
	defer NewRegister("").clear()

	// testutil.Event is the equivalent of Event.
	// Event2 does not have an equivalent in the global registry
	NewRegister(namespace).
		Set(&Event{}).
		Set(&Event2{})

	NewRegister("").
		Set(&testutil.Event{})

	evt1 := Event{Val: "1"}

	// case 1
	cevt1, err := NewRegister(namespace).Convert(&evt1)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	ccevt1, ok := cevt1.(testutil.Event)
	if !ok {
		t.Fatalf("expect the converted event type be %T, got false", testutil.Event{})
	}
	if want, val := evt1.Val, ccevt1.Val; want != val {
		t.Fatalf("expecet %v, %v be equals", want, val)
	}

	// case 2
	cevt1, err = NewRegister(namespace).Convert(evt1)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	ccevt1, ok = cevt1.(testutil.Event)
	if !ok {
		t.Fatalf("expect the converted event type be %T, got false", testutil.Event{})
	}
	if want, val := evt1.Val, ccevt1.Val; want != val {
		t.Fatalf("expecet %v, %v be equals", want, val)
	}

	// case error
	// try to convert an event that is not registred in the global registry
	// must returns a not found error
	evt2 := Event2{Val: "2"}
	_, err = NewRegister(namespace).Convert(evt2)
	if wantErr := ErrNotFoundInRegistry; !errors.Is(err, wantErr) {
		t.Fatalf("expect err be %v, got %v", wantErr, err)
	}
}
