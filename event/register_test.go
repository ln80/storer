package event

import (
	"context"
	"errors"
	"testing"
)

func TestRegister(t *testing.T) {

	type Event struct{ Val string }
	type Event2 struct{ Val string }

	namespace := "foo"
	ctx := context.WithValue(context.Background(), ContextNamespaceKey, namespace)

	reg := NewRegister(namespace)

	// both events are registred whithin the given namespace
	reg.
		Set(&Event{}).
		Set(&Event2{})

	// get an unregistred event
	if _, err := reg.Get("foo.NoEvent"); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expect err be %v, got %v", ErrNotFoundInRegistry, err)
	}

	// succesfully find Event in reg
	e, err := reg.Get(TypeOf(&Event{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}
	if _, ok := e.(*Event); !ok {
		t.Fatalf("expected casting to %s is ok, got false", TypeOf(&Event{}))
	}
	if _, err = reg.Get(TypeOfWithContext(ctx, &Event{})); err != nil {
		t.Fatal("expected err be nil, got", err)
	}

	// only Event2 is registred in global register
	globReg := NewRegister("")
	globReg.
		Set(&Event2{})

	if _, err = globReg.Get(TypeOf(&Event{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expected err be %v, got %v", ErrNotFoundInRegistry, err)
	}
	_, err = reg.Get(TypeOf(&Event2{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}

	// the global registry, in contrast to reg with namespace, does not force it's namespace prefix in event name
	// thus, Event2 name in registry is {packageName}.Event2 instead of {namespace}.Event2
	if _, err = globReg.Get(TypeOfWithContext(ctx, &Event2{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatal("Expected err be nil, got", err)
	}
}
