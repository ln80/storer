package event

import (
	"context"
	"testing"
)

func TestTypeOf(t *testing.T) {

	type Event struct{ Val string }

	t.Run("test TypeOf", func(t *testing.T) {
		wantT := "event.Event"
		t1, t2 := TypeOf(Event{}), TypeOf(&Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}
	})

	t.Run("test TypeOfWithContext", func(t *testing.T) {
		ctx := context.Background()

		wantT := "event.Event"
		t1, t2 := TypeOfWithContext(ctx, Event{}), TypeOfWithContext(ctx, &Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}

		namespace := "test"
		ctx = context.WithValue(ctx, ContextNamespaceKey, namespace)
		wantT = namespace + ".Event"
		t1, t2 = TypeOfWithContext(ctx, Event{}), TypeOfWithContext(ctx, &Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}
	})

}
