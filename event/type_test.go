package event

import (
	"context"
	"testing"

	"github.com/ln80/storer/event/testutil"
)

func TestTypeOf(t *testing.T) {
	t.Run("test TypeOf", func(t *testing.T) {
		wantT := "testutil.Event"
		t1, t2 := TypeOf(testutil.Event{}), TypeOf(&testutil.Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}
	})

	t.Run("test TypeOfWithContext", func(t *testing.T) {
		ctx := context.Background()

		wantT := "testutil.Event"
		t1, t2 := TypeOfWithContext(ctx, testutil.Event{}), TypeOfWithContext(ctx, &testutil.Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}

		namespace := "test"
		ctx = context.WithValue(ctx, ContextNamespaceKey, namespace)
		wantT = namespace + ".Event"
		t1, t2 = TypeOfWithContext(ctx, testutil.Event{}), TypeOfWithContext(ctx, &testutil.Event{})
		if t1 != t2 {
			t.Fatalf("expect %s, %s be equals", t1, t2)
		}
		if t1 != wantT {
			t.Fatalf("expect %s, %s be equals", t1, wantT)
		}
	})

}
