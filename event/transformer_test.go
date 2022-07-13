package event

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/ln80/storer/event/testutil"
)

func TestTransformEvents(t *testing.T) {
	ctx := context.Background()

	gstmID := "tenantID"

	stmID := NewStreamID(gstmID, "service", "rootEntityID")

	// test data combines pointers and struct value
	evts := []interface{}{
		&testutil.Event{
			Val: "1",
		},
		testutil.Event{
			Val: "2",
		},
		testutil.Event{
			Val: "3",
		},
	}
	envs := Envelop(ctx, stmID, evts)

	t.Run("partial failure assert atomicity", func(t *testing.T) {
		wantErr := errors.New("transform fake test error")
		if err := Transform(ctx, envs, func(ctx context.Context, copyPtrs ...interface{}) error {
			for i := range copyPtrs {
				if i%2 == 1 {
					return wantErr
				}
				ptr, _ := copyPtrs[i].(*testutil.Event)
				ptr.Val = "0"
			}
			return nil
		}); !errors.Is(err, wantErr) {
			t.Fatalf("expect %v, %v be equals", err, wantErr)
		}
		for i, env := range envs {
			want, got := (testutil.Event{Val: strconv.Itoa(i + 1)}), env.Event()

			if reflect.TypeOf(got).Kind() == reflect.Ptr {
				if !reflect.DeepEqual(&want, got) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			} else {
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}
		}
	})

	t.Run("assert transform events successfully", func(t *testing.T) {
		// for i, env := range envs {
		// 	t.Logf("%d: %+v %p", i, env.Event(), env.Event())
		// }
		if err := Transform(ctx, envs, func(ctx context.Context, copyPtrs ...interface{}) error {
			for i := range copyPtrs {
				ptr, _ := copyPtrs[i].(*testutil.Event)
				ptr.Val = "0"
			}
			return nil
		}); err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		for _, env := range envs {
			// t.Logf("%d: %+v %p", i, env.Event(), env.Event())
			if want, got := "0", env.Event().(*testutil.Event).Val; want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})
}
