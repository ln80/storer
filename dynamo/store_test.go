package dynamo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/event/sourcing"
)

func TestEventStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping event store test")
	}

	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		// prepare event store
		store := NewEventStore(dbsvc, table)
		stmID := event.NewStreamID("dwRQAc68PhHQh4BUnrNsoS", "service")

		// test append events to stream
		envs := event.Envelop(ctx, stmID, genEvents(10))
		if err := event.Stream(envs).Validate(); err != nil {
			t.Fatalf("expect to gen a valid chunk, got err: %v", err)
		}
		if err := store.Append(ctx, stmID, envs...); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// test append chunk twice
		err := store.Append(ctx, stmID, envs...)
		if err == nil || !errors.Is(err, event.ErrAppendEventsConflict) {
			t.Fatalf("expect conflict error to occur, got: %v", err)
		}

		// test load appended chunk
		renvs, err := store.Load(ctx, stmID)
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if l := len(renvs); l != 10 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 10, l)
		}

		// test data integrity
		for i, env := range envs {
			if !cmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, formatEnv(env), formatEnv(renvs[i]))
			}
		}

		// test load a sub part of the stream
		renvs, err = store.Load(ctx, stmID, time.Unix(0, 0), renvs[2].At())
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if l := len(renvs); l != 3 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 3, l)
		}
	})
}

func TestEventSourcingStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping event sourcing store test")
	}

	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		// prepare event store
		store := NewEventSourcingStore(dbsvc, table)
		stmID := event.NewStreamID("dwRQAc68PhHQh4BUnrNsoS", "service")

		// test append events to stream
		stm := sourcing.Envelop(ctx, stmID, event.NewVersion(), genEvents(10))
		if err := stm.Validate(); err != nil {
			t.Fatalf("expect to gen a valid chunk, got err: %v", err)
		}
		if err := store.AppendStream(ctx, stm); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// test append chunk twice
		err := store.AppendStream(ctx, stm)
		if err == nil || !errors.Is(err, event.ErrAppendEventsConflict) {
			t.Fatalf("expect conflict error to occur, got: %v", err)
		}

		// test load appended stream
		rstm, err := store.LoadStream(ctx, stmID)
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if rid, id := stm.ID().String(), stmID.String(); rid != id {
			t.Fatalf("expect loaded stream ID be %s, got %s", rid, id)
		}
		renvs := rstm.Unwrap()
		if l := len(renvs); l != 10 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 10, l)
		}

		// test data integrity
		envs := stm.Unwrap()
		for i, env := range envs {
			if !cmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, formatEnv(env), formatEnv(renvs[i]))
			}
		}

		// test load sub part of the stream
		rstm, err = store.LoadStream(ctx, stmID, stm.Version().Add(0, 10), stm.Version().Add(0, 30))
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		renvs = rstm.Unwrap()
		if l := len(renvs); l != 2 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 2, l)
		}
		if !cmpEnv(renvs[0], envs[1]) {
			t.Fatalf("event data altered %v %v", formatEnv(renvs[0]), formatEnv(envs[1]))
		}
	})
}
