package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/event/sourcing"
)

func EventStoreTest(t *testing.T, ctx context.Context, store event.Store) {
	t.Run("test event store", func(t *testing.T) {
		stmID := event.NewStreamID(event.UID().String())
		// test append events to stream
		envs := event.Envelop(ctx, stmID, GenEvts(10))
		if err := event.Stream(envs).Validate(func(v *event.Validation) {
			v.SkipVersion = true
		}); err != nil {
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
			if !CmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(renvs[i]))
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
		// test data integrity
		for i, env := range renvs {
			if !CmpEnv(env, envs[i]) {
				t.Fatalf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(envs[i]))
			}
		}
	})
}

func EventSourcingStoreTest(t *testing.T, ctx context.Context, store sourcing.Store) {
	t.Run("test event sourcing store", func(t *testing.T) {
		stmID := event.NewStreamID(event.UID().String())

		// test append events to stream with invalid current version of the stream
		stm := sourcing.Envelop(ctx, stmID, event.VersionMin, GenEvts(10))
		if err := store.AppendToStream(ctx,
			stm); !errors.Is(err, event.ErrAppendEventsFailed) {
			t.Fatalf("expect failed append err, got: %v", err)
		}

		// test append events to stream
		stm = sourcing.Envelop(ctx, stmID, event.VersionZero, GenEvts(10))
		if err := store.AppendToStream(ctx, stm); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// test append chunk twice
		err := store.AppendToStream(ctx, stm)
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
			if !CmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(renvs[i]))
			}
		}

		// test load sub part of the stream
		rstm, err = store.LoadStream(ctx, stmID, stm.Version().Trunc().Add(0, 10), stm.Version().Trunc().Add(0, 30))
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		renvs = rstm.Unwrap()
		if l := len(renvs); l != 3 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 3, l)
		}
		if !CmpEnv(renvs[0], envs[1]) {
			t.Fatalf("event data altered %v %v", FormatEnv(renvs[0]), FormatEnv(envs[1]))
		}
	})
}

func EventStreamerTest(t *testing.T, ctx context.Context, store interface {
	event.Streamer
	event.Store
}) {
	t.Run("test event streamer", func(t *testing.T) {
		gstmID := event.UID().String()

		stmID1 := event.NewStreamID(gstmID, "service1")
		stmID2 := event.NewStreamID(gstmID, "service2")

		// test append events to stream
		envs1 := event.Envelop(ctx, stmID1, GenEvts(10))
		if err := store.Append(ctx, stmID1, envs1...); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}
		envs2 := event.Envelop(ctx, stmID2, GenEvts(10))
		if err := store.Append(ctx, stmID2, envs2...); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		count := 0
		if err := store.Replay(ctx, event.NewStreamID(gstmID), event.StreamFilter{}, func(ctx context.Context, env event.Envelope) error {
			count++
			return nil
		}); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		if wantl, l := 20, count; wantl != l {
			t.Fatalf("expect events count be %d, got %d", wantl, l)
		}
	})
}
