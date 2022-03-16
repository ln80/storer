package testutil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redaLaanait/storer/event"
)

func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if attempts--; attempts > 0 {
			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}

// MakeIntegrationTest prepares a test suite that:
// 1 append event to store;
// 2 query the event stream and check data integrity;
// 3 query the global stream and check data integrity;
func MakeIntegrationTest_StoreAndStreamer(store event.Store, streamer event.Streamer) func(ctx context.Context) error {

	RegisterEvent()

	return func(ctx context.Context) error {
		// gstmID := event.UID().String()
		gstmID := "testest"
		stmID := event.NewStreamID(gstmID, "service")

		evts := []interface{}{
			&Event1{
				Val: "1",
			},
			&Event2{
				Val: "2",
			},
		}

		envs := event.Envelop(ctx, stmID, evts)

		if err := store.Append(ctx, stmID, envs...); err != nil {
			return fmt.Errorf("expect to append events, got err: %w", err)
		}

		result, err := store.Load(ctx, stmID)
		if err != nil {
			return fmt.Errorf("expect to load events got err %v", err)
		}
		if l := len(result); l != 2 {
			return fmt.Errorf("invalid loaded events length, must be %d got: %d", 2, l)
		}

		for i, env := range envs {
			if !CmpEnv(env, result[i]) {
				return fmt.Errorf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(result[i]))
			}
		}

		result2 := make([]event.Envelope, 0)

		streamFn := func() error {
			if err := streamer.Replay(ctx, event.NewStreamID(gstmID), event.StreamFilter{}, func(ctx context.Context, ev event.Envelope) error {
				result2 = append(result2, ev)
				return nil
			}); err != nil {
				return err
			}
			if len(result2) == 0 {
				return errors.New("events not found in global stream")
			}
			return nil
		}

		// make sure to wait and retry if needed the global stream is likely eventually consistent
		if err := retry(3, time.Second, streamFn); err != nil {
			return fmt.Errorf("expect to stream events from global stream, got err %w", err)
		}
		if l := len(result2); l != 2 {
			return fmt.Errorf("invalid streamed events length, must be %d got: %d", 2, l)
		}
		ver := event.VersionMin
		for i, env := range envs {
			if !CmpEnv(env, result2[i]) {
				return fmt.Errorf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(result2[i]))
			}

			if !result2[i].GlobalVersion().Equal(ver) {
				return fmt.Errorf("expect global version be %v, got %v", ver, result2[i].GlobalVersion())
			}
			ver = ver.Incr()
		}

		return nil
	}
}
