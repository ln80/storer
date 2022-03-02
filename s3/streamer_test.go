package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redaLaanait/storer/event"
	interevent "github.com/redaLaanait/storer/internal/event"
	"github.com/redaLaanait/storer/internal/testutil"
)

func TestEventStreamer_Persist(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping s3 test event streamer persist in short mode")
	}

	ctx := context.Background()

	gstmID := event.NewStreamID(event.UID().String())

	withBucket(t, s3svc, func(bucket string) {
		streamer := NewEventStreamer(s3svc, bucket, nil, func(cfg *StreamerConfig) {
			cfg.Provider = ProviderMinio
		})

		t.Run("test persist stream chunk with invalid stmID", func(t *testing.T) {
			gstmID := event.NewStreamID("")
			err := streamer.(interevent.Persister).Persist(ctx, gstmID,
				event.Envelop(ctx, gstmID, []interface{}{
					&testutil.Event1{
						Val: "test",
					},
				}),
			)
			if wanterr := event.ErrInvalidStream; !errors.Is(err, wanterr) {
				t.Fatalf("expect err be %v, got %v", wanterr, err)
			}
		})

		t.Run("test persist stream chunk with invalid stream global version", func(t *testing.T) {
			nokVer := event.NewVersion()
			err := streamer.(interevent.Persister).Persist(ctx, gstmID, event.Envelop(ctx, gstmID, []interface{}{
				&testutil.Event1{
					Val: "test 1",
				},
				&testutil.Event1{
					Val: "test 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetGlobalVersion(nokVer)
				nokVer.Add(2, 0)
			}))
			if wanterr := event.ErrInvalidStream; !errors.Is(err, wanterr) {
				t.Fatalf("expect err be %v, got %v", wanterr, err)
			}
		})

		t.Run("test successfully persist a stream chunk", func(t *testing.T) {
			okVer := event.NewVersion()
			chunk := event.Envelop(ctx, gstmID, []interface{}{
				&testutil.Event1{
					Val: "test 1",
				},
				&testutil.Event1{
					Val: "test 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetGlobalVersion(okVer)
				okVer = okVer.Incr()
			})
			if err := streamer.(interevent.Persister).Persist(ctx, gstmID, chunk); err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}

			envs := []event.Envelope{}
			if err := streamer.Replay(ctx, gstmID, event.StreamFilter{}, func(ctx context.Context, e event.Envelope) error {
				envs = append(envs, e)
				return nil
			}); err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}
			if wantl, l := len(chunk), len(envs); wantl != l {
				t.Fatalf("expect %d events be persisted, got %d", wantl, l)
			}
			for i, env := range envs {
				if !testutil.CmpEnv(env, chunk[i]) {
					t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(chunk[i]))
				}
			}
		})
	})
}
func TestEventStreamer_Replay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping s3 test event streamer replay in short mode")
	}

	ctx := context.Background()
	gstmID := event.NewStreamID(event.UID().String())

	withBucket(t, s3svc, func(bucket string) {
		streamer := NewEventStreamer(s3svc, bucket, nil, func(cfg *StreamerConfig) {
			cfg.Provider = ProviderMinio
			cfg.StreamLatest = true
		})

		gver := event.NewVersion()
		startingDate := time.Now().AddDate(0, 0, -1)

		chunk1At := startingDate
		chunk1 := event.Envelop(ctx, gstmID, testutil.GenEvts(20), func(env event.RWEnvelope) {
			env.SetGlobalVersion(gver)
			env.SetAt(chunk1At)
			gver = gver.Incr()
			chunk1At = chunk1At.Add(5 * time.Millisecond)
		})
		if err := streamer.(interevent.Persister).Persist(ctx, gstmID, chunk1); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}
		chunk2 := event.Envelop(ctx, gstmID, testutil.GenEvts(20), func(env event.RWEnvelope) {
			env.SetGlobalVersion(gver)
			gver = gver.Incr()
		})
		if err := streamer.(interevent.Persister).Persist(ctx, gstmID, chunk2); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}

		t.Run("fully replay the stream", func(t *testing.T) {
			envs := []event.Envelope{}
			err := streamer.Replay(ctx, gstmID, event.StreamFilter{}, func(ctx context.Context, ev event.Envelope) error {
				envs = append(envs, ev)
				return nil
			})
			if err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}
			if wantl, l := len(chunk1)+len(chunk2), len(envs); wantl != l {
				t.Fatalf("expect %d events be replayed, got %d", wantl, l)
			}
			for i, env := range envs {
				var ch event.Envelope
				if i < 20 {
					ch = chunk1[i]
				} else {
					ch = chunk2[i-20]
				}
				if !testutil.CmpEnv(env, ch) {
					t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(ch))
				}
			}
		})

		t.Run("replay stream from to version", func(t *testing.T) {
			envs := []event.Envelope{}
			filter := event.StreamFilter{
				From: event.NewVersion().Add(10, 0),
				To:   event.NewVersion().Add(29, 0),
			}
			err := streamer.Replay(ctx, gstmID, filter, func(ctx context.Context, ev event.Envelope) error {
				envs = append(envs, ev)
				return nil
			})
			if err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}
			if wantl, l := 20, len(envs); wantl != l {
				t.Fatalf("expect %d events be replayed, got %d", wantl, l)
			}
			for i, env := range envs {
				var ch event.Envelope
				if i < 10 {
					ch = chunk1[i+10]
				} else {
					ch = chunk2[i-10]
				}
				if !testutil.CmpEnv(env, ch) {
					t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(ch))
				}
			}
		})

		t.Run("replay stream since until timestamp", func(t *testing.T) {
			envs := []event.Envelope{}
			filter := event.StreamFilter{
				Since: startingDate,
				Until: startingDate.Add(20 * time.Minute), // this must be enaught to query only chunk1 events
			}
			err := streamer.Replay(ctx, gstmID, filter, func(ctx context.Context, ev event.Envelope) error {
				envs = append(envs, ev)
				return nil
			})
			if err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}
			if wantl, l := 20, len(envs); wantl != l {
				t.Fatalf("expect %d events be replayed, got %d", wantl, l)
			}
			for i, env := range envs {
				if !testutil.CmpEnv(env, chunk1[i]) {
					t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(chunk1[i]))
				}
			}
		})
	})
}

func TestEventMaintainer_MergeChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping s3 test event maintainer contat chunks in short mode")
	}

	ctx := context.Background()
	gstmID := event.NewStreamID(event.UID().String())

	withBucket(t, s3svc, func(bucket string) {
		streamer := NewEventStreamer(s3svc, bucket, nil, func(cfg *StreamerConfig) {
			cfg.Provider = ProviderMinio
			cfg.StreamLatest = false
		})
		// persist chunks
		gver := event.NewVersion()

		chunkSize := 20
		chunkCount := 10
		for i := 0; i < chunkCount; i++ {
			if err := streamer.(interevent.Persister).Persist(ctx, gstmID,
				event.Envelop(ctx, gstmID, testutil.GenEvts(chunkSize), func(env event.RWEnvelope) {
					env.SetGlobalVersion(gver)
					gver = gver.Incr()
				}),
			); err != nil {
				t.Fatalf("expect err be nil, got: %v", err)
			}
		}

		filter := event.StreamFilter{
			From: event.NewVersion(),
		}

		if err := streamer.(StreamMaintainer).MergeChunks(ctx, gstmID, filter); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}

		count := 0
		if err := streamer.Replay(ctx, gstmID, filter, func(ctx context.Context, e event.Envelope) error {
			count++
			return nil
		}); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}

		if wantl, l := chunkCount*chunkSize, count; wantl != l {
			t.Fatalf("expect %d events be persisted, got %d", wantl, l)
		}

		// make sure Replay events works when it has to to query both partitions and last uploaded chunks
		streamer = NewEventStreamer(s3svc, bucket, nil, func(cfg *StreamerConfig) {
			cfg.Provider = ProviderMinio
			cfg.StreamLatest = true
		})
		latestChunkSize := 50
		if err := streamer.(interevent.Persister).Persist(ctx, gstmID,
			event.Envelop(ctx, gstmID, testutil.GenEvts(latestChunkSize), func(env event.RWEnvelope) {
				env.SetGlobalVersion(gver)
				gver = gver.Incr()
			}),
		); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}
		count = 0
		if err := streamer.Replay(ctx, gstmID, filter, func(ctx context.Context, e event.Envelope) error {
			count++
			return nil
		}); err != nil {
			t.Fatalf("expect err be nil, got: %v", err)
		}

		if wantl, l := chunkCount*chunkSize+latestChunkSize, count; wantl != l {
			t.Fatalf("expect %d events be persisted, got %d", wantl, l)
		}
	})
}
