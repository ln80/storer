package dynamo

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/ln80/storer/event"
	"github.com/ln80/storer/json"
	"github.com/ln80/storer/testutil"
)

type publisherMock struct {
	err    error
	traces map[string][]event.Envelope
}

var _ event.Publisher = &publisherMock{}

func (p *publisherMock) Publish(ctx context.Context, dest string, envs []event.Envelope) error {
	if p.err != nil {
		return p.err
	}
	if p.traces == nil {
		p.traces = make(map[string][]event.Envelope)
	}
	p.traces[dest] = envs
	return nil
}

type persisterMock struct {
	err    error
	traces map[string][]event.Envelope
}

var _ Persister = &persisterMock{}

func (p *persisterMock) Persist(ctx context.Context, stmID event.StreamID, envs event.Stream) error {
	if p.err != nil {
		return p.err
	}
	if p.traces == nil {
		p.traces = make(map[string][]event.Envelope)
	}
	p.traces[stmID.String()] = envs
	return nil
}

func makeRecord(ser event.Serializer, gstmID string, envs []event.Envelope) Record {
	chunk, _, _ := ser.MarshalEventBatch(envs)
	return Record{
		Item: Item{
			HashKey: gstmID,
		},
		Events: chunk,
	}
}

func TestNewForwarder(t *testing.T) {

	tcs := []struct {
		dbsvc AdminAPI
		table string
		pub   event.Publisher
		ok    bool
	}{
		{
			dbsvc: nil,
			table: "table name",
			ok:    false,
		},
		{
			dbsvc: dbsvc,
			table: "",
			ok:    false,
		},
		{
			dbsvc: dbsvc,
			table: "table name",
			pub:   nil,
			ok:    false,
		},
		{
			dbsvc: nil,
			table: "",
			ok:    false,
		},
		{
			dbsvc: dbsvc,
			table: "table name",
			pub:   &publisherMock{},
			ok:    true,
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			defer func() {
				if tc.ok {
					if r := recover(); r != nil {
						t.Fatal("expect to not panics, got", r)
					}
				} else {
					if r := recover(); r == nil {
						t.Fatal("expect to panics")
					}
				}

			}()

			NewForwarder(tc.dbsvc, tc.table, nil, tc.pub)
		})
	}
}

func TestEventForwarder(t *testing.T) {
	ctx := context.Background()

	ser := json.NewEventSerializer("")

	withTable(t, dbsvc, func(table string) {
		t.Run("test forward invalid record case 1", func(t *testing.T) {
			gstmID := event.UID().String()

			fwd := NewForwarder(dbsvc, table, &persisterMock{}, &publisherMock{})

			evtAt := time.Now()
			nokEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event1{
					Val: "test content 2",
				},
				&testutil.Event1{
					Val: "test content 3",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(-1 * time.Minute)
			})

			err := fwd.Forward(ctx, []Record{makeRecord(ser, gstmID, nokEnvs)})
			if wantErr := event.ErrInvalidStream; !errors.Is(err, wantErr) {
				t.Fatalf("expect err %v to occur, got %v", wantErr, err)
			}
		})

		t.Run("test forward invalid record case 2", func(t *testing.T) {
			gstmID := event.UID().String()

			fwd := NewForwarder(dbsvc, table, &persisterMock{}, &publisherMock{})

			evtAt := time.Now().Add(-10 * time.Hour)
			okEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event1{
					Val: "test content 2",
				},
				&testutil.Event1{
					Val: "test content 3",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			nokEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event2{
					Val: "test content 4",
				},
				&testutil.Event1{
					Val: "test content 5",
				},
			}, func(env event.RWEnvelope) {
				at := evtAt
				env.SetAt(at)
				evtAt = evtAt.Add(-1 * time.Minute)
			})

			err := fwd.Forward(ctx, []Record{
				makeRecord(ser, gstmID, okEnvs),
				makeRecord(ser, gstmID, nokEnvs),
			})
			if wantErr := event.ErrInvalidStream; !errors.Is(err, wantErr) {
				t.Fatalf("expect err %v to occur, got %v", wantErr, err)
			}
		})

		t.Run("test forward correctable invalid record", func(t *testing.T) {
			gstmID := event.UID().String()

			fwd := NewForwarder(dbsvc, table, &persisterMock{}, &publisherMock{})

			evtAt := time.Now()
			okEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				nil, // this entry must be escaped by Envelop func
				&testutil.Event1{
					Val: "test content 2",
				},
				&testutil.Event1{
					Val: "test content 3",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			if err := fwd.Forward(ctx, []Record{makeRecord(ser, gstmID, okEnvs)}); err != nil {
				t.Fatalf("expect err be nil, got %v", err)
			}
		})

		t.Run("test forward with persist failure", func(t *testing.T) {
			gstmID := event.UID().String()

			per := &persisterMock{
				err: errors.New("persist error"),
			}
			fwd := NewForwarder(dbsvc, table, per, &publisherMock{})

			evtAt := time.Now()
			okEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event1{
					Val: "test content 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			err := fwd.Forward(ctx, []Record{
				makeRecord(ser, gstmID, okEnvs),
			})
			if wantErr := per.err; !errors.Is(err, wantErr) {
				t.Fatalf("expect err %v to occur, got %v", wantErr, err)
			}
		})

		t.Run("test forward with publish failure", func(t *testing.T) {
			gstmID := event.UID().String()

			pub := &publisherMock{
				err: errors.New("publish error"),
			}
			fwd := NewForwarder(dbsvc, table, &persisterMock{}, pub)

			evtAt := time.Now()
			okEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event2{
					Val: "test content 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			err := fwd.Forward(ctx, []Record{
				makeRecord(ser, gstmID, okEnvs),
			})
			if wantErr := pub.err; !errors.Is(err, wantErr) {
				t.Fatalf("expect err: %v to occur, got %v", wantErr, err)
			}
		})

		t.Run("test successfully forward events", func(t *testing.T) {
			gstmID := event.UID().String()

			per := &persisterMock{}
			pub := &publisherMock{}
			fwd := NewForwarder(dbsvc, table, per, pub)

			evtAt := time.Now()
			okEnvs := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event2{
					Val: "test content 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			okEnvs2 := event.Envelop(ctx, event.NewStreamID(gstmID), []interface{}{
				&testutil.Event1{
					Val: "test content 1",
				},
				&testutil.Event1{
					Val: "test content 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Minute)
			})

			err := fwd.Forward(ctx, []Record{
				makeRecord(ser, gstmID, okEnvs),
				makeRecord(ser, gstmID, okEnvs2),
			})
			if err != nil {
				t.Fatalf("expect err be nil , got %v", err)
			}
			if wantl, l := len(okEnvs)+len(okEnvs2), len(per.traces[gstmID]); wantl != l {
				t.Fatalf("expect %d events be persisted, got %d", wantl, l)
			}
			if err := event.Stream(per.traces[gstmID]).Validate(func(v *event.Validation) {
				v.GlobalStream = true
			}); err != nil {
				t.Fatalf("expect err be nil, got err: %v", err)
			}
			if wantver, v := event.NewVersion().Add(3, 0), per.traces[gstmID][len(per.traces[gstmID])-1].GlobalVersion(); !wantver.Equal(v) {
				t.Fatalf("expect vesions be equals, got: %v, %v", wantver, v)
			}
			if wantl, l := 1, len(pub.traces[(&testutil.Event2{}).EvDests()[0]]); wantl != l {
				t.Fatalf("expect %d events be published, got %d", wantl, l)
			}

			// check idempotency
			per = &persisterMock{}
			pub = &publisherMock{}
			fwd = NewForwarder(dbsvc, table, per, pub)

			err = fwd.Forward(ctx, []Record{
				makeRecord(ser, gstmID, okEnvs),
				makeRecord(ser, gstmID, okEnvs2),
			})
			if err != nil {
				t.Fatalf("expect err be nil , got %v", err)
			}
			if wantl, l := 0, len(per.traces[gstmID]); wantl != l {
				t.Fatalf("expect %d events be persisted, got %d", wantl, l)
			}
			if wantl, l := 0, len(pub.traces[(&testutil.Event2{}).EvDests()[0]]); wantl != l {
				t.Fatalf("expect %d events be published, got %d", wantl, l)
			}
		})
	})

}
