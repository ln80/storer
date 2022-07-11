package dynamo

import (
	"context"

	"github.com/ln80/storer/event"
	"github.com/ln80/storer/json"
)

// Forwarder defines the service responsible for forwarding events from the dynamodb change stream
// to a permanent store and push-based workers/projectors.
type Forwarder interface {
	Forward(ctx context.Context, recs []Record) error
}

type forwarder struct {
	svc       ClientAPI
	table     string
	persister Persister
	publisher event.Publisher
	*ForwarderConfig
}

type ForwarderConfig struct {
	Serializer event.Serializer
}

// Persister define the service that persists chunks in a durable store e.g S3
type Persister interface {
	Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error
}

func NewForwarder(dbsvc ClientAPI, table string, per Persister, pub event.Publisher, opts ...func(cfg *ForwarderConfig)) Forwarder {
	fwd := &forwarder{
		svc:       dbsvc,
		table:     table,
		persister: per,
		publisher: pub,
		ForwarderConfig: &ForwarderConfig{
			Serializer: json.NewEventSerializer(""),
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(fwd.ForwarderConfig)
	}

	return fwd
}

func (f *forwarder) Forward(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}

	// find/initialize records global streams
	gstms, err := getGSTMBatch(ctx, f.svc, f.table, GSTMFilter{
		StreamIDs: streamIDsFrom(recs),
	})
	if err != nil {
		return err
	}

	// update global streams checkpoint and set event global version
	mevs, err := f.checkpoint(gstms, recs)
	if err != nil {
		return err
	}

	// fan-out events to corresponding destinations
	for dest, evs := range event.RouteEvents(mevs) {
		if err := f.publisher.Publish(ctx, dest, evs); err != nil {
			return err
		}
	}

	// persist enriched events in a permanent store
	for gstmID, evs := range mevs {
		if err := f.persister.Persist(ctx, event.NewStreamID(gstmID), evs); err != nil {
			return err
		}
	}

	if err := persistGSTMBatch(ctx, f.svc, f.table, gstms); err != nil {
		return err
	}

	return nil
}

// checkpoint iterates over the given records, enrich their events by setting the global version,
// and maintains the related global stream checkpoint.
func (f *forwarder) checkpoint(gstms map[string]*GSTM, recs []Record) (map[string][]event.Envelope, error) {
	mevs := make(map[string][]event.Envelope)

	for _, r := range recs {
		evts, err := f.Serializer.UnmarshalEventBatch(r.Events)
		if err != nil {
			return nil, err
		}
		if len(evts) == 0 {
			continue
		}

		gstmID := evts[0].GlobalStreamID()
		gstm, ok := gstms[gstmID]
		if !ok {
			return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, "record gstm not found")
		}

		if gstm.LastEventID >= evts[0].ID() {
			continue
		}

		// get current global version
		gver, _ := event.ParseVersion(gstm.Version)

		for _, ev := range evts {
			gver = gver.Incr()

			if err := gstm.Update(ev, gver); err != nil {
				return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, err)
			}

			rev, ok := ev.(interface {
				event.Envelope
				SetGlobalVersion(v event.Version) event.Envelope
			})
			if !ok {
				return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, "unmarshled event does not support SetGlobalVersion")
			}
			rev.SetGlobalVersion(gver)
			mevs[gstmID] = append(mevs[gstmID], rev)
		}
	}

	for gstmID, evs := range mevs {
		if err := event.Stream(evs).Validate(func(v *event.Validation) {
			v.GlobalStream = true
		}); err != nil {
			return nil, event.Err(err, gstmID, "during checkpoint process")
		}
	}

	return mevs, nil
}
