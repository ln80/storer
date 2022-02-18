package dynamo

import (
	"context"

	"github.com/redaLaanait/storer/event"
)

type Forwarder interface {
	Forward(ctx context.Context, recs []Record) error
}

type forwarder struct {
	svc        ClientAPI
	table      string
	serializer event.Serializer
	persister  event.Persister
	publisher  event.Publisher
}

func NewForwarder(dbsvc ClientAPI, table string, per event.Persister, pub event.Publisher, ser event.Serializer) Forwarder {
	return &forwarder{
		svc:        dbsvc,
		table:      table,
		persister:  per,
		publisher:  pub,
		serializer: ser,
	}
}

func (f *forwarder) Forward(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}

	// find/initialize records global streams
	gstms, err := getGSTMBatch(ctx, f.svc, f.table, streamIDsFrom(recs))
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

func (f *forwarder) checkpoint(gstms map[string]*GSTM, recs []Record) (map[string][]event.Envelope, error) {
	mevs := make(map[string][]event.Envelope)

	for _, r := range recs {
		evts, err := f.serializer.UnmarshalEventBatch(r.Events)
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

		ver, _ := event.ParseVersion(gstm.Version)
		for _, ev := range evts {
			gstm.LastEventID = ev.ID()
			gstm.UpdatedAt = ev.At().UnixNano()

			ver = ver.Incr()
			rev, ok := ev.(interface {
				event.Envelope
				SetGlobalVersion(v event.Version) event.Envelope
			})
			if !ok {
				return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, "unmarchled event does not support SetGlobalVersion")
			}
			rev.SetGlobalVersion(ver)
			mevs[gstmID] = append(mevs[gstmID], rev)
		}
		gstm.Version = ver.String()
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
