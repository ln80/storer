package dynamo

import (
	"context"
	"time"

	"github.com/ln80/storer/signal"
)

type monitor struct {
	signal.Monitor

	svc   ClientAPI
	table string
}

var _ signal.Monitor = &monitor{}

func NewMonitor(dbsvc ClientAPI, table string) signal.Monitor {
	return &monitor{
		svc:   dbsvc,
		table: table,
	}
}

// ActiveStreams implements ActiveStreams method of signal.Monitor interface.
// It generates ActiveStreams signals based on the current state of gstms.
func (m *monitor) ActiveStreams(ctx context.Context, since time.Time) ([]*signal.ActiveStream, error) {
	gstms, err := getGSTMBatch(ctx, m.svc, m.table, GSTMFilter{
		UpdatedAfter: since,
	})
	if err != nil {
		return nil, err
	}
	sigs := make([]*signal.ActiveStream, 0, len(gstms))
	for _, gstm := range gstms {
		gstm := gstm
		sig := signal.ActiveStreamSignal(
			gstm.StreamID,
			gstm.Version,
			gstm.UpdatedAt,
			time.Now().UnixNano(),
		)
		// enrich signal with last active day info if it exists
		if day := gstm.LastActiveDay(since); day != nil {
			sig.SetLastActiveDay(day.Day, day.Version)
		}
		sigs = append(sigs, sig)
	}
	return sigs, nil
}
