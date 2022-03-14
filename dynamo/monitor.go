package dynamo

import (
	"context"
	"time"

	"github.com/redaLaanait/storer/signal"
)

type monitor struct {
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
		sigs = append(sigs, signal.ActiveStreamSignal(gstm.StreamID, gstm.UpdatedAt, time.Now().UnixNano()))
	}
	return sigs, nil
}
