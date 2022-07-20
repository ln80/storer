package local

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ln80/storer/event"
	_s3 "github.com/ln80/storer/s3"
	"github.com/ln80/storer/storer-cli/internal/logger"
)

type publishLogger struct {
	Origin event.Publisher
}

func (p *publishLogger) Publish(ctx context.Context, dest string, evts []event.Envelope) error {
	fields := []logger.Field{logger.F("Dest", dest), logger.F("Total", strconv.Itoa(len(evts)))}
	if err := p.Origin.Publish(ctx, dest, evts); err != nil {
		logger.ErrorWithMsg(err, "Failed to publish events", fields...)
		return err
	}
	logger.Info("Events published", fields...)
	return nil
}

var _ event.Publisher = &publishLogger{}

type persistLogger struct {
	Origin _s3.Persister
}

func (p *persistLogger) Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error {
	fields := []logger.Field{
		logger.F("StmID", stmID.String()),
		logger.F("GVer", evts[len(evts)-1].GlobalVersion().String()),
		logger.F("Total", strconv.Itoa(len(evts))),
	}

	if err := p.Origin.Persist(ctx, stmID, evts); err != nil {
		logger.ErrorWithMsg(err, "Failed to Persist events", fields...)
		return err
	}
	logger.Info("Events persisted", fields...)

	b, err := json.MarshalIndent(evts, "", " ")
	if err == nil {
		logger.Debug(fmt.Sprintf(`Events payload:
%s
		`, string(b)))
	}
	return nil
}

var _ _s3.Persister = &persistLogger{}
