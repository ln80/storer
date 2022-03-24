package s3

import (
	"context"
	"log"
	"time"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/signal"
)

// MakeSignalProcessor returns a processor that handles internal signals under the S3 package
func MakeSignalProcessor(streamMtn StreamMaintainer) signal.Processor {
	return func(ctx context.Context, sig signal.Signal) error {
		switch sig := sig.(type) {
		case *signal.ActiveStream:
			currentVer, _ := event.Ver(sig.CurrentVersion)
			lastActiveDayVer, _ := event.Ver(sig.LastActiveDayVersion)

			ignored, err := streamMtn.mergeDailyChunks(
				ctx, event.NewStreamID(sig.StreamID()),
				time.Unix(0, sig.LastUpdatedAt).UTC(),
				currentVer, lastActiveDayVer)

			if ignored {
				log.Printf(
					"Process ActiveStream signal: %v - merge daily chunks of stream %s was ignored at %v\n",
					sig, sig.GlobalStreamID, time.Unix(0, sig.SentAt).UTC(),
				)
			}

			if err != nil {
				return err
			}
		}

		return nil
	}
}
