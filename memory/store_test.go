package memory

import (
	"context"
	"testing"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/internal/testutil"
)

func TestEventStore(t *testing.T) {
	event.NewRegister("").
		Set(testutil.Event1{}).
		Set(testutil.Event2{})

	ctx := context.Background()

	testutil.EventStoreTest(t, ctx, NewEventStore())
	testutil.EventSourcingStoreTest(t, ctx, NewEventStore())
	testutil.EventStreamerTest(t, ctx, NewEventStore())
}
