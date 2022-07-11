package memory

import (
	"context"
	"testing"

	"github.com/ln80/storer/event"
	"github.com/ln80/storer/testutil"
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
