package memory

import (
	"context"
	"testing"

	"github.com/ln80/storer/testutil"
)

func TestEventStore(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	testutil.EventStoreTest(t, ctx, NewEventStore())
	testutil.EventSourcingStoreTest(t, ctx, NewEventStore())
	testutil.EventStreamerTest(t, ctx, NewEventStore())
}
