package dynamo

import (
	"context"
	"testing"

	"github.com/redaLaanait/storer/testutil"
)

func TestEventStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping event store test")
	}

	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		testutil.EventStoreTest(t, ctx, NewEventStore(dbsvc, table))
	})
}

func TestEventSourcingStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping event sourcing store test")
	}

	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		testutil.EventSourcingStoreTest(t, ctx, NewEventStore(dbsvc, table))
	})
}
