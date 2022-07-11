package dynamo

import (
	"context"
	"testing"

	"github.com/ln80/storer/testutil"
)

func TestEventStore(t *testing.T) {
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		testutil.EventStoreTest(t, ctx, NewEventStore(dbsvc, table))
	})
}

func TestEventSourcingStore(t *testing.T) {
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		testutil.EventSourcingStoreTest(t, ctx, NewEventStore(dbsvc, table))
	})
}
