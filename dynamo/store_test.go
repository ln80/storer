package dynamo

import (
	"context"
	"strconv"
	"testing"

	"github.com/ln80/storer/testutil"
)

func TestNewEventStore(t *testing.T) {

	tcs := []struct {
		dbsvc AdminAPI
		table string
		ok    bool
	}{
		{
			dbsvc: nil,
			table: "table name",
			ok:    false,
		},
		{
			dbsvc: dbsvc,
			table: "",
			ok:    false,
		},
		{
			dbsvc: nil,
			table: "",
			ok:    false,
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			defer func() {
				if tc.ok {
					if r := recover(); r != nil {
						t.Fatal("expect to not panics, got", r)
					}
				} else {
					if r := recover(); r == nil {
						t.Fatal("expect to panics")
					}
				}

			}()

			NewEventStore(tc.dbsvc, tc.table, nil, nil)
		})
	}
}
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
