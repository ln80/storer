package dynamo

import (
	"context"
	"testing"
)

func TestCreateEventTable(t *testing.T) {
	ctx := context.Background()

	table := genTableName("test-table")

	if err := CreateTable(ctx, dbsvc, table); err != nil {
		t.Fatalf("expect create table, got err: %v", err)
	}

	if err := DeleteTable(ctx, dbsvc, table); err != nil {
		t.Fatalf("expect delete table, got err: %v", err)
	}
}
