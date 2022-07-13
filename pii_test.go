package storer

import (
	"context"
	"testing"

	"github.com/ln80/pii"
	pii_memory "github.com/ln80/pii/memory"
	"github.com/ln80/storer/memory"
	"github.com/ln80/storer/testutil"
)

func TestPIIProtectWrapper(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	store := memory.NewEventStore()

	// check: https://github.com/ln80/pii for more details
	pf := pii.NewFactory(func(namespace string) pii.Protector {
		return pii.NewProtector(namespace, pii_memory.NewKeyEngine())
	})
	defer pf.Monitor(ctx)

	store = ProtectPII(store, pf)

	testutil.EventStoreTest(t, ctx, store)
	testutil.EventSourcingStoreTest(t, ctx, store)
	testutil.EventStreamerTest(t, ctx, store)
}
