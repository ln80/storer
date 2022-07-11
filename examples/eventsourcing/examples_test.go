package sourcing_test

import (
	"context"
	"fmt"
	"log"

	"github.com/ln80/storer/event"
	"github.com/ln80/storer/event/sourcing"
	"github.com/ln80/storer/examples/eventsourcing/bank"
	"github.com/ln80/storer/memory"
)

func Example() {
	ctx := context.Background()

	// namespace e.g. bounded context
	var namespace = "bank"

	event.NewRegister(namespace).
		Set(bank.AccountOpened{}).
		Set(bank.MoneyWithrawn{}).
		Set(bank.MoneyDeposited{})

	var store sourcing.Store = memory.NewEventStore()

	tenantID := "faa1bb0a-e0cc-47ea-a03a-998939743c68"
	accountID := "70a77d0c-0c8e-41f2-8251-5b82f2c40bde"

	// aggregate stream ID which is a sub of tenant global stream
	stmID := event.NewStreamID(tenantID, namespace, accountID)

	// create account aggregate + execute first command
	acc, err := bank.OpenAccount(accountID, "Paris Denesik")
	if err != nil {
		log.Fatal(err)
	}
	if err := acc.DepositMoney(1000); err != nil {
		log.Fatal(err)
	}

	// flush changes to event store
	if err := store.AppendToStream(ctx, sourcing.Envelop(ctx, stmID, event.VersionZero, acc.EvClearChanges())); err != nil {
		log.Fatal(err)
	}

	// load aggregate stream + rebuild account aggregate + check the current balance
	stm, err := store.LoadStream(ctx, stmID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("stream version: %v\n", stm.Version())
	for i, evt := range stm.Unwrap() {
		fmt.Printf("evt %d: %v, ver: %s\n", i+1, event.TypeOf(evt.Event()), evt.Version())
	}
	acc, err = bank.BuildAccount(stm.Unwrap().Events())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("account balance: %d\n", acc.Balance)

	// update account aggregate + flush changes
	if err := acc.WithrawMoney(500); err != nil {
		log.Fatal(err)
	}
	if err := store.AppendToStream(ctx, sourcing.Envelop(ctx, stmID, stm.Version(), acc.EvClearChanges())); err != nil {
		log.Fatal(err)
	}

	// only load the last appended event
	stm, err = store.LoadStream(ctx, stmID, stm.Version().Incr())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("stream version: %v\n", stm.Version())
	for i, evt := range stm.Unwrap() {
		fmt.Printf("evt %d: %v, ver: %s\n", i+1, event.TypeOf(evt.Event()), evt.Version())
	}
	fmt.Printf("account balance: %d\n", acc.Balance)

	// Output:
	// stream version: 00000000000000000001.0000000010
	// evt 1: bank.AccountOpened, ver: 00000000000000000001.0000000000
	// evt 2: bank.MoneyDeposited, ver: 00000000000000000001.0000000010
	// account balance: 1000
	// stream version: 00000000000000000002.0000000000
	// evt 1: bank.MoneyWithrawn, ver: 00000000000000000002.0000000000
	// account balance: 500
}
