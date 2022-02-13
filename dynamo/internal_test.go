package dynamo

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/redaLaanait/storer/event"
)

func TestInternal_GSTM(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stream metadata test")
	}
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		// prepare global stream
		stmID := event.UID().String()
		ver := event.NewVersion()
		gstm0 := GSTM{
			StreamID: stmID,
			Item: Item{
				HashKey:  gstmHashKey(),
				RangeKey: gstmRangeKey(stmID),
			},
			UpdatedAt:   time.Now().UnixNano(),
			Version:     ver.String(),
			LastEventID: event.UID().String(),
		}

		// test gstm not persited yet
		if _, err := getGSTM(ctx, dbsvc, table, stmID); !errors.Is(err, ErrGSTMNotFound) {
			t.Fatalf("expect err %v to occur, got: %v", ErrGSTMNotFound, err)
		}
		// test persist and find gstm
		if err := persistGSTM(ctx, dbsvc, table, gstm0); err != nil {
			t.Fatalf("expect persist gstm, got err: %v", err)
		}
		rgstm, err := getGSTM(ctx, dbsvc, table, stmID)
		if err != nil {
			t.Fatalf("expect find gstm, got err: %v", err)
		}
		if !reflect.DeepEqual(&gstm0, rgstm) {
			t.Fatal("expect gstms be equal, got:", spew.Sdump(gstm0), spew.Sdump(rgstm))
		}

		// test persist gstm twice (idempotency)
		if err := persistGSTM(ctx, dbsvc, table, gstm0); err != nil {
			t.Fatalf("expect persist gstm, got err: %v", err)
		}
		// make sure that gstm does not altered
		rgstm, err = getGSTM(ctx, dbsvc, table, stmID)
		if err != nil {
			t.Fatalf("expect find gstm, got err: %v", err)
		}
		if !reflect.DeepEqual(&gstm0, rgstm) {
			t.Fatal("expect gstms be equal, got:", spew.Sdump(gstm0), spew.Sdump(rgstm))
		}

		// test move forward the stream checkpoint
		gstm1 := gstm0
		gstm1.Version = ver.Incr().String()
		gstm1.LastEventID = event.UID().String()
		gstm1.UpdatedAt = time.Now().UnixNano()
		if err := persistGSTM(ctx, dbsvc, table, gstm1); err != nil {
			t.Fatalf("expect persist gstm, got err: %v", err)
		}
		rgstm, err = getGSTM(ctx, dbsvc, table, stmID)
		if err != nil {
			t.Fatalf("expect find gstm, got err: %v", err)
		}
		if !reflect.DeepEqual(&gstm1, rgstm) {
			t.Fatal("expect gstms be equal, got:", spew.Sdump(gstm1), spew.Sdump(rgstm))
		}

		// test de-increment the checkpoint
		gstm02 := gstm1
		gstm02.Version = ver.String()
		gstm02.LastEventID = event.UID().String()
		gstm02.UpdatedAt = time.Now().UnixNano()

		// the write must fail and be ignored i.e return nil
		if err := persistGSTM(ctx, dbsvc, table, gstm02); err != nil {
			t.Fatalf("expect persist gstm, got err: %v", err)
		}
		rgstm, err = getGSTM(ctx, dbsvc, table, stmID)
		if err != nil {
			t.Fatalf("expect find gstm, got err: %v", err)
		}
		if reflect.DeepEqual(&gstm02, rgstm) {
			t.Fatal("expect gstms not be equal, got:", spew.Sdump(gstm02), spew.Sdump(rgstm))
		}
		if !reflect.DeepEqual(&gstm1, rgstm) {
			t.Fatal("expect gstms be equal, got:", spew.Sdump(gstm02), spew.Sdump(rgstm))
		}
	})
}

func TestInternal_GSTM_Batch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stream metadata test")
	}
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		stmIDs := []string{
			event.UID().String(),
			event.UID().String(),
			event.UID().String(),
			event.UID().String(),
		}

		// test find stms not yet persisted in table
		// this must return an init & invalid list of stms
		initstms, err := getGSTMBatch(ctx, dbsvc, table, stmIDs)
		if err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}
		if l, rl := len(initstms), len(stmIDs); l != rl {
			t.Fatalf("expect len(initstms), len(stmIDs) be equal, got: %d, %d", l, rl)
		}
		for _, stmID := range stmIDs {
			if stm, ok := initstms[stmID]; !ok || stmID != stm.StreamID {
				t.Fatalf("expect stm %v ID be %s", stm, stmID)
			}

			if err := initstms[stmID].Validate(); err == nil {
				t.Fatalf("expect init stm be invalid, got err: %v", err)
			}
		}

		// test persist stms in batch
		// populate init stms with valid data
		for _, stmID := range stmIDs {
			initstms[stmID].Version = event.NewVersion().Incr().String()
			initstms[stmID].LastEventID = event.UID().String()
			initstms[stmID].UpdatedAt = time.Now().UnixNano()
		}
		if err := persistGSTMBatch(ctx, dbsvc, table, initstms); err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}
		rstms, err := getGSTMBatch(ctx, dbsvc, table, stmIDs)
		if err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}
		if l, rl := len(rstms), len(stmIDs); l != rl {
			t.Fatalf("expect len(rstms) and len(stmIDs) be equal, got: %d, %d", l, rl)
		}
		if stm0, rstm0 := initstms[stmIDs[0]], rstms[stmIDs[0]]; !reflect.DeepEqual(stm0, rstm0) {
			t.Fatal("expect gstms be equal, got: ", *stm0, *rstm0)
		}

		// test partial stm update
		// corrupt the last stm and check that previous ones are correctly updated
		corruptstmID := stmIDs[len(stmIDs)-1]
		for stmID := range initstms {
			if stmID == corruptstmID {
				continue
			}
			initstms[stmID].Version = event.NewVersion().Incr().String()
			initstms[stmID].LastEventID = event.UID().String()
			initstms[stmID].UpdatedAt = time.Now().UnixNano()
		}
		initstms[corruptstmID].Version = event.VersionZero.String()
		if err = persistGSTMBatch(ctx, dbsvc, table, initstms); err == nil {
			t.Errorf("expect err %v, got nil", ErrValidateGSTMFailed)
		}
		rstms2, err := getGSTMBatch(ctx, dbsvc, table, stmIDs)
		if err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}

		for stmID, stm := range initstms {
			if stmID == corruptstmID {
				if reflect.DeepEqual(stm, rstms2[stmID]) {
					t.Fatal("expect gstms be not equal")
				}
				continue
			}
			if *stm != *initstms[stmID] {
				t.Fatalf("expect gstms be equal, got %v, %v", *stm, *initstms[stmID])
			}
		}
	})
}
