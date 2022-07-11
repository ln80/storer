package dynamo

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/ln80/storer/event"
)

func genGSTMTestEvt(ctx context.Context, stmID event.StreamID) event.Envelope {
	return event.Envelop(ctx, stmID, []interface{}{struct{}{}})[0]
}

func TestGSTM(t *testing.T) {
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		// prepare global stream
		stmID := event.UID().String()
		gver := event.NewVersion()
		gstm0 := GSTM{
			StreamID: stmID,
			Item: Item{
				HashKey:  gstmHashKey(),
				RangeKey: gstmRangeKey(stmID),
			},
		}

		// update gstm
		if err := gstm0.Update(genGSTMTestEvt(ctx, event.NewStreamID(stmID)), gver); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// test last active day record is acurate
		if lastActiveDay := gstm0.LastActiveDay(time.Now()); lastActiveDay != nil {
			t.Fatalf("expect last active day be nil, got %v", lastActiveDay)
		}
		if want, got :=
			(&ActiveDay{
				Day:     time.Now().Format("2006/01/02"),
				Version: gver.String(),
			}), gstm0.LastActiveDay(time.Now().AddDate(0, 0, 1)); !reflect.DeepEqual(want, got) {
			t.Fatalf("expect %v, %v, be equals", want, got)
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
		if err := gstm1.Update(genGSTMTestEvt(ctx, event.NewStreamID(stmID)), gver.Incr()); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
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
		gstm02.Update(genGSTMTestEvt(ctx, event.NewStreamID(stmID)), gver)

		// the write must silently fail and be ignored for idempotency reason
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
	})
}

func TestGSTM_Batch(t *testing.T) {
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {
		stmIDs := []string{
			event.UID().String(),
			event.UID().String(),
			event.UID().String(),
			event.UID().String(),
		}

		// test find streams not yet persisted in table
		// this must return an empty list of streams
		initstms, err := getGSTMBatch(ctx, dbsvc, table, GSTMFilter{
			StreamIDs: stmIDs,
		})
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

		// test persist gstms in batch

		// populate initial gstms with valid data
		for _, stmID := range stmIDs {
			initstms[stmID].Update(
				genGSTMTestEvt(ctx, event.NewStreamID(stmID)), event.NewVersion().Incr())
		}
		if err := persistGSTMBatch(ctx, dbsvc, table, initstms); err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}
		rstms, err := getGSTMBatch(ctx, dbsvc, table, GSTMFilter{
			StreamIDs: stmIDs,
		})
		if err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}
		if l, rl := len(rstms), len(stmIDs); l != rl {
			t.Fatalf("expect len(rstms) and len(stmIDs) be equal, got: %d, %d", l, rl)
		}
		if stm0, rstm0 := initstms[stmIDs[0]], rstms[stmIDs[0]]; !reflect.DeepEqual(stm0, rstm0) {
			t.Fatal("expect gstms be equal, got: ", *stm0, *rstm0)
		}

		// test partial gstms update
		// corrupt the last gstm and check that previous ones are correctly updated
		corruptedstmID := stmIDs[len(stmIDs)-1]
		for stmID := range initstms {
			if stmID == corruptedstmID {
				continue
			}
			initstms[stmID].Update(
				genGSTMTestEvt(ctx, event.NewStreamID(stmID)), event.NewVersion().Incr())
		}
		initstms[corruptedstmID].Version = event.VersionZero.String()

		if err = persistGSTMBatch(ctx, dbsvc, table, initstms); err == nil {
			t.Errorf("expect err %v, got nil", ErrValidateGSTMFailed)
		}
		rstms2, err := getGSTMBatch(ctx, dbsvc, table, GSTMFilter{
			StreamIDs: stmIDs,
		})
		if err != nil {
			t.Fatalf("expect err be nil, got err: %v", err)
		}

		for stmID, stm := range initstms {
			if stmID == corruptedstmID {
				if reflect.DeepEqual(stm, rstms2[stmID]) {
					t.Fatal("expect gstms be not equal")
				}
				continue
			}
			if reflect.DeepEqual(*stm, initstms[stmID]) {
				t.Fatalf("expect gstms be equal, got %v, %v", spew.Sdump(stm), spew.Sdump((initstms[stmID])))
			}
		}
	})
}
