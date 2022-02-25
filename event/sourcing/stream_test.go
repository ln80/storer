package sourcing

import (
	"context"
	"reflect"
	"testing"

	"github.com/redaLaanait/storer/event"
)

func TestStream(t *testing.T) {
	ctx := context.Background()

	type Event struct{ Val string }

	evts := []interface{}{
		&Event{Val: "1"},
		&Event{Val: "2"},
	}

	stmID := event.NewStreamID("gstmID")

	stm := Envelop(ctx, stmID, event.VersionZero, evts)

	if want, val := stmID, stm.ID(); want.String() != val.String() {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := event.NewVersion().Add(0, 10), stm.Version(); !want.Equal(val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := evts, stm.Unwrap().Events(); !reflect.DeepEqual(want, val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
}
