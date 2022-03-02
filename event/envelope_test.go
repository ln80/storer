package event

import (
	"context"
	"testing"
	"time"

	"github.com/redaLaanait/storer/event/testutil"
)

func TestEnvelope(t *testing.T) {
	ctx := context.Background()

	gstmID := "tenantID"
	stmID := NewStreamID(gstmID, "service", "rootEntityID")

	t.Run("test envelop event", func(t *testing.T) {
		evts := []interface{}{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		envs := Envelop(ctx, stmID, evts)
		for i, env := range envs {
			if want, val := gstmID, env.GlobalStreamID(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := stmID.String(), env.StreamID(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := VersionZero, env.Version(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := VersionZero, env.GlobalVersion(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := evts[i], env.Event(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := "", env.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOf(testutil.Event{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOf(testutil.Event2{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
			if ok := env.At().After(time.Now().Add(-1 * time.Second)); !ok {
				t.Fatalf("expect %v be less than few second ago", env.At())
			}
			if nowant, val := "", env.ID(); nowant == val {
				t.Fatalf("expect %v, %v be not equals", nowant, val)
			}
		}
	})

	t.Run("test envelop event with context values", func(t *testing.T) {
		user := "Joyce Pfeffer IV"
		ctx := context.WithValue(
			context.WithValue(ctx, ContextUserKey, user),
			ContextNamespaceKey, "foo")

		evts := []interface{}{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		envs := Envelop(ctx, stmID, evts)
		for i, env := range envs {
			if want, val := user, env.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOfWithContext(ctx, testutil.Event{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOfWithContext(ctx, testutil.Event2{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
		}
	})

	t.Run("test envelop event with options", func(t *testing.T) {
		evts := []interface{}{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		user := "Joyce Pfeffer IV"
		tm := time.Now().AddDate(0, 0, -1)
		tm0 := tm
		ver := NewVersion()
		ver0 := ver

		envs := Envelop(ctx, stmID, evts,
			func(env RWEnvelope) {
				env.SetAt(tm)
				tm = tm.Add(-5 * time.Second)
			},
			func(env RWEnvelope) {
				env.SetUser(user)
			},
			func(env RWEnvelope) {
				env.SetVersion(ver)
				env.SetGlobalVersion(ver)
				ver = ver.Incr()
			},
		)
		for i, env := range envs {
			if want, val := user, env.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := ver0.Add(uint64(i), 0), env.Version(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := ver0.Add(uint64(i), 0), env.GlobalVersion(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOf(testutil.Event{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOf(testutil.Event2{}), env.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
			if want, val := tm0.Add(-5*time.Duration(i)*time.Second), env.At(); !val.Equal(want) {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}
	})

	t.Run("test envelop event with version incr", func(t *testing.T) {
		evts := []interface{}{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		ver0 := NewVersion()
		envs := Envelop(ctx, stmID, evts,
			WithVersionIncr(ver0, VersionSeqDiff10p0),
		)
		for i, env := range envs {
			if want, val := ver0.Add(uint64(i), 0), env.Version(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}
	})
}
