package event

import (
	"context"
	"testing"
)

type eventToPub1 struct{ Val string }

func (evt *eventToPub1) Dests() []string {
	return []string{"dest1"}
}

type eventToPub2 struct{ Val string }

func (evt *eventToPub2) Dests() []string {
	return []string{"dest2", "dest3"}
}
func TestPublisher(t *testing.T) {
	evts := []interface{}{
		&eventToPub1{},
		&eventToPub1{},
		&eventToPub2{},
	}
	envs := Envelop(context.Background(), NewStreamID("gstmID"), evts)
	menv := map[string][]Envelope{
		"gstmID": {},
	}
	for _, env := range envs {
		menv[env.GlobalStreamID()] = append(menv[env.GlobalStreamID()], env)
	}

	denvs := RouteEvents(menv)
	if wantl, l := 3, len(denvs); wantl != l {
		t.Fatalf("expect %d, %d be equals", wantl, l)
	}
	for dest, envs := range denvs {
		switch dest {
		case "dest1":
			if wantl, l := 2, len(envs); wantl != l {
				t.Fatalf("expect %d, %d be equals", wantl, l)
			}
			for i, env := range envs {
				if want, val := evts[i], env.Event(); want != val {
					t.Fatalf("expect %d, %d be equals", want, val)
				}
			}
		case "dest2":
			if wantl, l := 1, len(envs); wantl != l {
				t.Fatalf("expect %d, %d be equals", wantl, l)
			}
			if want, val := evts[2], envs[0].Event(); want != val {
				t.Fatalf("expect %d, %d be equals", want, val)
			}
		case "dest3":
			if wantl, l := 1, len(envs); wantl != l {
				t.Fatalf("expect %d, %d be equals", wantl, l)
			}
			if want, val := evts[2], envs[0].Event(); want != val {
				t.Fatalf("expect %d, %d be equals", want, val)
			}
		}
	}
}
