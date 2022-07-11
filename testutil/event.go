package testutil

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/ln80/storer/event"
)

const (
	Dest2 = "dest2"
)

type Event1 struct {
	Val string
}
type Event2 struct {
	Val string
}

func (e *Event2) EvDests() []string {
	return []string{Dest2}
}

var _ event.Publishable = &Event2{}

func GenEvts(count int) []interface{} {
	evts := make([]interface{}, count)
	for i := 0; i < count; i++ {
		var evt interface{}
		if i%2 == 0 {
			evt = &Event2{"val " + strconv.Itoa(i)}
		} else {
			evt = &Event1{"val " + strconv.Itoa(i)}
		}

		evts[i] = evt
	}
	return evts
}

func FormatEnv(env event.Envelope) string {
	return fmt.Sprintf(`
		stmID: %s
		type: %s
		evtID: %s
		at: %v
		version: %v
		globalVersion: %v
		user: %s
		data: %v
	`, env.StreamID(), env.Type(), env.ID(), env.At().UnixNano(), env.Version(), env.GlobalVersion(), env.User(), env.Event())
}

func CmpEnv(env1, env2 event.Envelope) bool {
	return env1.ID() == env2.ID() &&
		env1.StreamID() == env2.StreamID() &&
		env1.GlobalStreamID() == env2.GlobalStreamID() &&
		env1.User() == env2.User() &&
		env1.At().Equal(env2.At()) &&
		env1.Version().Equal(env2.Version()) &&
		reflect.DeepEqual(env1.Event(), env2.Event())
}

func RegisterEvent(namespace string) event.Register {
	return event.NewRegister(namespace).
		Set(Event1{}).
		Set(Event2{})
}
