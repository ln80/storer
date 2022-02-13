package event

import (
	"log"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {

	log.Println("---->", time.Now().UnixNano(), time.Now())
	type event struct{ Val string }
	type event2 struct{ Val string }

	reg := NewRegister("foo")
	defer reg.Clear()
	// if count := reg.Count(); count != 0 {
	// 	t.Errorf("Expected: O, got: %d", count)
	// }

	reg.
		Set(&event{}).
		Set(&event{})

	// fmt.Println(reg.Events())
	// if count := reg.Count(); count != 1 {
	// 	t.Errorf("Expected: 1, got: %d", count)
	// }

	if _, err := reg.Get("NoEvent"); err == nil {
		t.Errorf("Expected error, got nil")
	}

	e, err := reg.Get(TypeOf(&event{}))
	if err != nil {
		t.Error("Expected err to be nil, got", err)
	}

	if _, ok := e.(*event); !ok {
		t.Errorf("Expected casting to %s is ok, got false", TypeOf(&event{}))
	}

	globReg := NewRegister("")
	defer globReg.Clear()

	globReg.
		Set(&event2{})

	_, err = reg.Get(TypeOf(&event2{}))
	if err != nil {
		t.Error("Expected err to be nil, got", err)
	}

	_, err = globReg.Get(TypeOf(&event2{}))
	if err != nil {
		t.Error("Expected err to be nil, got", err)
	}
}
