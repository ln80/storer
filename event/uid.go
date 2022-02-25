package event

import (
	"github.com/rs/xid"
)

type ID interface {
	String() string
	// Time() time.Time
}

func UID() ID {
	return xid.New()
}

// func UIDFrom(text string) (ID, error) {
// 	return xid.FromString(text)
// }
