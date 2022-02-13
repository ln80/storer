package event

import "fmt"

func Err(err error, stmID string, extra ...interface{}) error {
	return fmt.Errorf("%w: stream=%s extra=%v", err, stmID, extra)
}
