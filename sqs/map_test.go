package sqs

import (
	"strconv"
	"testing"
)

func TestQueueMap(t *testing.T) {
	tcs := []struct {
		raw   string
		ok    bool
		count int
	}{
		{
			raw: "main=;",
			ok:  false,
		},
		{
			raw: "main=;admin=",
			ok:  false,
		},
		{
			raw: "main=http://dopey-thickness.biz;;",
			ok:  false,
		},
		{
			raw: "main=dopey-thickness.biz;",
			ok:  false,
		},
		{
			raw: "main=http://dopey-thickness.biz;admin=boo",
			ok:  false,
		},
		{
			raw: "main=http://dopey-thickness.biz;admin",
			ok:  false,
		},
		{
			raw: "",
			ok:  true,
		},
		{
			raw:   "main=http://dopey-thickness.biz;admin=https://unkempt-verve.org",
			ok:    true,
			count: 2,
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			m, err := ParseQueueMap(tc.raw)
			if tc.ok {
				if want, got := tc.count, len(m); want != got {
					t.Fatalf("expect %d, %d be equals", want, got)
				}
			} else {
				if err == nil {
					t.Fatal("expect err be not nil")
				}
			}
		})
	}
}
