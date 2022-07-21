package sqs

import (
	"reflect"
	"strconv"
	"testing"
)

func TestQueueMap(t *testing.T) {
	tcs := []struct {
		raw   string
		ok    bool
		count int
		hasWC bool
		wcs   []string
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
		{
			raw: `main=http://dopey-thickness.biz;
			admin=https://unkempt-verve.org;
			*=https://spotless-treaty.info;
			*.1=http://slimy-legging.net`,
			ok:    true,
			count: 4,
			hasWC: true,
			wcs:   []string{"*", "*.1"},
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			m, err := ParseQueueMap(tc.raw)
			if tc.ok {
				if want, got := tc.count, len(m); want != got {
					t.Fatalf("expect %d, %d be equals", want, got)
				}
				if tc.hasWC {
					if want, got := tc.wcs, m.WildCards(); !reflect.DeepEqual(want, got) {
						t.Fatalf("expect %v, %v be equals", want, got)
					}
				}
			} else {
				if err == nil {
					t.Fatal("expect err be not nil")
				}
			}
		})
	}
}
