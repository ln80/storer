package timeutil

import "time"

func BeginningOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func EndOfDay(t time.Time) time.Time {
	return BeginningOfDay(t).AddDate(0, 0, 1).Add(-time.Nanosecond)
}

func NextDay(t time.Time) time.Time {
	return BeginningOfDay(t).AddDate(0, 0, 1)
}

func DateEqual(t1, t2 time.Time) bool {
	y1, m1, d1 := t1.Date()
	y2, m2, d2 := t2.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}
