package event

import (
	"context"
)

type Publisher interface {
	Publish(ctx context.Context, dest string, evts []Envelope) error
}

func RouteEvents(mevs map[string][]Envelope) map[string][]Envelope {
	destEvts := make(map[string][]Envelope)

	for _, evs := range mevs {
		for _, ev := range evs {
			dests := eventDests(ev.Event())
			for _, d := range dests {
				destEvts[d] = append(destEvts[d], ev)
			}
		}
	}
	return destEvts
}

func eventDests(ev interface{}) []string {
	if ev, ok := ev.(interface{ Dests() []string }); ok {
		return ev.Dests()
	}

	return []string{}
}
