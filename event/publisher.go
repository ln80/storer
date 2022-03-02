package event

import (
	"context"
)

// Publishable presents an event that must be forwared (in a push fashion) to some system Processors (ex: push-based Projectors)
// Note that not all events must be publishable,
// i.e poll-based Projectors may query the durable event store and replay a chunk of events in a regular basis
type Publishable interface{ EvDests() []string }

// Publisher presents the service responsible for publishing events to the given destinations
type Publisher interface {
	Publish(ctx context.Context, dest string, evts []Envelope) error
}

// RouteEvents implements the fan-out logic, i.e group event per destinations.
// an event may be routed to many destinations
func RouteEvents(mevs map[string][]Envelope) map[string][]Envelope {
	destEvts := make(map[string][]Envelope)
	for _, evs := range mevs {
		for _, ev := range evs {
			for _, d := range ev.Dests() {
				destEvts[d] = append(destEvts[d], ev)
			}
		}
	}
	return destEvts
}

// eventDests resolves destinations to publish event to.
// If the given event is not Publishable, then it checks the equivalent event from the global namespace.
// The latter is usualy a copy of the original, but likely defined in a global shared event-repo / go module.
func eventDests(ctx context.Context, evt interface{}) (dests []string) {
	pevt, ok := evt.(Publishable)
	if !ok {
		evt, err := NewRegisterFrom(ctx).Convert(pevt)
		if err != nil {
			return
		}
		pevt, ok = evt.(Publishable)
		if !ok {
			return
		}
	}
	dests = pevt.EvDests()
	return
}
