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

	// Publish a chunk of events to a given destination.
	// It may fails if the destination resource is not found in the publisher implementation config.
	// The implementation must deal with batch sent ops and retry logic.
	Publish(ctx context.Context, dest string, evts []Envelope) error

	// Broadcast is an advanced version of Publish method.
	// It takes a map of events grouped by global stream id.
	// It uses the default RouteEvents function defined in the event package to filter and group events by destination.
	// It also publish all given events to wildcards destination if they are supported by the publisher implementation.
	Broadcast(ctx context.Context, evts map[string][]Envelope) error
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
		evt, err := NewRegisterFrom(ctx).Convert(evt)
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
