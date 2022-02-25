package memory

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/event/sourcing"
)

type sortableUID string

type store struct {
	db map[string][]event.Envelope

	mu sync.RWMutex
	// global check points map is used to track global stream version
	gChkps map[string]event.Version
	// event Ids map is used to ensure append idempotency
	evtIDs map[string]sortableUID
}

type EventStore interface {
	event.Store
	sourcing.Store
	event.Streamer
}

var (
	_ EventStore = &store{}
)

func NewEventStore() EventStore {
	return &store{
		db:     make(map[string][]event.Envelope),
		gChkps: make(map[string]event.Version),
		evtIDs: make(map[string]sortableUID),
	}
}

func (s *store) Append(ctx context.Context, id event.StreamID, evts ...event.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.db[id.String()]; !ok {
		s.db[id.String()] = make([]event.Envelope, 0)
		s.evtIDs[id.String()] = ""
	}
	if _, ok := s.gChkps[id.GlobalID()]; !ok {
		s.gChkps[id.GlobalID()] = event.NewVersion()
	}
	evtl := len(evts)
	if evtl == 0 {
		return nil
	}

	if sortableUID(evts[0].ID()) <= s.evtIDs[id.String()] {
		return event.ErrAppendEventsConflict
	}
	s.evtIDs[id.String()] = sortableUID(evts[evtl-1].ID())

	for i := range evts {
		if rwenv, ok := evts[i].(interface {
			event.Envelope
			SetGlobalVersion(v event.Version) event.Envelope
		}); ok {
			rwenv.SetGlobalVersion(s.gChkps[id.GlobalID()])
			s.gChkps[id.GlobalID()] = s.gChkps[id.GlobalID()].Incr()
		}
	}

	s.db[id.String()] = append(s.db[id.String()], evts...)

	return nil
}

func (s *store) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envs, ok := s.db[id.String()]
	if !ok {
		return nil, nil
	}

	rl := len(trange)
	if rl == 0 {
		return envs, nil
	}
	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if since := trange[0]; ok && env.At().Before(since) {
			continue
		}
		if rl > 1 {
			if until := trange[1]; ok && env.At().After(until) {
				continue
			}
		}
		fenvs = append(fenvs, env)
	}
	return fenvs, nil
}

func (s *store) AppendToStream(ctx context.Context, chunk sourcing.Stream) error {
	if chunk.Empty() {
		return nil
	}
	if err := chunk.Validate(); err != nil {
		return err
	}

	// resolve stream current version and ensure that stream's chunks are in sequence
	s.mu.Lock()
	stmdb, ok := s.db[chunk.ID().String()]
	s.mu.Unlock()

	lastVer := event.VersionZero
	if ok {
		lastVer = stmdb[len(stmdb)-1].Version()
	}
	if !chunk.Version().Trunc().Equal(event.VersionMin) && !chunk.Version().Next(lastVer) {
		log.Println("v----->", chunk.Version())
		return event.Err(event.ErrAppendEventsFailed, chunk.ID().String(), "invalid chunk version, it must be in sequence with: "+lastVer.String())
	}

	return s.Append(ctx, chunk.ID(), chunk.Unwrap()...)
}

func (s *store) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envs, ok := s.db[id.String()]
	if !ok {
		return nil, nil
	}

	rl := len(vrange)
	if rl == 0 {
		return sourcing.NewStream(id, envs), nil
	}
	fenvs := make([]event.Envelope, 0)

	for _, env := range envs {
		env := env
		if from := vrange[0]; ok && env.Version().Before(from) {
			continue
		}
		if rl > 1 {
			if to := vrange[1]; ok && env.Version().After(to) {
				continue
			}
		}
		fenvs = append(fenvs, env)
	}
	return sourcing.NewStream(id, fenvs), nil
}

func (s *store) Replay(ctx context.Context, id event.StreamID, f event.StreamFilter, h event.StreamHandler) error {
	stmIDs := []string{}
	for k := range s.db {
		if strings.HasPrefix(k, id.String()) {
			stmIDs = append(stmIDs, k)
		}
	}
	if len(stmIDs) == 0 {
		return nil
	}

	envs := []event.Envelope{}
	for _, stmID := range stmIDs {
		envs = append(envs, s.db[stmID]...)
	}
	sort.Slice(envs, func(a, b int) bool {
		return envs[a].GlobalVersion().Before(envs[b].GlobalVersion())
	})

	f.Build()
	cur := event.NewCursor(id.String())
	for _, env := range envs {
		ignore, err := event.ValidateEvent(env, cur, func(v *event.Validation) {
			v.GlobalStream = id.Global()
			v.Filter = f
		})
		if err != nil {
			return err
		}
		if ignore {
			continue
		}
		if err := h(ctx, env); err != nil {
			return err
		}
	}
	return nil
}
