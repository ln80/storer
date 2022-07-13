package storer

import (
	"github.com/ln80/storer/dynamo"
	"github.com/ln80/storer/event"
	"github.com/ln80/storer/event/sourcing"
	"github.com/ln80/storer/memory"
	"github.com/ln80/storer/s3"
)

// EventStore combines event-sourcing store, event-logging store, and steamer interfaces
type EventStore interface {
	event.Store
	event.Streamer
	sourcing.Store
}

type dynamodbS3Store struct {
	// dynamo.EventStore satisfies both event.Store and sourcing.Store
	dynamo.EventStore
	// event.Streamer is supposed to contain the implementation found in S3 package
	event.Streamer
}

// interface guard
var _ EventStore = &dynamodbS3Store{}

// NewDynamodbS3Store returns an event store implementation on top of Dynamodb and S3:
// Aggregate stream are saved and streamed from Dynamodb table.
// Global stream is streamed / replayed from S3 with good-enough order guaranteed.
// It may panics if any of resquired of parameters are missing.
func NewDynamodbS3Store(dbsvc dynamo.ClientAPI, table string, s3svc s3.ClientAPI, bucket string) EventStore {
	return &dynamodbS3Store{
		EventStore: dynamo.NewEventStore(dbsvc, table),
		Streamer:   s3.NewStreamer(s3svc, bucket),
	}
}

// interface guard
var _ EventStore = memory.EventStore(nil)

// NewInMemoryStore returns an in-memory event store, mainly used for tests.
func NewInMemoryStore() EventStore {
	return memory.NewEventStore()
}
