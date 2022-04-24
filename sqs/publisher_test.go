package sqs

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/testutil"
)

const msgGroupID = "eventGrpID"

type event1 struct{ Val string }

type event2 struct{ Val string }

func (evt *event2) EvMsgGroupID() string {
	return msgGroupID
}

func TestEventPublisher(t *testing.T) {
	ctx := context.Background()

	stmID := event.UID().String()
	queue := "http://queue.url"

	dest1 := "dest1"
	destNotFound := "destNotFound"

	evts := []interface{}{
		&event1{
			Val: "test content 1",
		},
		&event1{
			Val: "test content 2",
		},
		&event2{ // Note: the 3rd event belongs to a different Message Group
			Val: "test content 3",
		},
	}
	// Batch has 14 events in total. This applies two SQS batches to be saved.
	for i := 4; i < 15; i++ {
		evts = append(evts, &event1{
			Val: "test content " + strconv.Itoa(i),
		})
	}

	evtAt := time.Now()
	envs := event.Envelop(ctx, event.NewStreamID(stmID), evts, func(env event.RWEnvelope) {
		env.SetAt(evtAt)
		evtAt = evtAt.Add(1 * time.Minute)
	})

	t.Run("test publish with empty queues map", func(t *testing.T) {
		sqsvc := &clientMock{}
		pub := NewPublisher(sqsvc, nil)
		if err := pub.Publish(ctx, dest1, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
	})

	t.Run("test publish with queue dest not found", func(t *testing.T) {
		pub := NewPublisher(&clientMock{}, map[string]string{dest1: queue})
		if wanterr, err := ErrDestQueueNotFound, pub.Publish(ctx, destNotFound, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test publish with infra error", func(t *testing.T) {
		sqsvc := &clientMock{err: errors.New("infra error")}
		pub := NewPublisher(sqsvc, map[string]string{dest1: queue})
		if wanterr, err := ErrPublishEventFailed, pub.Publish(ctx, dest1, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test publish with invalid msg size error", func(t *testing.T) {
		sqsvc := &clientMock{}
		pub := NewPublisher(sqsvc, map[string]string{dest1: queue})

		evtAt := time.Now()
		envs := event.Envelop(ctx, event.NewStreamID(stmID), []interface{}{
			&event1{
				Val: testutil.RandStringValueOf(int(300 * 1000)),
			},
		}, func(env event.RWEnvelope) {
			env.SetAt(evtAt)
			evtAt = evtAt.Add(1 * time.Minute)
		})

		if wanterr, err := ErrPublishInvalidMsgSizeLimit, pub.Publish(ctx, dest1, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test successfully publish events", func(t *testing.T) {
		sqsvc := &clientMock{}
		queues := map[string]string{dest1: queue}
		pub := NewPublisher(sqsvc, queues)
		if err := pub.Publish(ctx, dest1, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantl, l := len(envs), len(sqsvc.traces[queues[dest1]]); wantl != l {
			t.Fatalf("expect traces len be %v, got %d", wantl, l)
		}
		if wantgrp, grp := msgGroupID, *sqsvc.traces[queues[dest1]][2].MessageGroupId; wantgrp != grp {
			t.Fatalf("expect group ids be equals, got %s, %s", wantgrp, grp)
		}
	})
}
