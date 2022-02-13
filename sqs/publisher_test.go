package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/json"
)

type Event1 struct{ Val string }

type Event2 struct{ Val string }

func (evt *Event2) EvMsgGroupID() string {
	return "Event2grpID"
}

type clientMock struct {
	err    error
	traces map[string][]types.SendMessageBatchRequestEntry
}

func (c *clientMock) SendMessageBatch(ctx context.Context,
	params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.traces == nil {
		c.traces = make(map[string][]types.SendMessageBatchRequestEntry)
	}
	c.traces[*params.QueueUrl] = params.Entries

	return nil, nil
}

var _ ClientAPI = &clientMock{}

func TestPublisher(t *testing.T) {
	ctx := context.Background()
	ser := json.NewEventSerializer("")
	stmID := event.UID().String()

	dest1 := "dest1"
	dest2 := "dest2"

	evtAt := time.Now()
	envs := event.Envelop(ctx, event.NewStreamID(stmID), []interface{}{
		&Event1{
			Val: "test content 1",
		},
		&Event1{
			Val: "test content 2",
		},
		&Event2{ // the 3rd event has a custom group ID
			Val: "test content 3",
		},
	}, func(env event.RWEnvelope) {
		env.SetAt(evtAt)
		evtAt = evtAt.Add(1 * time.Minute)
	})

	t.Run("test publish with empty queues map", func(t *testing.T) {
		sqsvc := &clientMock{}

		pub := NewPublisher(sqsvc, nil, ser)

		if wanterr, err := ErrDestQueueNotFound, pub.Publish(ctx, dest1, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test publish with queue dest not found", func(t *testing.T) {
		pub := NewPublisher(&clientMock{}, map[string]string{dest1: "http://queue.url"}, ser)

		if wanterr, err := ErrDestQueueNotFound, pub.Publish(ctx, dest2, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test publish with infra error", func(t *testing.T) {
		sqsvc := &clientMock{err: errors.New("infra error")}
		pub := NewPublisher(sqsvc, map[string]string{dest1: "http://queue.url"}, ser)
		if wanterr, err := ErrPublishEventFailed, pub.Publish(ctx, dest1, envs); !errors.Is(err, wanterr) {
			t.Fatalf("expect err be %v, got %v", wanterr, err)
		}
	})

	t.Run("test successfully publish events", func(t *testing.T) {
		sqsvc := &clientMock{}
		queues := map[string]string{dest1: "http://queue.url"}
		pub := NewPublisher(sqsvc, queues, ser)
		if err := pub.Publish(ctx, dest1, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantl, l := len(envs), len(sqsvc.traces[queues[dest1]]); wantl != l {
			t.Fatalf("expect traces len be equal, got %d, %d", wantl, l)
		}
		if wantgrp, grp := "Event2grpID", *sqsvc.traces[queues[dest1]][2].MessageGroupId; wantgrp != grp {
			t.Fatalf("expect group ids be equals, got %s, %s", wantgrp, grp)
		}
	})

}
