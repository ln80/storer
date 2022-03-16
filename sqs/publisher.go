package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redaLaanait/storer/event"
)

var (
	ErrDestQueueNotFound  = errors.New("destination queue not found")
	ErrPublishEventFailed = errors.New("publish events failed")
)

type publisher struct {
	svc        ClientAPI
	queues     map[string]string
	serializer event.Serializer
}

func NewPublisher(sqsvc ClientAPI, queues map[string]string, ser event.Serializer) event.Publisher {
	return &publisher{
		svc:        sqsvc,
		queues:     queues,
		serializer: ser,
	}
}

var _ event.Publisher = &publisher{}

func (p *publisher) Publish(ctx context.Context, dest string, evts []event.Envelope) error {
	if len(evts) == 0 {
		return nil
	}
	stmID := evts[0].StreamID()

	// skip publish if queues map is empty (still not intuitive)
	if p.queues == nil {
		// return event.Err(ErrDestQueueNotFound, stmID, "dest: "+dest)
		return nil
	}

	queue, ok := p.queues[dest]
	if !ok {
		return event.Err(ErrDestQueueNotFound, stmID, "dest: "+dest)
	}

	entries := make([]types.SendMessageBatchRequestEntry, 0, len(evts))
	for _, e := range evts {
		msg, err := p.serializer.MarshalEvent(e)
		if err != nil {
			return err
		}

		msgGroupID := e.GlobalStreamID()
		if evt, ok := e.Event().(interface{ EvMsgGroupID() string }); ok {
			msgGroupID = evt.EvMsgGroupID()
		}

		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                     aws.String(e.ID()),
			MessageGroupId:         aws.String(msgGroupID),
			MessageDeduplicationId: aws.String(e.ID()),
			MessageBody:            aws.String(string(msg)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"Type": {
					DataType:    aws.String("String"),
					StringValue: aws.String(e.Type()),
				},
				"Version": {
					DataType:    aws.String("String"),
					StringValue: aws.String(e.Version().String()),
				},
				"GlobalStreamID": {
					DataType:    aws.String("String"),
					StringValue: aws.String(e.GlobalStreamID()),
				},
				"GlobalVersion": {
					DataType:    aws.String("Number"),
					StringValue: aws.String(e.GlobalVersion().String()),
				},
				"At": {
					DataType:    aws.String("Number"),
					StringValue: aws.String(fmt.Sprintf("%d", e.At().UnixNano())),
				},
			},
		})
	}
	if _, err := p.svc.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(queue),
	}); err != nil {
		return event.Err(ErrPublishEventFailed, stmID, "dest: "+dest, err.Error())
	}
	return nil
}
