package sqs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/json"
)

const (
	// SQS size Msg Limit is 256 KB.
	// 6KB for meta-data + safety margin
	MsgSizeLimit = 256000 // 250 KB

	// SQS size Msg Batch Limit is 256 KB.
	// 6KB for meta-data + safety margin
	MsgBatchSizeLimit = 256000 // 250 KB

	// SQS Msg batch entries limit is 10
	MsgBatchEntriesLimit = 10
)

var (
	ErrDestQueueNotFound          = errors.New("destination queue not found")
	ErrPublishEventFailed         = errors.New("publish events failed")
	ErrPublishInvalidMsgSizeLimit = errors.New("publish failed, message exceeds size limit")
)

type publisher struct {
	svc    ClientAPI
	queues map[string]string
	*PublisherConfig
}

type PublisherConfig struct {
	Serializer event.Serializer
}

func NewPublisher(svc ClientAPI, queues map[string]string, opts ...func(cfg *PublisherConfig)) event.Publisher {
	if svc == nil {
		panic("event publisher invalid SQS client: nil value")
	}
	pub := &publisher{
		svc:    svc,
		queues: queues,
		PublisherConfig: &PublisherConfig{
			Serializer: json.NewEventSerializer(""),
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(pub.PublisherConfig)
	}

	return pub
}

var _ event.Publisher = &publisher{}

func (p *publisher) Publish(ctx context.Context, dest string, evts []event.Envelope) error {
	if len(evts) == 0 {
		return nil
	}

	// skip publish if queues map is empty
	if p.queues == nil {
		return nil
	}

	queue, ok := p.queues[dest]
	if !ok {
		return fmt.Errorf("%w: %s in %v", ErrDestQueueNotFound, dest, p.queues)
	}

	entries := make([]types.SendMessageBatchRequestEntry, 0)
	totalSize := 0
	appendFn := func(entry types.SendMessageBatchRequestEntry, entrySize int) {
		entries = append(entries, entry)
		totalSize += entrySize
	}
	resetFn := func() {
		entries = make([]types.SendMessageBatchRequestEntry, 0)
		totalSize = 0
	}

	for _, e := range evts {
		msg, err := p.Serializer.MarshalEvent(e)
		if err != nil {
			return err
		}

		msgSize := len([]byte(msg))
		if msgSize > MsgSizeLimit {
			return fmt.Errorf("%w: event details: (type: %s, id: %s, size: %d)",
				ErrPublishInvalidMsgSizeLimit, e.Type(), e.ID(), msgSize)
		}

		// resolve message group ID
		msgGroupID := e.GlobalStreamID()
		if evt, ok := e.Event().(interface{ EvMsgGroupID() string }); ok {
			msgGroupID = evt.EvMsgGroupID()
		}

		entry := types.SendMessageBatchRequestEntry{
			Id:                     aws.String(e.ID()),
			MessageGroupId:         aws.String(msgGroupID),
			MessageDeduplicationId: aws.String(e.ID()),
			MessageBody:            aws.String(string(msg)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"GSTMID": {
					DataType:    aws.String("String"),
					StringValue: aws.String(e.GlobalStreamID()),
				},
				"GVer": {
					DataType:    aws.String("Number"),
					StringValue: aws.String(e.GlobalVersion().String()),
				},
				"Type": {
					DataType:    aws.String("String"),
					StringValue: aws.String(e.Type()),
				},
			},
		}

		sizeLimitAlreadyReached := false
		if sizeLimitAlreadyReached = totalSize+msgSize > MsgBatchSizeLimit; !sizeLimitAlreadyReached {
			appendFn(entry, msgSize)
		}
		if len(entries) == MsgBatchEntriesLimit || sizeLimitAlreadyReached {
			if err := p.doSendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				Entries:  entries,
				QueueUrl: aws.String(queue),
			}); err != nil {
				return err
			}
			resetFn()

			if sizeLimitAlreadyReached {
				appendFn(entry, msgSize)
			}
		}
	}

	if len(entries) == 0 {
		return nil
	}

	return p.doSendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(queue),
	})
}

func (p *publisher) doSendMessageBatch(ctx context.Context, input *sqs.SendMessageBatchInput) error {
	out, err := p.svc.SendMessageBatch(ctx, input)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPublishEventFailed, err)
	}

	attempts := 1
	for out != nil && len(out.Failed) > 0 && attempts <= 5 {
		time.Sleep(time.Duration(attempts) * 50 * time.Millisecond)
		out, err = p.svc.SendMessageBatch(ctx, input)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPublishEventFailed, err)
		}
		attempts++
	}

	if out != nil && len(out.Failed) > 0 {
		return fmt.Errorf("%w: failed entries: %v", ErrPublishEventFailed, out.Failed)
	}
	return nil
}
