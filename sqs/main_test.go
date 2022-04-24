package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type clientMock struct {
	err    error
	traces map[string][]types.SendMessageBatchRequestEntry
}

var _ ClientAPI = &clientMock{}

func (c *clientMock) SendMessageBatch(ctx context.Context,
	params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.traces == nil {
		c.traces = make(map[string][]types.SendMessageBatchRequestEntry)
	}
	if _, ok := c.traces[*params.QueueUrl]; !ok {
		c.traces[*params.QueueUrl] = []types.SendMessageBatchRequestEntry{}
	}
	c.traces[*params.QueueUrl] = append(c.traces[*params.QueueUrl], params.Entries...)

	return &sqs.SendMessageBatchOutput{}, nil
}
