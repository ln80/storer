package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type ClientAPI interface {
	SendMessageBatch(ctx context.Context,
		params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}
