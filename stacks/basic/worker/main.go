package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/json"
	"github.com/redaLaanait/storer/s3"
	"github.com/redaLaanait/storer/signal"
	"github.com/redaLaanait/storer/sqs"
	"github.com/redaLaanait/storer/stacks/utils"
)

var (
	receiver signal.Receiver
	merger   s3.StreamMerger
)

type handler func(ctx context.Context, event events.SQSEvent) (err error)

func makeHandler(receiver signal.Receiver, worker signal.Processor) handler {
	return func(ctx context.Context, event events.SQSEvent) error {
		ctx = utils.HackCtx(ctx)
		data := make([][]byte, 0)
		for _, rec := range event.Records {
			data = append(data, []byte(rec.Body))
		}

		return receiver.Receive(ctx, data, worker)
	}
}

func makeWorker(merger s3.StreamMerger) signal.Processor {
	return func(ctx context.Context, sig signal.Signal) error {
		switch sig := sig.(type) {
		case *signal.ActiveStream:
			rng := event.StreamFilter{
				Since: time.Unix(0, sig.Since),
				Until: time.Unix(0, sig.Until),
			}

			return merger.MergeChunks(ctx, event.NewStreamID(sig.StreamID()), rng)
		}

		// TODO return error if signal is unrecognized ??

		return nil
	}
}

func init() {
	queue, bucket := os.Getenv("SQS_QUEUE"), os.Getenv("S3_BUCKET")
	if queue == "" || bucket == "" {
		log.Fatal(fmt.Errorf(`
			missed env params:
				SQS_QUEUE: %s,
				S3_BUCKET: %s
		`, queue, bucket))
	}

	_, s3svc, sqsvc, err := utils.InitAWSClients()
	if err != nil {
		log.Fatal(err)
	}

	ser := json.NewEventSerializer("")
	receiver = sqs.NewSignalManager(sqsvc, queue)
	merger = s3.NewStreamManager(s3svc, bucket, ser)
}

func main() {
	lambda.Start(makeHandler(receiver, makeWorker(merger)))
}
