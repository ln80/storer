package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/ln80/storer/s3"
	"github.com/ln80/storer/signal"
	"github.com/ln80/storer/sqs"
	"github.com/ln80/storer/stacks/utils"
)

var (
	receiver   signal.Receiver
	maintainer s3.StreamMaintainer
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
		// err = errors.New("fake error test alarm worker")
	}
}

func init() {
	queue, bucket := os.Getenv("SQS_QUEUE"), os.Getenv("S3_BUCKET")
	if queue == "" || bucket == "" {
		log.Fatalf(`
			missed env params:
				SQS_QUEUE: %s,
				S3_BUCKET: %s
		`, queue, bucket)
	}

	_, s3svc, sqsvc, err := utils.InitAWSClients()
	if err != nil {
		log.Fatal(err)
	}

	receiver = sqs.NewSignalManager(sqsvc, queue)

	maintainer = s3.NewStreamMaintainer(s3svc, bucket, func(cfg *s3.StreamerConfig) {
		if val := os.Getenv("S3_PARTITION_MIN_SIZE"); val != "" {
			partsize, err := strconv.Atoi(val)
			if err != nil {
				log.Fatalf(`invalid S3_PARTITION_MIN_SIZE value: %s`, val)
			}
			cfg.PartitionMinSize = partsize
		}
	})
}

func main() {
	worker := signal.CombineProcessors([]signal.Processor{
		s3.MakeSignalProcessor(maintainer),
	})

	lambda.Start(makeHandler(receiver, worker))
}
