package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/ln80/storer/dynamo"
	"github.com/ln80/storer/internal/timeutil"
	"github.com/ln80/storer/signal"
	"github.com/ln80/storer/sqs"
	"github.com/ln80/storer/stacks/utils"
)

var (
	monitor signal.Monitor
	sender  signal.Sender
)

type handler func(ctx context.Context, event events.CloudWatchEvent) (err error)

func makeHandler(monitor signal.Monitor, sender signal.Sender) handler {
	return func(ctx context.Context, event events.CloudWatchEvent) (err error) {
		ctx = utils.HackCtx(ctx)

		defer sender.FlushBuffer(ctx, &err)

		var signals []*signal.ActiveStream

		since := timeutil.BeginningOfDay(time.Now().UTC())
		signals, err = monitor.ActiveStreams(ctx, since)
		if err != nil {
			return
		}

		for _, sig := range signals {
			if err = sender.Send(ctx, sig); err != nil {
				return
			}
		}

		return
	}
}

func init() {
	table, queue := os.Getenv("DYNAMODB_TABLE"), os.Getenv("SQS_QUEUE")
	if table == "" || queue == "" {
		log.Fatalf(`
			missed env params:
				DYNAMODB_TABLE: %v,
				SQS_QUEUE: %s
			`, table, queue)
	}
	dbsvc, _, sqsvc, err := utils.InitAWSClients()
	if err != nil {
		log.Fatal(err)
	}

	monitor = dynamo.NewMonitor(dbsvc, table)
	sender = sqs.NewSignalManager(sqsvc, queue)
}

func main() {
	lambda.Start(makeHandler(monitor, sender))
}
