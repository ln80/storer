package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/redaLaanait/storer/dynamo"
	"github.com/redaLaanait/storer/signal"
	"github.com/redaLaanait/storer/sqs"
	"github.com/redaLaanait/storer/stacks/utils"
)

var (
	monitor signal.Monitor
	sender  signal.Sender
)

type handler func(ctx context.Context, event events.CloudWatchEvent) (err error)

func makeHandler(monitor signal.Monitor, sender signal.Sender) handler {
	return func(ctx context.Context, event events.CloudWatchEvent) (err error) {
		ctx = utils.HackCtx(ctx)
		var signals []*signal.ActiveStream

		t := time.Now()

		//
		beginday := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, -1)
		signals, err = monitor.ActiveStreams(ctx, beginday)
		if err != nil {
			return
		}

		defer sender.FlushBuffer(ctx, &err)
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
		log.Fatal(fmt.Errorf(`
			missed env params:
				DYNAMODB_TABLE: %v,
				SQS_QUEUE: %s
			`, table, queue))
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
