package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/redaLaanait/storer/dynamo"
	"github.com/redaLaanait/storer/json"
	"github.com/redaLaanait/storer/s3"
	"github.com/redaLaanait/storer/sqs"
	"github.com/redaLaanait/storer/stacks/utils"
)

var fwd dynamo.Forwarder

func init() {
	table, bucket := os.Getenv("DYNAMODB_TABLE"), os.Getenv("S3_BUCKET")
	if table == "" || bucket == "" {
		log.Fatal(fmt.Errorf(`
			missed env params:
				DYNAMODB_TABLE: %v,
				S3_BUCKET: %s
		`, table, bucket))
	}
	queues, err := utils.ParseStringToMap(os.Getenv("SQS_PUBLISH_QUEUES"))
	if err != nil {
		log.Fatal(fmt.Errorf("invalid sqs queues env param %s, err: %w", os.Getenv("SQS_QUEUES"), err))
	}
	//aa

	dbsvc, s3svc, sqsvc, err := utils.InitAWSClients()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to init aws client: %w", err))
	}

	ser := json.NewEventSerializer("")

	fwd = dynamo.NewForwarder(dbsvc, table,
		s3.NewStreamManager(s3svc, bucket, ser),
		sqs.NewPublisher(sqsvc, queues, ser),
		ser,
	)
}

type handler func(ctx context.Context, event events.DynamoDBEvent) error

func makeHandler(fwd dynamo.Forwarder) handler {
	return func(ctx context.Context, event events.DynamoDBEvent) error {
		ctx = utils.HackCtx(ctx)
		recs := []dynamo.Record{}
		for _, ev := range event.Records {
			if key := ev.Change.NewImage["_pk"].String(); key == "internal" {
				continue
			}
			switch ev.EventName {
			case "INSERT":
				rec := dynamo.Record{
					Item: dynamo.Item{
						HashKey:  ev.Change.NewImage["_pk"].String(),
						RangeKey: ev.Change.NewImage["_sk"].String(),
					},
				}
				attrMap, err := utils.FromDynamoDBEventAVMap(ev.Change.NewImage)
				if err != nil {
					return err
				}
				if err := attributevalue.UnmarshalMap(attrMap, &rec); err != nil {
					return err
				}
				if rec.HashKey == "" {
					continue
				}
				recs = append(recs, rec)
			default:
				log.Printf("event store must be immutable, unauthorized action: %s change: %v", ev.EventName, ev.Change)
			}
		}
		if len(recs) == 0 {
			return nil
		}

		return fwd.Forward(ctx, recs)
	}
}

func main() {
	lambda.Start(makeHandler(fwd))
}
