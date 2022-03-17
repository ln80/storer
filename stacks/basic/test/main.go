package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/redaLaanait/storer/dynamo"
	"github.com/redaLaanait/storer/json"
	"github.com/redaLaanait/storer/s3"
	"github.com/redaLaanait/storer/stacks/utils"
	"github.com/redaLaanait/storer/testutil"
)

var (
	dbsvc           dynamo.ClientAPI
	s3svc           s3.ClientAPI
	integrationTest func(ctx context.Context) error
)

func main() {
	lambda.Start(func(ctx context.Context, _ interface{}) error {
		return integrationTest(ctx)
	})
}

func init() {
	table, bucket := os.Getenv("DYNAMODB_TABLE"), os.Getenv("S3_BUCKET")
	if table == "" || bucket == "" {
		log.Fatal(fmt.Errorf(`
			missed env params:
				DYNAMODB_TABLE: %v,
				S3_BUCKET: %s
		`, table, bucket))
	}

	var err error
	dbsvc, s3svc, _, err = utils.InitAWSClients()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to init aws client: %w", err))
	}

	store := dynamo.NewEventStore(dbsvc, table)

	streamer := s3.NewStreamManager(s3svc, bucket, json.NewEventSerializer(""))

	integrationTest = testutil.MakeIntegrationTest_StoreAndStreamer(store, streamer)
}
