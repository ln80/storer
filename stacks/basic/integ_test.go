package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/ln80/storer/dynamo"
	"github.com/ln80/storer/event"
	"github.com/ln80/storer/s3"
	"github.com/ln80/storer/stacks/utils"
	"github.com/ln80/storer/testutil"
)

func integTestEnvVars() (table, bucket, schedularFn string, err error) {
	table, bucket, schedularFn =
		os.Getenv("DYNAMODB_TABLE"),
		os.Getenv("S3_BUCKET"),
		os.Getenv("LAMBDA_SCHEDULAR_FUNC")

	if table == "" || bucket == "" || schedularFn == "" {
		err = fmt.Errorf(`
			missed env params:
				DYNAMODB_TABLE: %v,
				S3_BUCKET: %s,
				LAMBDA_SCHEDULAR_FUNC: %s,
		`, table, bucket, schedularFn)
	}
	return
}

func TestIntegration(t *testing.T) {
	ctx := context.Background()

	table, bucket, schedularFn, err := integTestEnvVars()
	if err != nil {
		log.Fatalf("failed to get env vars: %v", err)
	}

	dbsvc, s3svc, _, err := utils.InitAWSClients()
	if err != nil {
		log.Fatalf("failed to init aws client: %v", err)
	}

	store := dynamo.NewEventStore(dbsvc, table)
	streamer := s3.NewStreamMaintainer(s3svc, bucket)

	stmID := event.NewStreamID(event.UID().String(), "service")

	chunkSize := 20
	envs1, envs2 := event.Envelop(ctx, stmID, testutil.GenEvts(chunkSize)), event.Envelop(ctx, stmID, testutil.GenEvts(chunkSize))
	chunksFn := func() ([]event.Envelope, []event.Envelope) {
		return envs1, envs2
	}
	// make sure to make the total chunks size is gth 10
	// PartitionMinSize is 10 (see integration test stack params in Makefile)
	testutil.StoreAndStreamerIntegrationTest(t, store, streamer, stmID, false, chunksFn)

	// force the daily schedular execution
	// a signal must be triggered and processed. As a result, new partition will be created
	invoke, err := utils.InitLambdaClients()
	if err != nil {
		t.Fatal("expect err be nil got", err)
	}
	out, err := invoke.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(schedularFn),
	})
	if err != nil {
		t.Fatal("expect err be nil got", err)
	}
	log.Printf("invoke schedular lambda output: %d %v\n", out.StatusCode, string(out.Payload))

	// wait, streamer is eventually consistent
	time.Sleep(2 * time.Second)

	streamer = s3.NewStreamMaintainer(s3svc, bucket, func(cfg *s3.StreamerConfig) {
		// avoid replay events from chunks, only query the new created partitions
		cfg.ResumeWithLatestChunks = false
	})

	// avoid persist events twice
	// queried chunks of events must remains the same
	testutil.StoreAndStreamerIntegrationTest(t, store, streamer, stmID, true, chunksFn)
}
