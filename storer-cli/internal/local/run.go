package local

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/ln80/storer/dynamo"
	_s3 "github.com/ln80/storer/s3"
	_sqs "github.com/ln80/storer/sqs"
)

func checkEndpoint(value string, name string) error {
	if _, err := url.ParseRequestURI(value); err != nil {
		return fmt.Errorf("invalid '%s' endpoint: %s", name, value)
	}
	return nil
}

func Run(ctx context.Context, appName, dynamodbEndpoint, s3Endpoint, sqsEndpoint, queues string) error {
	if appName == "" {
		return fmt.Errorf("invalid application name, empty value found")
	}
	queueMap, err := _sqs.ParseQueueMap(queues)
	if err != nil {
		return err
	}
	if err := checkEndpoint(dynamodbEndpoint, "dynamodb"); err != nil {
		return err
	}
	if err := checkEndpoint(s3Endpoint, "s3"); err != nil {
		return err
	}
	if err := checkEndpoint(sqsEndpoint, "sqs"); err != nil {
		return err
	}

	table := eventTableName(appName)
	bucket := eventBucketName(appName)
	internalBucket := internalBucketName(appName)

	log.Println("[DEBUG] local resources ", table, bucket, internalBucket)
	cfg, err := awsConfig()
	if err != nil {
		return fmt.Errorf("failed to load aws config: %v", err)
	}
	dbClient := dynamodbClient(cfg, dynamodbEndpoint)
	streamClient := dynamodbStreamClient(cfg, dynamodbEndpoint)
	s3Client := s3Client(cfg, s3Endpoint)
	sqsClient := sqsClient(cfg, sqsEndpoint)

	if err := createBucketIfNotExist(ctx, s3Client, internalBucket); err != nil {
		return fmt.Errorf("create internal bucket %s failed: %v", internalBucket, err)
	}
	fmt.Fprintf(os.Stdout, "create event bucket '%s' if not exists...", bucket)
	if err := createBucketIfNotExist(ctx, s3Client, bucket); err != nil {
		return fmt.Errorf("create event bucket '%s' failed: %v", bucket, err)
	}

	if err := createEventTableIfNotExist(ctx, dbClient, table); err != nil {
		return fmt.Errorf("create event table '%s' failed: %v", table, err)
	}

	fwd := dynamo.NewForwarder(dbClient, table,
		_s3.NewStreamePersister(s3Client, bucket),
		_sqs.NewPublisher(sqsClient, queueMap),
	)

	return newStreamPoller(dbClient, streamClient, table, s3Client, internalBucket).
		OnChange(func(rec *types.Record) error {
			r, err := fromDynamodbStreamsToRecord(rec.Dynamodb.NewImage)
			if err != nil {
				return err
			}

			b, _ := json.Marshal(r)
			log.Println("[DEBUG] stream changes", string(b), err)
			if err != nil {
				return err
			}

			return fwd.Forward(context.Background(), []dynamo.Record{*r})
		}).Poll(ctx)
}
