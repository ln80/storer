package local

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/ln80/storer/dynamo"
	_s3 "github.com/ln80/storer/s3"
	_sqs "github.com/ln80/storer/sqs"
	"github.com/ln80/storer/storer-cli/internal/logger"
)

func checkEndpoint(value string, name string) error {
	if _, err := url.ParseRequestURI(value); err != nil {
		return logger.Error(fmt.Errorf("invalid '%s' endpoint: %s", name, value))
	}
	return nil
}

func createResources(table, bucket, internalBucket string) error {

	return nil
}

func RunEventStore(ctx context.Context, appName, dynamodbEndpoint, s3Endpoint, sqsEndpoint, queues string) error {
	if appName == "" {
		return logger.Error(fmt.Errorf("application name not found"))
	}

	logger.Info(fmt.Sprintf(`

	=======================================================
	 STORER Local Event Store for "%s"
	=======================================================

	`, appName))
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

	cfg, err := awsConfig()
	if err != nil {
		return logger.Error(fmt.Errorf("failed to load aws local config: %v", err))
	}

	dbClient := dynamodbClient(cfg, dynamodbEndpoint)
	streamClient := dynamodbStreamClient(cfg, dynamodbEndpoint)
	s3Client := s3Client(cfg, s3Endpoint)
	sqsClient := sqsClient(cfg, sqsEndpoint)

	table := eventTableName(appName)
	bucket := eventBucketName(appName)
	internalBucket := internalBucketName(appName)

	logger.Debug(fmt.Sprintf(`Start provisioning local AWS resources:

	- Dynamodb Event Table: %s
	- S3 Event Bucket: %s
	- S3 Internal Bucket: %s
	...
	...
	...`, table, bucket, internalBucket))

	if err := createBucketIfNotExist(ctx, s3Client, internalBucket); err != nil {
		return logger.Error(fmt.Errorf("create internal bucket %s failed: %v", internalBucket, err))
	}
	if err := createBucketIfNotExist(ctx, s3Client, bucket); err != nil {
		return logger.Error(fmt.Errorf("create event bucket '%s' failed: %v", bucket, err))
	}
	if err := createEventTableIfNotExist(ctx, dbClient, table); err != nil {
		return logger.Error(fmt.Errorf("create event table '%s' failed: %v", table, err))
	}

	logger.Debug(`
	...
	...
	Finish provisioning local AWS resources
	`)

	fwd := dynamo.NewForwarder(dbClient, table,
		&persistLogger{_s3.NewStreamePersister(s3Client, bucket)},
		&publishLogger{_sqs.NewPublisher(sqsClient, queueMap)},
	)

	return newStreamPoller(dbClient, streamClient, table, s3Client, internalBucket).
		OnChange(func(rec *types.Record) error {
			r, err := fromDynamodbStreamsToRecord(rec.Dynamodb.NewImage)
			if err != nil {
				return err
			}

			return fwd.Forward(ctx, []dynamo.Record{*r})
		}).
		Poll(ctx)
}
