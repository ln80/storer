package local

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ln80/storer/storer-cli/internal"
)

const (
	InternalBucketSuffix = "storer-internal"
)

func eventBucketName(appName string) string {
	return internal.Sanitize(appName) + "-event-bucket"
}

func internalBucketName(appName string) string {
	return internal.Sanitize(appName) + "-" + InternalBucketSuffix
}

func createBucketIfNotExist(ctx context.Context, client *s3.Client, bucketName string) error {
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName), // Required

	}); err != nil {
		var e *types.BucketAlreadyOwnedByYou
		if errors.As(err, &e) {
			return nil
		}
		return err
	}

	return nil
}
