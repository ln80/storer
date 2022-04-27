package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func createBucket(ctx context.Context, s3svc AdminAPI, name string) error {
	if _, err := s3svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraintEuWest1,
		},
	}); err != nil {
		// return err
		return nil
	}

	return nil
}

// func deleteBucket(ctx context.Context, s3svc AdminAPI, name string) error {
// 	// if _, err := s3svc.DeleteObjects(ctx, &s3.DeleteObjectsInput{
// 	// 	Bucket: aws.String(name),
// 	// 	Delete: &types.Delete{

// 	// 	},

// 	// }); err != nil {
// 	// 	return err
// 	// }
// 	if _, err := s3svc.(*s3.Client).DeleteBucket(ctx, &s3.DeleteBucketInput{
// 		Bucket: aws.String(name),
// 	}); err != nil {
// 		return err
// 	}
// 	return nil
// }
