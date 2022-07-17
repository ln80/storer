package local

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func awsConfig() (aws.Config, error) {
	key, secret := "minioadmin", "minioadmin"
	if os.Getenv("MINIO_ROOT_USER") != "" {
		key = os.Getenv("MINIO_ROOT_USER")
	}
	if os.Getenv("MINIO_ROOT_PASSWORD ") != "" {
		key = os.Getenv("MINIO_ROOT_USER")
	}

	return config.LoadDefaultConfig(
		context.Background(),
		// config.WithRegion(""),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)

}
func dynamodbClient(cfg aws.Config, endpoint string) *dynamodb.Client {
	cfg, _ = config.LoadDefaultConfig(
		context.Background(),
	)
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.EndpointResolver = dynamodb.EndpointResolverFromURL(endpoint)
	})
}

func dynamodbStreamClient(cfg aws.Config, endpoint string) *dynamodbstreams.Client {
	cfg, _ = config.LoadDefaultConfig(
		context.Background(),
	)
	return dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		o.EndpointResolver = dynamodbstreams.EndpointResolverFromURL(endpoint)
	})
}

// func S3Client(cfg aws.Config, endpoint string) *s3.Client {
// 	cfg, _ = config.LoadDefaultConfig(
// 		context.Background(),
// 		config.WithRegion(""),
// 		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
// 			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
// 				return aws.Endpoint{
// 					URL:               endpoint,
// 					HostnameImmutable: true,
// 				}, nil
// 			})),
// 		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
// 	)

// 	return s3.NewFromConfig(cfg)
// }

func s3Client(cfg aws.Config, endpoint string) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(endpoint, func(e *aws.Endpoint) {
			e.URL = endpoint
			e.HostnameImmutable = true
		})
	})
}

func sqsClient(cfg aws.Config, endpoint string) *sqs.Client {
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.EndpointResolver = sqs.EndpointResolverFromURL(endpoint)
	})
}
