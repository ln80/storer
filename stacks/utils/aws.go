package utils

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func InitAWSClients() (dbsvc *dynamodb.Client, s3svc *s3.Client, sqsvc *sqs.Client, err error) {
	var cfg aws.Config
	cfg, err = config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		return
	}
	dbsvc, s3svc, sqsvc = dynamodb.NewFromConfig(cfg), s3.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	return
}

func InitLambdaClients() (svc *lambda.Client, err error) {
	var cfg aws.Config
	cfg, err = config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		return
	}
	svc = lambda.NewFromConfig(cfg)
	return
}

// HackCtx to workaround https://github.com/aws/aws-sam-cli/issues/2510
// seems be fixed in sam cli 1.37.0
func HackCtx(ctx context.Context) context.Context {
	if os.Getenv("AWS_SAM_LOCAL") == "true" {
		return context.Background()
	}

	return ctx
}
