package utils

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func ParseStringToMap(str string) (m map[string]string, err error) {
	str = strings.Trim(str, " ")
	if str == "" {
		return
	}
	for _, val := range strings.Split(str, ";") {
		splits := strings.Split(val, "=")
		if len(splits) != 2 {
			err = fmt.Errorf("invalid map value %s", val)
			return
		}
		m[splits[0]] = splits[1]
	}
	return
}

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

// HackCtx to workaround https://github.com/aws/aws-sam-cli/issues/2510
// seems be fixed in sam cli 1.37.0
func HackCtx(ctx context.Context) context.Context {
	if os.Getenv("AWS_SAM_LOCAL") == "true" {
		return context.Background()
	}

	return ctx
}
