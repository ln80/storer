package s3

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ln80/storer/testutil"
)

var s3svc AdminAPI

func awsConfig(endpoint string) (cfg aws.Config, err error) {
	cfg, err = config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(""),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					HostnameImmutable: true,
				}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	return
}

func withBucket(t *testing.T, s3svc AdminAPI, tfn func(bucket string)) {
	ctx := context.Background()

	bucket := "test-bucket"
	if err := createBucket(ctx, s3svc, bucket); err != nil {
		t.Fatalf("failed to create test event bucket: %v", err)
	}

	tfn(bucket)
}

func TestMain(m *testing.M) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		log.Println("s3 test endpoint not found")
		return
	}

	cfg, err := awsConfig(endpoint)
	if err != nil {
		log.Fatal(err)
		return
	}

	s3svc = s3.NewFromConfig(cfg)

	testutil.RegisterEvent("")

	os.Exit(m.Run())
}
