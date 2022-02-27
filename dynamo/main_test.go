package dynamo

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/internal/testutil"
)

var dbsvc AdminAPI

var rdm = rand.New(rand.NewSource(time.Now().UnixNano()))

func genTableName(prefix string) string {
	now := strconv.FormatInt(time.Now().UnixNano(), 36)
	random := strconv.FormatInt(int64(rdm.Int31()), 36)
	return prefix + "-" + now + "-" + random
}

func awsConfig(endpoint string) (cfg aws.Config, err error) {
	cfg, err = config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(""),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			})),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("TEST", "TEST", "TEST")),
	)
	return
}

func withTable(t *testing.T, dbsvc AdminAPI, tfn func(table string)) {
	ctx := context.Background()

	table := genTableName("tmp-event-table")
	if err := CreateTable(ctx, dbsvc, table); err != nil {
		t.Fatalf("failed to create test event table: %v", err)
	}

	defer func() {
		if err := DeleteTable(ctx, dbsvc, table); err != nil {
			t.Fatalf("failed to clean aka remove test event table: %v", err)
		}
	}()

	tfn(table)
}

func TestMain(m *testing.M) {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		log.Println("dynamodb test endpoint not found")
		return
	}

	cfg, err := awsConfig(endpoint)
	if err != nil {
		log.Fatal(err)
		return
	}
	dbsvc = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.EndpointResolver = dynamodb.EndpointResolverFromURL(endpoint)
	})

	event.NewRegister("").
		Set(testutil.Event1{}).
		Set(testutil.Event2{})

	os.Exit(m.Run())
}
