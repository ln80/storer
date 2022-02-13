package dynamo

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/redaLaanait/storer/event"
)

var dbsvc AdminAPI

var rdm = rand.New(rand.NewSource(time.Now().UnixNano()))

func genTableName(prefix string) string {
	now := strconv.FormatInt(time.Now().UnixNano(), 36)
	random := strconv.FormatInt(int64(rdm.Int31()), 36)
	return prefix + "-" + now + "-" + random
}

type Event1 struct{ Val string }
type Event2 struct{ Val string }

func (e *Event2) Dests() []string {
	return []string{"dest2"}
}

func genEvents(count int) []interface{} {
	evts := make([]interface{}, count)
	for i := 0; i < count; i++ {
		var evt interface{}
		if i%2 == 0 {
			evt = &Event2{"val " + strconv.Itoa(i)}
		} else {
			evt = &Event1{"val " + strconv.Itoa(i)}
		}

		evts[i] = evt
	}
	return evts
}

func formatEnv(env event.Envelope) string {
	return fmt.Sprintf(`
		stmID: %s
		evtID: %s
		at: %v
		version: %v
		globalVersion: %v
		user: %s
		data: %v
	`, env.StreamID(), env.ID(), env.At().UnixNano(), env.Version(), env.GlobalVersion(), env.User(), env.Event())
}

func cmpEnv(env1, env2 event.Envelope) bool {
	return env1.ID() == env2.ID() &&
		env1.StreamID() == env2.StreamID() &&
		env1.GlobalStreamID() == env2.GlobalStreamID() &&
		env1.User() == env2.User() &&
		env1.At().Equal(env2.At()) &&
		env1.Version().Equal(env2.Version()) &&
		reflect.DeepEqual(env1.Event(), env2.Event())
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
		log.Fatal("dynamodb test endpoint not found")
		return
	}

	cfg, err := awsConfig(endpoint)
	if err != nil {
		log.Fatal(err)
		return
	}

	dbsvc = dynamodb.NewFromConfig(cfg)

	event.NewRegister("").
		Set(Event1{}).
		Set(Event2{})

	os.Exit(m.Run())
}
