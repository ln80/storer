package local

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	stmtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/ln80/storer/dynamo"
	"github.com/ln80/storer/storer-cli/internal"
)

func eventTableName(appName string) string {
	return internal.Sanitize(appName) + "-event-table"
}

func createEventTableIfNotExist(ctx context.Context, client *dynamodb.Client, tableName string) error {
	if err := dynamo.CreateTable(ctx, client, tableName); err != nil {

		var (
			er1 *types.TableAlreadyExistsException
			er2 *types.ResourceInUseException
		)
		if errors.As(err, &er1) || errors.As(err, &er2) {
			return nil
		}

		return err
	}

	return nil
}

func fromDynamodbStreamsToRecord(attrs map[string]stmtypes.AttributeValue) (*dynamo.Record, error) {
	img, err := attributevalue.FromDynamoDBStreamsMap(attrs)
	if err != nil {
		return nil, err
	}
	rec := dynamo.Record{}
	if err := attributevalue.UnmarshalMap(img, &rec); err != nil {
		return nil, err
	}

	return &rec, nil
}
