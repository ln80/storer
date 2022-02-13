package dynamo

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/redaLaanait/storer/event"
)

var (
	ErrValidateGSTMFailed    = errors.New("global stream validation error")
	ErrUpdateGSTMFailed      = errors.New("failed to update global stream")
	ErrCreateGSTMFailed      = errors.New("failed to create global stream")
	ErrUnexpectedGSTMFailure = errors.New("unexpected failure")
	ErrGSTMNotFound          = errors.New("global stream not found")
)

type GSTM struct {
	Item
	StreamID    string `dynamodbav:"stmID"`
	Version     string `dynamodbav:"gver"`
	LastEventID string `dynamodbav:"levtID"`
	UpdatedAt   int64  `dynamodbav:"uat"`
}

func (gstm GSTM) Validate() error {
	if gstm.StreamID == "" {
		return event.Err(ErrValidateGSTMFailed, gstm.StreamID, "empty streamID")
	}
	if ver, err := event.ParseVersion(gstm.Version); err != nil || ver.Before(event.VersionMin) {
		return event.Err(ErrValidateGSTMFailed, gstm.StreamID, "invalid version sequence: "+gstm.Version)
	}
	if gstm.LastEventID == "" {
		return event.Err(ErrValidateGSTMFailed, gstm.StreamID, "empty lastEventID")
	}
	if gstm.UpdatedAt < 0 {
		return event.Err(ErrValidateGSTMFailed, gstm.StreamID, "empty updatedAt")
	}
	return nil
}

func gstmHashKey() string {
	return "internal"
}

func gstmRangeKey(stmID string) string {
	return strings.Join([]string{"gstm", stmID}, "#")
}

func persistGSTMBatch(ctx context.Context, dbsvc ClientAPI, table string, gstms map[string]*GSTM) error {
	if len(gstms) == 0 {
		return nil
	}
	for stmID, gstm := range gstms {
		if gstm == nil {
			return event.Err(ErrUnexpectedGSTMFailure, stmID, "empty gstm")
		}
		if err := persistGSTM(ctx, dbsvc, table, *gstm); err != nil {
			return err
		}
	}
	return nil
}

func persistGSTM(ctx context.Context, dbsvc ClientAPI, table string, gstm GSTM) error {
	if err := gstm.Validate(); err != nil {
		return err
	}
	mgstm, err := attributevalue.MarshalMap(gstm)
	if err != nil {
		return event.Err(ErrUnexpectedGSTMFailure, gstm.StreamID, err)
	}
	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(HashKey),
			).And(
				expression.AttributeNotExists(
					expression.Name(RangeKey),
				),
			),
		).Build()
	if err != nil {
		return event.Err(ErrUnexpectedGSTMFailure, gstm.StreamID, err)
	}

	// fmt.Printf("ConditionExpression: %s\n", spew.Sdump(expr.Condition()))
	// fmt.Printf("Names: %s\n", spew.Sdump(expr.Names()))
	// fmt.Printf("Values: %s\n", spew.Sdump(expr.Values()))
	// fmt.Printf("Update Expression: %q\n", aws.ToString(expr.Update()))
	if _, err = dbsvc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(table),
		Item:                      mgstm,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		if IsConditionCheckFailure(err) {
			expr, err := expression.
				NewBuilder().
				WithUpdate(
					expression.
						Set(expression.Name("gver"), expression.Value(gstm.Version)).
						Set(expression.Name("levtID"), expression.Value(gstm.LastEventID)).
						Set(expression.Name("uat"), expression.Value(gstm.UpdatedAt)),
				).WithCondition(
				expression.
					Not(expression.
						Equal(expression.Name("levtID"), expression.Value(gstm.LastEventID))).
					And(
						expression.LessThan(expression.Name("gver"), expression.Value(gstm.Version)),
					),
			).Build()
			if err != nil {
				return event.Err(ErrUnexpectedGSTMFailure, gstm.StreamID, err)
			}

			// fmt.Printf("ConditionExpression: %s\n", spew.Sdump(expr.Condition()))
			// fmt.Printf("Names: %s\n", spew.Sdump(expr.Names()))
			// fmt.Printf("Values: %s\n", spew.Sdump(expr.Values()))
			// fmt.Printf("Update Expression: %q\n", aws.ToString(expr.Update()))
			if _, err = dbsvc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				Key: map[string]types.AttributeValue{
					HashKey:  &types.AttributeValueMemberS{Value: gstm.HashKey},
					RangeKey: &types.AttributeValueMemberS{Value: gstm.RangeKey},
				},
				TableName:                 aws.String(table),
				ConditionExpression:       expr.Condition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				UpdateExpression:          expr.Update(),
			}); err != nil && !IsConditionCheckFailure(err) {
				return event.Err(ErrUpdateGSTMFailed, gstm.StreamID, err)
			}
			return nil
		}
		return event.Err(ErrCreateGSTMFailed, gstm.StreamID, err)
	}
	return nil
}

func getGSTM(ctx context.Context, dbsvc ClientAPI, table string, gstmID string) (*GSTM, error) {
	expr, err := expression.NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(gstmHashKey())).
				And(expression.
					Key(RangeKey).
					BeginsWith(gstmRangeKey(gstmID)),
				),
		).Build()
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, err)
	}

	out, err := dbsvc.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
	})
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, gstmID, err)
	}

	items := []GSTM{}
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &items); err != nil {
		return nil, err
	}
	if l := len(items); l != 1 {
		return nil, event.Err(ErrGSTMNotFound, gstmID, "search count: "+strconv.Itoa(l))
	}
	return &items[0], nil
}

func getGSTMBatch(ctx context.Context, dbsvc ClientAPI, table string, stmIDs []string) (map[string]*GSTM, error) {
	if len(stmIDs) == 0 {
		return nil, nil
	}

	b := expression.NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(gstmHashKey())).
				And(expression.
					Key(RangeKey).
					BeginsWith(gstmRangeKey("")),
				),
		)
	if l := len(stmIDs); l > 0 {
		ops := []expression.OperandBuilder{}
		for i := 0; i < l; i++ {
			ops = append(ops, expression.Value(stmIDs[i]))
			ops = append(ops, expression.Value(stmIDs[i]))
		}

		b.WithFilter(
			expression.Name("stmID").In(ops[0], ops[1:l]...))
	}

	strStmID := strings.Join(stmIDs, ",")

	expr, err := b.Build()
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strStmID, err)
	}

	// fmt.Printf("ConditionExpression: %s\n", spew.Sdump(expr.Condition()))
	// fmt.Printf("Names: %s\n", spew.Sdump(expr.Names()))
	// fmt.Printf("Values: %s\n", spew.Sdump(expr.Values()))
	// fmt.Printf("Update Expression: %q\n", aws.ToString(expr.Update()))

	out, err := dbsvc.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ConsistentRead:            aws.Bool(true),
	})
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strStmID, err)
	}

	items := []GSTM{}
	err = attributevalue.UnmarshalListOfMaps(out.Items, &items)
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strStmID, err)
	}

	gstms := map[string]*GSTM{}
	for _, i := range items {
		gstm := i
		gstms[i.StreamID] = &gstm
	}
	for _, id := range stmIDs {
		if _, ok := gstms[id]; !ok {
			gstms[id] = &GSTM{
				Item: Item{
					HashKey:  gstmHashKey(),
					RangeKey: gstmRangeKey(id),
				},
				StreamID: id,
			}
		}
	}
	return gstms, nil
}
