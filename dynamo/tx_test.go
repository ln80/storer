package dynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestTx_Session(t *testing.T) {
	ctx := context.Background()

	// test get session from ctx
	ses0, ok := SessionFrom(ctx)
	if ok || ses0 != nil {
		t.Fatalf("expect session not found in ctx, got: %v", ses0)
	}

	// test create Session
	ses1 := NewSession(dbsvc)
	if ses1 == nil {
		t.Fatal("expect session be not nil")
	}

	// test add session to ctx
	sctx := ContextWithSession(ctx, ses1)
	ses2, ok := SessionFrom(sctx)
	if !ok || ses2 == nil {
		t.Fatal("expect session be found in ctx")
	}

	// test Session tx life cycle
	if err := ses2.StartTx(); err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if wanterr, err := ErrTxAlreadyStarted, ses2.StartTx(); !errors.Is(err, wanterr) {
		t.Fatalf("expect err be %v, got %v", wanterr, err)
	}
	if ok := ses2.HasTx(); !ok {
		t.Fatal("expect sesssion has active tx")
	}
	if err := ses2.CloseTx(); err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	// test we can close twice
	if err := ses2.CloseTx(); err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if ok := ses2.HasTx(); ok {
		t.Fatal("expect sesssion does not have active tx")
	}
}

func TestTx_Operations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping tx operation test")
	}
	ctx := context.Background()

	type TestItem struct {
		Item
		Val string `dynamodbav:"val"`
	}

	withTable(t, dbsvc, func(table string) {
		ses := NewSession(dbsvc)

		ses.StartTx()

		mitem0, _ := attributevalue.MarshalMap(TestItem{
			Item: Item{
				HashKey:  "abc",
				RangeKey: "0",
			},
		})
		if err := ses.Put(ctx, &dynamodb.PutItemInput{
			Item:      mitem0,
			TableName: aws.String(table),
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		mitem1, _ := attributevalue.MarshalMap(TestItem{
			Item: Item{
				HashKey:  "abc",
				RangeKey: "1",
			},
		})
		if err := ses.Put(ctx, &dynamodb.PutItemInput{
			Item:      mitem1,
			TableName: aws.String(table),
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		queryFn := func() (*dynamodb.QueryOutput, error) {
			expr, err := expression.
				NewBuilder().
				WithKeyCondition(
					expression.Key(HashKey).
						Equal(expression.Value("abc")),
				).
				Build()
			if err != nil {
				t.Fatalf("expect err be nil, got %v", err)
			}
			testQuery := &dynamodb.QueryInput{
				TableName:                 aws.String(table),
				KeyConditionExpression:    expr.KeyCondition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				ConsistentRead:            aws.Bool(true),
			}
			return dbsvc.Query(ctx, testQuery)
		}

		out, err := queryFn()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if out.Count != 0 {
			t.Fatalf("expect get items count be %d, got %d", 2, out.Count)
		}

		// commit to flush both items ops to table
		if err := ses.CommitTx(ctx); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		// we can commit twice without issues, idempotency..
		if err := ses.CommitTx(ctx); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if ok := ses.HasTx(); ok {
			t.Fatal("expect tx is closed")
		}

		// check that both items are persisted
		out, err = queryFn()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if out.Count != 2 {
			t.Fatalf("expect get items count be %d, got %d", 2, out.Count)
		}

		// test update and delete persisted items in same tx
		if err := ses.StartTx(); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		expr, err := expression.
			NewBuilder().
			WithUpdate(
				expression.
					Set(expression.Name("val"), expression.Value("updated")),
			).Build()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if err := ses.Update(ctx, &dynamodb.UpdateItemInput{
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: "abc"},
				RangeKey: &types.AttributeValueMemberS{Value: "0"},
			},
			UpdateExpression:          expr.Update(),
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			TableName:                 aws.String(table),
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if err := ses.Delete(ctx, &dynamodb.DeleteItemInput{
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: "abc"},
				RangeKey: &types.AttributeValueMemberS{Value: "1"},
			},
			TableName: aws.String(table),
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// check that items are not update
		out, err = queryFn()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if out.Count != 2 {
			t.Fatalf("expect get items count be %d, got %d", 2, out.Count)
		}

		if err := ses.CommitTx(ctx); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// check that items are update
		out, err = queryFn()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if out.Count != 1 {
			t.Fatalf("expect get items count be %d, got %d", 1, out.Count)
		}
		items := []TestItem{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &items)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantval, val := "updated", items[0].Val; wantval != val {
			t.Fatalf("expect values be equals, got %s, %s", wantval, val)
		}

		// test exec ops without starting tx
		if ok := ses.HasTx(); ok {
			t.Fatal("expect tx is closed")
		}
		if err := ses.Delete(ctx, &dynamodb.DeleteItemInput{
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: "abc"},
				RangeKey: &types.AttributeValueMemberS{Value: "0"},
			},
			TableName: aws.String(table),
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		out, err = queryFn()
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if out.Count != 0 {
			t.Fatalf("expect get items count be %d, got %d", 0, out.Count)
		}
	})

}
