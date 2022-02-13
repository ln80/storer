package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrTxAlreadyStarted = errors.New("transaction already started")
)

type ContextKey string

var sessionContextKey ContextKey = "dynamodbSessionKey"

type Session interface {
	CommitTx(ctx context.Context) error
	StartTx() error
	CloseTx() error
	HasTx() bool

	Put(ctx context.Context, p *dynamodb.PutItemInput) error
	Update(ctx context.Context, u *dynamodb.UpdateItemInput) error
	Check(ctx context.Context, c *types.ConditionCheck) error
	Delete(ctx context.Context, d *dynamodb.DeleteItemInput) error
}

func NewSession(db ClientAPI) Session {
	return &session{
		svc: db,
	}
}

func SessionFrom(ctx context.Context) (Session, bool) {
	ses, ok := ctx.Value(sessionContextKey).(Session)
	if ok {
		return ses, true
	}
	return nil, false
}

func ContextWithSession(ctx context.Context, s Session) context.Context {
	return context.WithValue(ctx, sessionContextKey, s)
}

type session struct {
	svc ClientAPI
	ops []txOp
}

var _ Session = &session{}

func (s *session) StartTx() error {
	if s.ops == nil {
		s.ops = []txOp{}
		return nil
	}
	return ErrTxAlreadyStarted
}

func (s *session) HasTx() bool {
	return s.ops != nil
}

func (s *session) CloseTx() error {
	s.ops = nil
	return nil
}

func (s *session) CommitTx(ctx context.Context) (err error) {
	defer func() {
		if err == nil {
			err = s.CloseTx()
		}
	}()

	count := len(s.ops)
	if count == 0 {
		return
	}

	if count == 1 {
		op := s.ops[0]
		if op.put != nil {
			_, err = s.svc.PutItem(ctx, op.put)
			return
		}
		if op.update != nil {
			_, err = s.svc.UpdateItem(ctx, op.update)
			return
		}
		if op.delete != nil {
			_, err = s.svc.DeleteItem(ctx, op.delete)
			return
		}
		return
	}

	txItems := make([]types.TransactWriteItem, count)
	for i, op := range s.ops {
		txItems[i] = types.TransactWriteItem{
			Put:            op.putTx(),
			Update:         op.updateTx(),
			Delete:         op.deleteTx(),
			ConditionCheck: op.checkTx(),
		}
	}
	if _, err = s.svc.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: txItems,
	}); err != nil {
		return err
	}
	return
}

func (s *session) Put(ctx context.Context, p *dynamodb.PutItemInput) error {
	if s.HasTx() {
		s.ops = append(s.ops, txOp{
			put: p,
		})
		return nil
	}
	if _, err := s.svc.PutItem(ctx, p); err != nil {
		return err
	}
	return nil
}

func (s *session) Update(ctx context.Context, u *dynamodb.UpdateItemInput) error {
	if s.HasTx() {
		s.ops = append(s.ops, txOp{
			update: u,
		})
		return nil
	}
	if _, err := s.svc.UpdateItem(ctx, u); err != nil {
		return err
	}
	return nil
}

func (s *session) Delete(ctx context.Context, d *dynamodb.DeleteItemInput) error {
	if s.HasTx() {
		s.ops = append(s.ops, txOp{
			delete: d,
		})
		return nil
	}
	if _, err := s.svc.DeleteItem(ctx, d); err != nil {
		return err
	}
	return nil
}

func (s *session) Check(ctx context.Context, c *types.ConditionCheck) error {
	if s.HasTx() {
		s.ops = append(s.ops, txOp{
			check: c,
		})
		return nil
	}
	return nil
}

type txOp struct {
	put    *dynamodb.PutItemInput
	update *dynamodb.UpdateItemInput
	delete *dynamodb.DeleteItemInput
	check  *types.ConditionCheck
}

func (op *txOp) putTx() *types.Put {
	if op.put == nil {
		return nil
	}
	return &types.Put{
		Item:                      op.put.Item,
		TableName:                 op.put.TableName,
		ConditionExpression:       op.put.ConditionExpression,
		ExpressionAttributeNames:  op.put.ExpressionAttributeNames,
		ExpressionAttributeValues: op.put.ExpressionAttributeValues,
	}
}

func (op *txOp) updateTx() *types.Update {
	if op.update == nil {
		return nil
	}
	return &types.Update{
		Key:                       op.update.Key,
		UpdateExpression:          op.update.UpdateExpression,
		TableName:                 op.update.TableName,
		ConditionExpression:       op.update.ConditionExpression,
		ExpressionAttributeNames:  op.update.ExpressionAttributeNames,
		ExpressionAttributeValues: op.update.ExpressionAttributeValues,
	}
}

func (op *txOp) deleteTx() *types.Delete {
	if op.delete == nil {
		return nil
	}
	return &types.Delete{
		Key:                       op.delete.Key,
		TableName:                 op.delete.TableName,
		ConditionExpression:       op.delete.ConditionExpression,
		ExpressionAttributeNames:  op.delete.ExpressionAttributeNames,
		ExpressionAttributeValues: op.delete.ExpressionAttributeValues,
	}
}

func (op *txOp) checkTx() *types.ConditionCheck {
	return op.check
}
