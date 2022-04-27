package dynamo

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

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
	ErrUnexpectedGSTMFailure = errors.New("global stream unexpected failure")
	ErrGSTMNotFound          = errors.New("global stream not found")
)

const (
	activeDayMaxRecords = 5
)

type ActiveDay struct {
	Day, Version string
}

// GSTM defines the global stream infos mainly checkpoint related ones.
type GSTM struct {
	Item
	StreamID    string `dynamodbav:"stmID"`
	Version     string `dynamodbav:"gver"`
	LastEventID string `dynamodbav:"evtID"`
	UpdatedAt   int64  `dynamodbav:"uat"`

	LastActiveDays map[string]ActiveDay `dynamodbav:"activeDays"`
}

// Validate the global stream infos
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
	if gstm.UpdatedAt <= 0 {
		return event.Err(ErrValidateGSTMFailed, gstm.StreamID, "empty updatedAt")
	}
	return nil
}

// Update the global stream checkpoint related infos.
// It also keeps records of previous active days version.
func (gstm *GSTM) Update(evt event.Envelope, gver event.Version) error {
	// update checkpoint infos
	gstm.LastEventID = evt.ID()
	gstm.Version = gver.String()
	gstm.UpdatedAt = evt.At().UnixNano()

	// init last active days record map if needed
	if gstm.LastActiveDays == nil {
		gstm.LastActiveDays = map[string]ActiveDay{}
	}

	day := time.Unix(0, gstm.UpdatedAt).Format("2006/01/02")
	gstm.LastActiveDays[day] = ActiveDay{
		Day:     day,
		Version: gver.String(),
	}

	// remove old day from record if max record exceeded
	if l := len(gstm.LastActiveDays); l > activeDayMaxRecords {
		days := make([]string, 0, l)
		for day := range gstm.LastActiveDays {
			days = append(days, day)
		}
		sort.Strings(days)
		delete(gstm.LastActiveDays, days[0])
	}

	// TODO enforce gstm's invariants rather than post-validate
	return gstm.Validate()
}

// LastActiveDay returns the stream version of the last active day before the given date.
// It returns nil if not found.
func (gstm *GSTM) LastActiveDay(before time.Time) *ActiveDay {
	if gstm.LastActiveDays == nil {
		return nil
	}

	days := make([]string, 0)
	for day := range gstm.LastActiveDays {
		if day < before.Format("2006/01/02") {
			days = append(days, day)
		}
	}

	dl := len(days)
	if dl == 0 {
		return nil
	}

	sort.Strings(days)
	laday := gstm.LastActiveDays[days[dl-1]]

	return &laday
}

func gstmHashKey() string {
	return "internal"
}

func gstmRangeKey(stmID string) string {
	return strings.Join([]string{"gstm", stmID}, "#")
}

// persistGSTM to the dynamodb store in batch
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

// persistGSTM to the dynamodb store.
// Note that it's idempotent
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
						Set(expression.Name("evtID"), expression.Value(gstm.LastEventID)).
						Set(expression.Name("uat"), expression.Value(gstm.UpdatedAt)).
						Set(expression.Name("activeDays"), expression.Value(gstm.LastActiveDays)),
				).WithCondition(
				expression.
					Not(expression.
						Equal(expression.Name("evtID"), expression.Value(gstm.LastEventID))).
					And(
						expression.LessThan(expression.Name("gver"), expression.Value(gstm.Version)),
					),
			).Build()
			if err != nil {
				return event.Err(ErrUnexpectedGSTMFailure, gstm.StreamID, err)
			}

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

// getGSTM returns the global stream infos.
// It returns error if gstm does not exists
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

// GSTMFilter define the filter used to query global streams
type GSTMFilter struct {
	StreamIDs    []string
	UpdatedAfter time.Time
}

// build the query gstms filter, and set default filter values.
func (f GSTMFilter) build() (filter expression.ConditionBuilder) {
	var idsF, updatedF expression.ConditionBuilder
	if l := len(f.StreamIDs); l > 0 {
		ops := []expression.OperandBuilder{}
		for i := 0; i < l; i++ {
			ops = append(ops, expression.Value(f.StreamIDs[i]))
			ops = append(ops, expression.Value(f.StreamIDs[i]))
		}
		idsF = expression.Name("stmID").In(ops[0], ops[1:l]...)
	}
	if !f.UpdatedAfter.IsZero() {
		updatedF = expression.Name("uat").GreaterThanEqual(expression.Value(f.UpdatedAfter.UnixNano()))
	}
	if updatedF.IsSet() && idsF.IsSet() {
		filter = expression.And(updatedF, idsF)
	} else if updatedF.IsSet() {
		filter = updatedF
	} else if idsF.IsSet() {
		filter = idsF
	}
	return
}

// getGSTMBatch return a list of global streams infos as a map index by the gstm ID.
func getGSTMBatch(ctx context.Context, dbsvc ClientAPI, table string, f GSTMFilter) (map[string]*GSTM, error) {
	b := expression.NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(gstmHashKey())).
				And(expression.
					Key(RangeKey).
					BeginsWith(gstmRangeKey("")),
				),
		)
	fexpr := f.build()
	if fexpr.IsSet() {
		b.WithFilter(fexpr)
	}

	strIDs := strings.Join(f.StreamIDs, ",")
	expr, err := b.Build()
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strIDs, err)
	}

	out, err := dbsvc.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ConsistentRead:            aws.Bool(true),
	})
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strIDs, err)
	}

	items := []GSTM{}
	err = attributevalue.UnmarshalListOfMaps(out.Items, &items)
	if err != nil {
		return nil, event.Err(ErrUnexpectedGSTMFailure, strIDs, err)
	}

	gstms := map[string]*GSTM{}
	for _, i := range items {
		gstm := i
		gstms[i.StreamID] = &gstm
	}
	for _, id := range f.StreamIDs {
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
