package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/redaLaanait/storer/event"
	"github.com/redaLaanait/storer/event/sourcing"
	"github.com/redaLaanait/storer/json"
)

type Record struct {
	Item
	Events  []byte `dynamodbav:"events"`
	Since   int64  `dynamodbav:"since"`
	Until   int64  `dynamodbav:"until"`
	Version string `dynamodbav:"version,omitempty"`
}

func streamIDsFrom(recs []Record) []string {
	mapIDs := make(map[string]string)
	for _, r := range recs {
		mapIDs[r.HashKey] = r.HashKey
	}
	ids := make([]string, 0, len(mapIDs))
	for k := range mapIDs {
		ids = append(ids, k)
	}
	return ids
}

func recordHashKey(si event.StreamID, ps ...int) string {
	page := 1
	if len(ps) > 0 {
		page = ps[0]
	}
	return fmt.Sprintf("%s#%d", si.GlobalID(), page)
}

func recordRangeKeyWithTimestamp(t time.Time) string {
	return fmt.Sprintf("t_%020d", t.UnixNano())
}

func recordRangeKeyWithVersion(ver event.Version) string {
	return fmt.Sprintf("v_%s", ver.Abs().String())
}

var (
	_ event.Store    = &store{}
	_ sourcing.Store = &store{}
)

type StoreConfig struct {
	CheckPreviousRecord bool
	serializer          event.Serializer
}

type store struct {
	svc   ClientAPI
	table string

	*StoreConfig
}

type StoreOption func(s *StoreConfig)

func WithSerializer(ser event.Serializer) StoreOption {
	return func(s *StoreConfig) {
		s.serializer = ser
	}
}

func NewEventStore(svc ClientAPI, table string, opts ...func(*StoreConfig)) interface {
	event.Store
	sourcing.Store
} {
	s := &store{
		svc:   svc,
		table: table,
		StoreConfig: &StoreConfig{
			serializer:          json.NewEventSerializer(""),
			CheckPreviousRecord: true,
		},
	}

	for _, opt := range opts {
		opt(s.StoreConfig)
	}
	return s
}

func (s *store) Append(ctx context.Context, id event.StreamID, evts ...event.Envelope) error {
	if len(evts) == 0 {
		return nil
	}
	t := evts[len(evts)-1].At()
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithTimestamp(t)
	}
	return s.save(ctx, id, nil, evts, keysFn)
}

func (s *store) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	var since, until, msince, muntil time.Time
	l := len(trange)
	if l > 1 {
		since, until = trange[0], trange[1]
	} else if l == 1 {
		since, until = trange[0], time.Now()
	} else {
		since, until = time.Unix(0, 0), time.Now()
	}

	// record is sorted with the last chunk evt timestamp
	// chunk's time window should not exceed 10 sec
	if !since.Before(time.Unix(5, 0)) && !since.IsZero() {
		msince = since.Add(-5 * time.Second)
	}
	muntil = until.Add(5 * time.Second)
	envs, err := s.load(ctx, id,
		recordRangeKeyWithTimestamp(msince),
		recordRangeKeyWithTimestamp(muntil),
	)
	if err != nil {
		return nil, err
	}
	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.At().Before(since) || env.At().After(until) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	return fenvs, nil
}

func (s *store) AppendToStream(ctx context.Context, stm sourcing.Stream) error {
	if stm.Empty() {
		return nil
	}
	if err := stm.Validate(); err != nil {
		return err
	}
	id := stm.ID()
	ver := stm.Version()
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithVersion(ver)
	}
	return s.save(ctx, id, &ver, stm.Unwrap(), keysFn)
}

func (s *store) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	var from, to event.Version
	l := len(vrange)
	if l > 1 {
		from, to = vrange[0], vrange[1]
	} else if l == 1 {
		from, to = vrange[0], event.VersionMax
	} else {
		from, to = event.VersionMin, event.VersionMax
	}

	envs, err := s.load(ctx, id, recordRangeKeyWithVersion(from),
		recordRangeKeyWithVersion(to))
	if err != nil {
		return nil, err
	}

	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.Version().Before(from) || env.Version().After(to) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	return sourcing.NewStream(
		id, fenvs,
	), nil
}

// checkPreviousRecordExists verifies the existence of the previous versionned record
// note that the check is not transactional, and suppose the store table is immutable
// a dirty read may occur if table is somehow corrupted
func (s *store) checkPreviousRecordExists(ctx context.Context, id event.StreamID, ver event.Version) error {
	prever := ver.Decr()
	out, err := s.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			HashKey:  &types.AttributeValueMemberS{Value: recordHashKey(id)},
			RangeKey: &types.AttributeValueMemberS{Value: recordRangeKeyWithVersion(prever)},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil || len(out.Item) == 0 {
		return event.Err(event.ErrAppendEventsFailed, id.String(), "previous record not found ver: %v", prever)
	}
	return nil
}

func (s *store) save(ctx context.Context, id event.StreamID, ver *event.Version, evts []event.Envelope, keysFn func() (string, string)) error {
	if len(evts) == 0 {
		return nil
	}

	ses, ok := SessionFrom(ctx)
	if !ok {
		ses = NewSession(s.svc)
	}

	b, err := s.serializer.MarshalEventBatch(evts)
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err.Error())
	}

	hk, rk := keysFn()
	r := Record{
		Item: Item{
			HashKey:  hk,
			RangeKey: rk,
		},
		Events: b,
		Since:  evts[0].At().UnixNano(),
		Until:  evts[len(evts)-1].At().UnixNano(),
	}

	if ver != nil {
		r.Version = ver.String()
		if ver.After(event.VersionMin) && s.CheckPreviousRecord {
			if err := s.checkPreviousRecordExists(ctx, id, *ver); err != nil {
				return err
			}
		}
	}

	mr, err := attributevalue.MarshalMap(r)
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err.Error())
	}

	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(RangeKey),
			),
		).Build()
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err.Error())
	}

	if err = ses.Put(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      mr,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		if IsConditionCheckFailure(err) {
			return event.Err(event.ErrAppendEventsConflict, id.String(), err.Error())
		}
		return event.Err(event.ErrAppendEventsFailed, id.String(), err.Error())
	}

	return nil
}

func (s *store) load(ctx context.Context, id event.StreamID, from, to string) ([]event.Envelope, error) {
	expr, err := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(recordHashKey(id))).
				And(expression.
					Key(RangeKey).
					Between(expression.Value(from), expression.Value(to))),
		).Build()
	if err != nil {
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err.Error())
	}

	p := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
	})
	records := []Record{}
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err.Error())
		}
		precs := []Record{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &precs)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err.Error())
		}
		records = append(records, precs...)
	}

	envs := []event.Envelope{}
	for _, r := range records {
		chunk, err := s.serializer.UnmarshalEventBatch(r.Events)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err.Error())
		}
		envs = append(envs, chunk...)
	}
	return envs, nil
}
