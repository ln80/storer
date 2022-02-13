package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	return fmt.Sprintf("%s#%d", si.Parts()[0], page)
}

func recordRangeKeyWithTimestamp(t time.Time) string {
	// return fmt.Sprintf("t_%s", event.TimeToVersion(t))
	return fmt.Sprintf("t_%020d", t.UnixNano())
}

func recordRangeKeyWithVersion(ver event.Version) string {
	return fmt.Sprintf("v_%s", ver.String())
}

type store struct {
	svc        ClientAPI
	table      string
	serializer event.Serializer
}

var _ event.Store = &store{}
var _ sourcing.Store = &store{}

type StoreOption func(s *store)

func WithSerializer(ser event.Serializer) StoreOption {
	return func(s *store) {
		s.serializer = ser
	}
}

func NewEventStore(svc ClientAPI, table string, opts ...func(*store)) event.Store {
	s := &store{
		svc:        svc,
		table:      table,
		serializer: json.NewEventSerializer(""),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func NewEventSourcingStore(svc ClientAPI, table string, opts ...func(*store)) sourcing.Store {
	s := &store{
		svc:        svc,
		table:      table,
		serializer: json.NewEventSerializer(""),
	}
	for _, opt := range opts {
		opt(s)
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

	// record contains a chunk of events and sorted with the last evt's timestamp
	// chunk's time window should not exceed 5 sec
	if !since.Before(time.Unix(5, 0)) && !since.IsZero() {
		msince = since.Add(-5 * time.Second)
	}
	muntil = until.Add(5 * time.Second)

	envs, _, err := s.load(ctx, id,
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

func (s *store) AppendStream(ctx context.Context, stm sourcing.Stream) error {
	if stm.Empty() {
		return nil
	}
	if err := stm.Validate(); err != nil {
		return err
	}
	id := stm.ID()
	ver := stm.Version()
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithVersion(ver.Abs())
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

	envs, v, err := s.load(ctx, id, recordRangeKeyWithVersion(from.Abs()),
		recordRangeKeyWithVersion(to.Abs()))
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
		id, v.Abs(), fenvs,
	), nil
}

func (s *store) save(ctx context.Context, id event.StreamID, ver *event.Version, evts []event.Envelope, keysFn func() (string, string)) error {
	ses, ok := SessionFrom(ctx)
	if !ok {
		ses = NewSession(s.svc)
	}

	if len(evts) == 0 {
		return nil
	}

	b, err := s.serializer.MarshalEventBatch(evts)
	if err != nil {
		return fmt.Errorf("%w: details(%s)", event.ErrAppendEventsFailed, err.Error())
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
	}

	mr, err := attributevalue.MarshalMap(r)
	if err != nil {
		return fmt.Errorf("%w: details(%s)", event.ErrAppendEventsFailed, err.Error())
	}

	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name("_sk"),
			),
		).Build()
	if err != nil {
		return fmt.Errorf("%w: details(%s)", event.ErrAppendEventsFailed, err.Error())
	}

	if err = ses.Put(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      mr,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		if IsConditionCheckFailure(err) {
			return fmt.Errorf("%w: stream(%s): details(%s)", event.ErrAppendEventsConflict, id.String(), err.Error())
		}
		return fmt.Errorf("%w: details(%s)", event.ErrAppendEventsFailed, err.Error())
	}

	return nil
}

func (s *store) load(ctx context.Context, id event.StreamID, from, to string) ([]event.Envelope, event.Version, error) {
	ver := event.VersionZero

	expr, err := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(recordHashKey(id))).
				And(expression.
					Key(RangeKey).
					Between(expression.Value(from), expression.Value(to))),
		).
		Build()
	if err != nil {
		return nil, ver, err
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
	}
	out, err := s.svc.Query(ctx, input)
	if err != nil {
		return nil, ver, err
	}

	records := []Record{}
	err = attributevalue.UnmarshalListOfMaps(out.Items, &records)
	if err != nil {
		return nil, event.VersionZero, err
	}

	envs := []event.Envelope{}
	for i, r := range records {
		chunk, err := s.serializer.UnmarshalEventBatch(r.Events)
		if err != nil {
			return nil, ver, err
		}
		envs = append(envs, chunk...)

		if int32(i) == out.Count-1 && len(r.Version) != 0 {
			ver, err = event.ParseVersion(r.Version)
			if err != nil {
				return nil, ver, err
			}
		}
	}
	return envs, ver, nil
}
