package dynamo

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
		splits := strings.Split(r.HashKey, "#")
		mapIDs[splits[0]] = splits[0]
	}
	ids := make([]string, 0, len(mapIDs))
	for k := range mapIDs {
		ids = append(ids, k)
	}
	return ids
}

func recordHashKey(stmID event.StreamID, ps ...int) string {
	page := 1
	if len(ps) > 0 {
		page = ps[0]
	}
	return fmt.Sprintf("%s#%d", stmID.GlobalID(), page)
}

func recordRangeKeyWithTimestamp(stmID event.StreamID, t time.Time) string {
	return fmt.Sprintf("%st_%020d", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), t.UnixNano())
}

func recordRangeKeyWithVersion(stmID event.StreamID, ver event.Version) string {
	return fmt.Sprintf("%s@v_%s", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), ver.Trunc().String())
}

var (
	_ event.Store    = &store{}
	_ sourcing.Store = &store{}
)

type StoreConfig struct {
	serializer event.Serializer
}

type store struct {
	svc   ClientAPI
	table string

	*StoreConfig

	mu         sync.RWMutex
	checkpoint map[string]string
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
			serializer: json.NewEventSerializer(""),
		},

		checkpoint: make(map[string]string),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
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
		return recordHashKey(id), recordRangeKeyWithTimestamp(id, t)
	}
	return s.doAppend(ctx, id, nil, evts, keysFn)
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

	// records are sorted by the last evt's timestamp
	// chunk's time window should not exceed 10 sec
	// TODO: enfore this invariant in Append method
	if !since.Before(time.Unix(5, 0)) && !since.IsZero() {
		msince = since.Add(-5 * time.Second)
	}
	muntil = until.Add(5 * time.Second)
	envs, err := s.doLoad(ctx, id,
		recordRangeKeyWithTimestamp(id, msince),
		recordRangeKeyWithTimestamp(id, muntil),
	)
	if err != nil {
		return nil, err
	}
	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.Event() == nil {
			return nil, event.Err(event.ErrInvalidStream, id.String(),
				"empty event data loaded from store", "it's likely to be a lazily unmarshaling issue")
		}
		if env.At().Before(since) || env.At().After(until) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	return fenvs, nil
}

// AppendToStream implements sourcing.Store interface
func (s *store) AppendToStream(ctx context.Context, stm sourcing.Stream) (err error) {
	if stm.Empty() {
		return
	}
	if err = stm.Validate(); err != nil {
		return
	}
	// update the stream in-memory checkpoint if the new chunk is succesfully appended
	defer func() {
		if err == nil {
			s.checkVersion(stm.ID(), stm.Version())
		}
	}()

	id := stm.ID()
	ver := stm.Version()
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithVersion(stm.ID(), ver)
	}
	// "check previous record" + "save new chunk" op is not transactional, and we suppose the store table is immutable
	// a dirty read may occur if table is somehow corrupted/updated during the append call
	// we ignore "check previous record exists" if the chunk to append is supposed being the first in the stream
	if ver.Trunc().After(event.VersionMin) {
		if err = s.previousRecordExists(ctx, id, ver); err != nil {
			return err
		}
	}
	// doAppend still perform another check to ensure the chunk to append's version does not already exist
	// although doAppend does not guarantees the previous record aka chunk exist
	err = s.doAppend(ctx, id, &ver, stm.Unwrap(), keysFn)
	return
}

// AppendToStream implements sourcing.Store interface
func (s *store) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (stm *sourcing.Stream, err error) {
	var from, to event.Version
	if l := len(vrange); l > 1 {
		from, to = vrange[0], vrange[1]
	} else if l == 1 {
		from, to = vrange[0], event.VersionMax
	} else {
		from, to = event.VersionMin, event.VersionMax
	}

	// update the stream memory checkpoint if last records of the stream are successfully loaded
	if to == event.VersionMax {
		defer func() {
			if err == nil {
				s.checkVersion(stm.ID(), stm.Version())
			}
		}()
	}

	envs, err := s.doLoad(ctx, id, recordRangeKeyWithVersion(id, from),
		recordRangeKeyWithVersion(id, to))
	if err != nil {
		return nil, err
	}

	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.Event() == nil {
			return nil, event.Err(event.ErrInvalidStream, id.String(),
				"empty event data loaded from store", "it's likely to be a lazily unmarshaling issue")
		}
		if env.Version().Before(from) || env.Version().After(to) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	stm = sourcing.NewStream(
		id, fenvs,
	)
	return
}

// checkVersion set the given version as the current one the stream in a memory cache
func (s *store) checkVersion(id event.StreamID, ver event.Version) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checkpoint[id.String()] = ver.String()
}

// lastCheckedVersion returns the given stream current version from the memory cache if it exists.
func (s *store) lastCheckedVersion(id event.StreamID) (ver string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ver, ok = s.checkpoint[id.String()]
	return
}

// previousRecordExists ensures the given stream chunks are in sequence in case of a version-based stream.
// It also maintains/refresh the in-memory checkpoint of the given stream.
func (s *store) previousRecordExists(ctx context.Context, id event.StreamID, verToAppend event.Version) error {
	// previous version should be equals to current stream version (without fractional part)
	prever := verToAppend.Decr()
	cacheUpdated := false

CHECK_PREVIOUS_RECORD:
	// get current stream version from memory cache
	lastver, ok := s.lastCheckedVersion(id)
	if ok {
		if prever.String() == lastver {
			return nil
		}

		// coming to point where cache is refreshed but prev ver still ahead means the vertoAppend is not in sequence
		if prever.String() < lastver || cacheUpdated {
			return event.Err(event.ErrAppendEventsFailed, id.String(), "invalid chunk version, it must be next to previous record version: "+prever.String())
		}
	}

	// invalidate the cache
	if !ok || prever.String() > lastver {

		// replace this query by
		// query lmit =1 order desc
		expr, err := expression.
			NewBuilder().
			WithProjection(expression.NamesList(
				expression.Name("version"),
			)).Build()
		if err != nil {
			return event.Err(event.ErrAppendEventsFailed, id.String(), err)
		}
		out, err := s.svc.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.table),
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: recordHashKey(id)},
				RangeKey: &types.AttributeValueMemberS{Value: recordRangeKeyWithVersion(id, prever)},
			},
			ConsistentRead:           aws.Bool(true),
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
		})
		if err != nil || len(out.Item) == 0 {
			return event.Err(event.ErrAppendEventsFailed, id.String(), "previous record not found of version: %v", prever)
		}

		s.checkVersion(id, prever)
		cacheUpdated = true

		goto CHECK_PREVIOUS_RECORD
	}
	return nil
}

// doAppend works for both version and timestamp based streams.
// It appends the given chunk of events to stream.
// The ver parameter must be not nil in case of a version-based stream
func (s *store) doAppend(ctx context.Context, id event.StreamID, ver *event.Version, evts []event.Envelope, keysFn func() (string, string)) error {
	if len(evts) == 0 {
		return nil
	}

	ses, ok := SessionFrom(ctx)
	if !ok {
		ses = NewSession(s.svc)
	}

	b, err := s.serializer.MarshalEventBatch(evts)
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
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
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(RangeKey),
			),
		).Build()
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	if err = ses.Put(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      mr,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		if IsConditionCheckFailure(err) {
			return event.Err(event.ErrAppendEventsConflict, id.String(), err)
		}
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	return nil
}

func (s *store) doLoad(ctx context.Context, id event.StreamID, from, to string) ([]event.Envelope, error) {
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
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
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
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		precs := []Record{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &precs)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		records = append(records, precs...)
	}

	envs := []event.Envelope{}
	for _, r := range records {
		chunk, err := s.serializer.UnmarshalEventBatch(r.Events)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		envs = append(envs, chunk...)
	}
	return envs, nil
}
