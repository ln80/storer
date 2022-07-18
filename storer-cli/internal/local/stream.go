package local

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ln80/storer/storer-cli/internal"
)

const (

	// PollerConfigLocation gives dynamodbstreams config file location
	PollerConfigLocation = "/dynamodbstreams/config.json"
)

type PollerConfig struct {
	Streams    map[string]*StreamConfig
	LastStream string
	At         time.Time
	mu         sync.Mutex `json:"-"`
}

type StreamConfig struct {
	Arn        string
	LastSeqNum string
	Chards     map[string]types.Shard `json:"-"`
}

func (cfg *PollerConfig) setCursor(cursor string) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.lastStreamConfig().LastSeqNum = cursor
}

func (cfg *PollerConfig) cursor() string {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	return cfg.lastStreamConfig().LastSeqNum
}

func (cfg *PollerConfig) lastStreamConfig() *StreamConfig {
	if _, ok := cfg.Streams[cfg.LastStream]; !ok {
		cfg.Streams[cfg.LastStream] = &StreamConfig{
			Arn: cfg.LastStream,
		}
	}
	if cfg.Streams[cfg.LastStream].Chards == nil {
		cfg.Streams[cfg.LastStream].Chards = map[string]types.Shard{}
	}
	return cfg.Streams[cfg.LastStream]
}

func (cfg *PollerConfig) clearChard(shardID string) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	delete(cfg.lastStreamConfig().Chards, shardID)
}

func (cfg *PollerConfig) addChard(shard types.Shard) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.lastStreamConfig().Chards[*shard.ShardId] = shard
}

type streamPoller struct {
	dynamo struct {
		table  string
		svc    *dynamodb.Client
		stmsvc *dynamodbstreams.Client
	}
	s3 struct {
		bucket string
		svc    *s3.Client
	}
	*PollerConfig
	onChangeFunc func(rec *types.Record) error
}

func newStreamPoller(
	dynamoSvc *dynamodb.Client,
	streamSvc *dynamodbstreams.Client,
	table string,
	s3svc *s3.Client,
	bucket string,
) *streamPoller {
	p := &streamPoller{
		dynamo: struct {
			table  string
			svc    *dynamodb.Client
			stmsvc *dynamodbstreams.Client
		}{
			table:  table,
			svc:    dynamoSvc,
			stmsvc: streamSvc,
		},
		s3: struct {
			bucket string
			svc    *s3.Client
		}{
			bucket: bucket,
			svc:    s3svc,
		},
	}
	return p
}

func (p *streamPoller) OnChange(f func(rec *types.Record) error) *streamPoller {
	p.onChangeFunc = f
	return p
}

func (p *streamPoller) fetchConfig(ctx context.Context) error {
	buf := s3manager.NewWriteAtBuffer([]byte{})
	if _, err := s3manager.NewDownloader(p.s3.svc).Download(ctx, buf, &s3.GetObjectInput{
		Key:    aws.String(PollerConfigLocation),
		Bucket: aws.String(p.s3.bucket),
	}); err != nil {
		var tce *s3types.NoSuchKey
		if errors.As(err, &tce) {
			p.PollerConfig = &PollerConfig{
				Streams: map[string]*StreamConfig{},
			}
			return nil
		}
		return err
	}
	if err := json.Unmarshal(buf.Bytes(), &p.PollerConfig); err != nil {
		return err
	}
	return nil
}

func (p *streamPoller) flushConfig(ctx context.Context) error {
	p.PollerConfig.At = time.Now()
	file, err := json.Marshal(p.PollerConfig)
	if err != nil {
		return err
	}

	if _, err = s3manager.
		NewUploader(p.s3.svc).
		Upload(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(p.s3.bucket),
			Key:         aws.String(PollerConfigLocation),
			Body:        bytes.NewReader(file),
			ContentType: aws.String("application/json"),
		}); err != nil {
		return err
	}
	return nil
}

func (p *streamPoller) getLatestStreamArn(ctx context.Context) (*string, error) {
	tableInfo, err := p.dynamo.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(p.dynamo.table)})
	if err != nil {
		return nil, err
	}
	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}
	return tableInfo.Table.LatestStreamArn, nil
}

func (p *streamPoller) getShardsInfos(ctx context.Context, stmArn *string, lastEvaluatedShardID *string) ([]types.Shard, *string, error) {
	des, err := p.dynamo.stmsvc.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn:             stmArn,
		ExclusiveStartShardId: lastEvaluatedShardID,
	})
	if err != nil {
		return nil, nil, err
	}
	return des.StreamDescription.Shards, des.StreamDescription.LastEvaluatedShardId, nil
}

// Poll the last stream of a dynamodb table and process records
func (p *streamPoller) Poll(ctx context.Context) (err error) {
	stmArn, err := p.getLatestStreamArn(ctx)
	if err != nil {
		return
	}
	if err = internal.Retry(5, 1*time.Second, func() error { return p.fetchConfig(ctx) }); err != nil {
		return
	}
	defer func() {
		graceCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		// err = Retry(5, 1*time.Second, func() error { return p.flushConfig(graceCtx) })
		err = p.flushConfig(graceCtx)
		log.Println("[DEBUG] Gracefully persist checkpoint cursor...", err)
		cancel()
	}()
	p.LastStream = aws.ToString(stmArn)

	var ch chan *types.Record = make(chan *types.Record, 1)

	// start record's processor
	go func() {
	PROC_LOOP:
		for {
			select {
			case rec := <-ch:
				if aws.ToString(rec.Dynamodb.SequenceNumber) <= p.cursor() {
					break
				}
				if rec.EventName != "INSERT" {
					break
				}
				if key := rec.Dynamodb.NewImage["_pk"].(*types.AttributeValueMemberS).Value; key == "internal" {
					break
				}
				// _ = internal.Retry(50, 1*time.Second, func() error { return p.onChangeFunc(rec) })
				_ = internal.Wait(1*time.Second, func() error {
					return p.onChangeFunc(rec)
				})

				p.setCursor(aws.ToString(rec.Dynamodb.SequenceNumber))
			case <-ctx.Done():
				break PROC_LOOP
			}
		}
	}()

	// start shards pollers
	var (
		ticker               *time.Ticker = time.NewTicker(1 * time.Second)
		tickerOnce           sync.Once
		lastEvaluatedShardID *string
	)

LOOP:
	for {
		select {
		case <-ticker.C:
			tickerOnce.Do(func() {
				ticker.Reset(10 * time.Second)
			})
			stmshards, lastShardID, perr := p.getShardsInfos(ctx, stmArn, lastEvaluatedShardID)
			if err != nil {
				err = perr
			}
			lastEvaluatedShardID = lastShardID
			// filter shard based on the seg number aka cursor
			shards := make([]types.Shard, 0)
			for _, shard := range stmshards {
				sh := shard
				if shard.SequenceNumberRange.EndingSequenceNumber == nil ||
					aws.ToString(shard.SequenceNumberRange.EndingSequenceNumber) > p.cursor() {
					shards = append(shards, sh)
				}
			}
			// foreach shard start a separate poller (msgs are only ordered per shard)
			for _, shard := range shards {
				if _, ok := p.lastStreamConfig().Chards[*shard.ShardId]; !ok {
					go func(shard *types.Shard) {
						var perr error
						shardID := aws.ToString(shard.ShardId)
						defer func() {
							if r := recover(); r != nil {
								perr = r.(error)
							}
							log.Printf("polling shard %s finished, err: %v\n", shardID, perr)
							p.clearChard(*shard.ShardId)
						}()
						log.Println("start polling shard", shardID)
						p.addChard(*shard)
						perr = p.pollShard(stmArn, shard, ch)
					}(&shard)
				}
			}
			time.Sleep(1 * time.Second)
		case <-ctx.Done():
			break LOOP
		}
	}
	return
}

func (p *streamPoller) pollShard(stmArn *string, shard *types.Shard, ch chan<- *types.Record) error {
	ctx := context.Background()

	iter, err := p.dynamo.stmsvc.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		StreamArn:         stmArn,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		return err
	}
	if iter.ShardIterator == nil {
		return nil
	}

	nextIter := iter.ShardIterator
	for nextIter != nil {
		recs, err := p.dynamo.stmsvc.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: nextIter,
			Limit:         aws.Int32(500),
		})
		if err != nil {
			return err
		}
		for _, rec := range recs.Records {
			ch <- &rec
		}
		nextIter = recs.NextShardIterator
		time.Sleep(1 * time.Second)
	}
	return nil
}
