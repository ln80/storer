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

func Retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		log.Println("[ERROR] Retry:", err)
		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			// jitter := time.Duration(rand.Int63n(int64(sleep)))
			// sleep = sleep + jitter/2

			time.Sleep(sleep)
			return Retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}

func Wait(sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		time.Sleep(sleep)
	}
	return nil
}

type Poller struct {
	appName string
	dynamo  struct {
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

func NewPoller(
	appName string,
	dynamoSvc *dynamodb.Client,
	streamSvc *dynamodbstreams.Client,
	table string,
	s3svc *s3.Client,
	bucket string,
) *Poller {
	p := &Poller{
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

func (p *Poller) OnChange(f func(rec *types.Record) error) *Poller {
	p.onChangeFunc = f
	return p
}

func (p *Poller) fetchConfig(ctx context.Context) error {
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
		// if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NoSuchKey" {
		// 	p.PollerConfig = &PollerConfig{
		// 		Streams: map[string]*StreamConfig{},
		// 	}
		// 	return nil
		// }
		return err
	}
	if err := json.Unmarshal(buf.Bytes(), &p.PollerConfig); err != nil {
		return err
	}
	return nil
}

func (p *Poller) flushConfig(ctx context.Context) error {
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

func (p *Poller) getLatestStreamArn(ctx context.Context) (*string, error) {
	tableInfo, err := p.dynamo.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(p.dynamo.table)})
	if err != nil {
		return nil, err
	}
	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}
	return tableInfo.Table.LatestStreamArn, nil
}

func (p *Poller) getShardsInfos(ctx context.Context, stmArn *string, lastEvaluatedShardID *string) ([]types.Shard, *string, error) {
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
func (p *Poller) Poll(ctx context.Context) (err error) {
	stmArn, err := p.getLatestStreamArn(ctx)
	if err != nil {
		return
	}
	if err = Retry(5, 1*time.Second, func() error { return p.fetchConfig(ctx) }); err != nil {
		return
	}
	defer func() {
		graceCtx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
		// err = Retry(5, 1*time.Second, func() error { return p.flushConfig(graceCtx) })
		err = p.flushConfig(graceCtx)
		log.Println("[DEBUG] Gracefully persist checkpoint cursor...", err)
		cancel()
	}()
	p.LastStream = aws.ToString(stmArn)

	var ch chan *types.Record = make(chan *types.Record, 1)

	// start record's processor
	go func() {
	procloop:
		for {
			select {
			case rec := <-ch:
				if aws.ToString(rec.Dynamodb.SequenceNumber) <= p.cursor() {
					break
				}
				_ = Retry(50, 1*time.Second, func() error { return p.onChangeFunc(rec) })

				p.setCursor(aws.ToString(rec.Dynamodb.SequenceNumber))
			case <-ctx.Done():
				break procloop
			}
		}
	}()

	// start shards pollers
	var (
		tick                 <-chan time.Time = time.Tick(1 * time.Second)
		tickonce             sync.Once
		lastEvaluatedShardID *string
	)

loop:
	for {
		select {
		case <-tick:
			tickonce.Do(func() {
				tick = time.Tick(10 * time.Second)
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
			break loop
		}
	}
	return
}

func (p *Poller) pollShard(stmArn *string, shard *types.Shard, ch chan<- *types.Record) error {
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
