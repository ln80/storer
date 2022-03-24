package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redaLaanait/storer/event"
	intevent "github.com/redaLaanait/storer/internal/event"
	"github.com/redaLaanait/storer/internal/timeutil"
	"github.com/redaLaanait/storer/json"
	"golang.org/x/sync/errgroup"
)

const (
	ProviderS3    = "s3"
	ProviderMinio = "minio"
)

const (
	FolderPartitions = "events"
	FolderChunks     = "chunks"
)

var (
	ErrStreamInvalidObjectKey  = errors.New("invalid stream object key")
	ErrStreamInvalidRangeKeys  = errors.New("invalid stream range keys")
	ErrStreamListObjectFailed  = errors.New("list stream object failed")
	ErrStreamMergeChunksFailed = errors.New("merge stream chunks failed")
	ErrStreamLoadChunksFailed  = errors.New("load stream chunks failed")
)

var objectKeyReg = regexp.MustCompile(
	"(\\w+)\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/(\\w+)\\/v(\\w+\\.\\w+)_(\\w+\\.\\w+)\\.(\\w+)",
)

// objectkey returns the key of the s3 object used for saving a chunk or a partition of events
func objectKey(stmID, root, ext string, date time.Time, fromVersion, toVersion event.Version) string {
	return fmt.Sprintf("%s/%s/%s/v%s_%s.%s", root, stmID, date.Format("2006/01/02"), fromVersion.String(), toVersion.String(), ext)
}

// parseObjectKey parses the object key and returns information about the chunk / partition.
// It may return an error if key format is invalid.
func parseObjectKey(key string) (root, stmID, day, fromVersion, toVersion, ext string, err error) {
	parts := objectKeyReg.FindStringSubmatch(key)
	if len(parts) != 9 {
		err = event.Err(ErrStreamInvalidObjectKey, stmID, "key: "+key)
		return
	}

	parts = parts[1:]

	root, stmID, day, fromVersion, toVersion, ext =
		parts[0],
		parts[1],
		fmt.Sprintf("%s/%s/%s", parts[2], parts[3], parts[4]),
		parts[5],
		parts[6],
		parts[7]

	return
}

// objectsRangeKeys returns the boundary keys of a range of chunks/partition object based on the given filter.
func objectsRangeKeys(stmID, root, ext string, f event.StreamFilter) (prefix, fromKey, toKey string) {
	f.Build()
	prefix = fmt.Sprintf("%s/%s/", root, stmID)
	fromKey = objectKey(stmID, root, ext, f.Since, f.From, f.From)
	toKey = objectKey(stmID, root, ext, f.Until, f.To, f.To)
	return
}

// mergeRangeKey returns the key of the object that contains the merge of chunks
func mergeRangeKey(mroot, from, to string) (key string, err error) {
	if len(from) == 0 || len(to) == 0 {
		err = event.Err(ErrStreamInvalidRangeKeys, "", fmt.Sprintf("keys: (%s, %s)", from, to))
		return
	}

	root1, stm1, day1, vfrom, _, ext1, err := parseObjectKey(from)
	if err != nil {
		return
	}
	root2, stm2, day2, _, vto, ext2, err := parseObjectKey(to)
	if err != nil {
		return
	}
	if root1 != root2 || stm1 != stm2 || day1 != day2 || ext1 != ext2 {
		err = event.Err(ErrStreamInvalidRangeKeys, stm1, fmt.Sprintf("keys: (%s, %s)", from, to))
		return
	}

	vf, err := event.ParseVersion(vfrom)
	if err != nil {
		return
	}
	vt, err := event.ParseVersion(vto)
	if err != nil {
		return
	}
	day, err := time.Parse("2006/01/02", day1)
	if err != nil {
		err = event.Err(ErrStreamInvalidRangeKeys, stm1, err.Error())
		return
	}

	key = objectKey(stm1, mroot, ext1, day, vf, vt)
	return
}

// makeObjectQuery returns the object SQL query to stream event from the object
func makeObjectQuery(provider string, filter event.StreamFilter) (query string) {
	// Hack: seems that Minio and S3 SQL are not compatible
	query = `SELECT * from`
	if provider == ProviderMinio {
		query += ` S3Object[*]._1[*]`
	} else {
		query += ` S3Object[*][*]`
	}
	query += fmt.Sprintf(` as ev
		WHERE
			ev.GVer BETWEEN '%s' AND '%s'
			AND
			ev.At BETWEEN %d AND %d

	`, filter.From.String(), filter.To.String(), filter.Since.UnixNano(), filter.Until.UnixNano())

	return query
}

// streamMerger presents the service that merge event chunks into partition.
// At this stage I see it as a specific interface of S3 package, but it may moves to internal package.
type streamMerger interface {
	mergeDailyChunks(ctx context.Context, stmID event.StreamID, date time.Time, currVer, lastActiveDayVer event.Version) (ignored bool, err error)
}

// StreamMaintainer defines the all on in one interface for s3 stream
type StreamMaintainer interface {
	// Streamer is part of the public API, it allows replaying the stream of events.
	event.Streamer

	streamMerger
}

var (
	_ StreamMaintainer   = &streamer{}
	_ intevent.Persister = &streamer{}
)

func newStreamService(svc ClientAPI, bucket string, opts ...func(cfg *StreamerConfig)) *streamer {
	if svc == nil {
		panic("streamer  invalid S3 client: nil value")
	}

	stmer := &streamer{
		svc:    svc,
		bucket: bucket,
		StreamerConfig: &StreamerConfig{
			Provider:               ProviderS3,
			ResumeWithLatestChunks: true,
			Serializer:             json.NewEventSerializer(""),
			PartitionMinSize:       100000,
		},
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(stmer.StreamerConfig)
	}

	return stmer
}

func NewStreamer(svc ClientAPI, bucket string, opts ...func(cfg *StreamerConfig)) event.Streamer {
	return newStreamService(svc, bucket, opts...)
}

func NewStreamePersister(svc ClientAPI, bucket string, opts ...func(cfg *StreamerConfig)) intevent.Persister {
	return newStreamService(svc, bucket, opts...)
}

func NewStreamMaintainer(svc ClientAPI, bucket string, opts ...func(cfg *StreamerConfig)) StreamMaintainer {
	return newStreamService(svc, bucket, opts...)
}

type streamer struct {
	bucket string

	svc ClientAPI

	*StreamerConfig
}

type StreamerConfig struct {
	ResumeWithLatestChunks bool
	Provider               string
	Serializer             event.Serializer
	PartitionMinSize       int
}

func (s *streamer) Persist(ctx context.Context, stmID event.StreamID, evts event.Stream) error {
	// if stm not global return err

	stm := event.Stream(evts)
	if stm.Empty() {
		return nil
	}

	if err := stm.Validate(func(v *event.Validation) {
		v.GlobalStream = true
	}); err != nil {
		return err
	}
	if stmID.String() != stm[0].GlobalStreamID() {
		return event.Err(event.ErrInvalidStream, stmID.String(), "found id: "+stm[0].GlobalStreamID())
	}

	chunk, err := s.Serializer.MarshalEventBatch(evts)
	if err != nil {
		return err
	}

	path := objectKey(stmID.String(), FolderChunks, s.Serializer.FileExt(), evts[0].At(), evts[0].GlobalVersion(),
		evts[len(evts)-1].GlobalVersion())

	if _, err = s3manager.
		NewUploader(s.svc).
		Upload(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(path),
			Body:        bytes.NewReader(chunk),
			ContentType: aws.String(s.Serializer.ContentType()),
		}); err != nil {
		return event.Err(event.ErrAppendEventsFailed, stmID.String(), err.Error())
	}

	return nil
}

func (s *streamer) queryObjectInput(bucket, key, query string) *s3.SelectObjectContentInput {
	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		Expression:     aws.String(query),
		ExpressionType: types.ExpressionType("SQL"),
	}

	switch s.Serializer.EventFormat() {
	case event.EventFormatJSON:
		input.InputSerialization = &types.InputSerialization{
			// CompressionType: aws.String("GZIP"),
			JSON: &types.JSONInput{Type: types.JSONType("DOCUMENT")},
		}
		input.OutputSerialization = &types.OutputSerialization{
			JSON: &types.JSONOutput{RecordDelimiter: aws.String("\n")},
		}
	}
	return input
}

func (s *streamer) listObject(ctx context.Context, stmID, root string, f event.StreamFilter) ([]string, error) {
	if len(stmID) == 0 {
		return nil, event.Err(event.ErrInvalidStream, stmID, "empty stream id")
	}

	prefix, minKey, maxKey := objectsRangeKeys(stmID, root, s.Serializer.FileExt(), f)

	keys := []string{}

	params := &s3.ListObjectsV2Input{
		Bucket:     aws.String(s.bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(minKey),
	}

	p := s3.NewListObjectsV2Paginator(s.svc, params)
	for loop := true; loop && p.HasMorePages(); {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, event.Err(ErrStreamListObjectFailed, stmID, err.Error())
		}
		for _, obj := range out.Contents {
			if aws.ToString(obj.Key) < minKey {
				continue
			}
			if aws.ToString(obj.Key) > maxKey {
				loop = false
				break
			}
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

type loadChunkRequest struct {
	key    string
	result chan []byte
}

type queryRequest struct {
	key    string
	result chan event.Envelope
}

// mergeDailyChunks merges chunks created during the day into a new partition object.
//
// The given current stream version and Last active day version are used to decide whether or not the minimum partition size is satisfied.
// The created partition will be based on all available daily chunks.
//
// Partition will end up created without satisfying the min size if the function is called in a different day than the given one.
//
// It may returns an ignored flag if create new partition conditions are not satisfied.
func (s *streamer) mergeDailyChunks(ctx context.Context, stmID event.StreamID, date time.Time, currVer, lastActiveDayVer event.Version) (ignored bool, err error) {
	begin, end := timeutil.BeginningOfDay(date), timeutil.EndOfDay(date)
	chunkf := event.StreamFilter{
		Since: begin,
		Until: end,
	}

	// check if partitions are already created during the day
	partKeys, err := s.listObject(ctx, stmID.GlobalID(), FolderPartitions, event.StreamFilter{
		Since: chunkf.Since,
		Until: chunkf.Until,
	})
	if err != nil {
		return false, err
	}

	// then make sure that the given current version allow us to create a new partition with minimum size.
	// if so, make sure to add the appropriate version to chunk filter
	if partCount := len(partKeys); partCount > 0 {
		// resolve today's partition version from the last found one key.
		lastPartKey := partKeys[partCount-1]
		_, _, _, _, partVerStr, _, err := parseObjectKey(lastPartKey)
		if err != nil {
			return false, err
		}
		partVer, err := event.Ver(partVerStr)
		if err != nil {
			return false, err
		}

		// load chunks starting from partitions version +1
		// Note that this filter helps to ensure idempotency.
		// however, data race may occur, controling the func callers is needed: serialization/queueing
		chunkf.From = partVer.Incr()

		// in this case the lastActiveDayVer is today's last partition version
		lastActiveDayVer = partVer
	}

	// Ignore merge if:
	//	- current version does not allow to create a new partition with min size.
	//  - given merge date is still today
	if lastActiveDayVer.Add(uint64(s.PartitionMinSize), 0).After(currVer) &&
		timeutil.DateEqual(time.Now().UTC(), begin) {
		return true, nil
	}

	chunKeys, err := s.listObject(ctx, stmID.GlobalID(), FolderChunks, chunkf)
	if err != nil {
		return false, err
	}
	chunkCount := len(chunKeys)
	if chunkCount == 0 {
		return false, nil
	}

	newPartKey, err := mergeRangeKey(FolderPartitions, chunKeys[0], chunKeys[chunkCount-1])
	if err != nil {
		return false, err
	}

	inst, ctx := s.newInstance(ctx)

	// run load chunks workers + the merge processor
	inst.runMergeChunks(ctx, stmID.String(), newPartKey, chunkCount,
		inst.runLoadChunks(ctx, stmID, chunKeys),
	)

	return false, inst.g.Wait()
}

func (s *streamer) queryObject(ctx context.Context, query, key string, queue chan event.Envelope) error {
	defer close(queue)
	resp, err := s.svc.SelectObjectContent(ctx, s.queryObjectInput(s.bucket, key, query))
	if err != nil {
		// return core.InfraError(err, fmt.Sprintf("failed to read stream from object '%s'", key))
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	r, w := io.Pipe()

	go func() {
		defer w.Close()

		for rawEv := range stream.Events() {

			switch v := rawEv.(type) {
			case *types.SelectObjectContentEventStreamMemberRecords:
				w.Write(v.Value.Payload)
				// case *types.SelectObjectContentEventStreamMemberEnd:
				// case *types.SelectObjectContentEventStreamMemberProgress:
				// case *types.SelectObjectContentEventStreamMemberCont:
				// case *types.SelectObjectContentEventStreamMemberStats:
			}
		}
	}()

	if err := s.Serializer.Decode(ctx, r, queue); err != nil {
		return err
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("failed to finish reading stream from object '%s' %v", key, err)
	}
	return nil
}

// resumeWithChunks returns the same filter used to query partitions or a one that complete it
// based on the last queried partition.
func resumeWithChunks(f event.StreamFilter, keys []string) (event.StreamFilter, error) {
	chf := f
	if kl := len(keys); kl != 0 {
		_, _, day, _, toVer, _, err := parseObjectKey(keys[kl-1])
		if err != nil {
			return chf, err
		}
		chf.From, err = event.ParseVersion(toVer)
		if err != nil {
			return chf, err
		}
		chf.Since, _ = time.Parse("2006/01/02", day)
	}
	chf.Build()
	return chf, nil
}

// Replay queries a window of the stream and process the events in order.
// It fails if the stream is corrupted e.g an invalid sequence is  somehow encountered.
func (s *streamer) Replay(ctx context.Context, id event.StreamID, f event.StreamFilter, h event.StreamHandler) error {
	f.Build()

	partkeys, err := s.listObject(ctx, id.GlobalID(), FolderPartitions, f)
	if err != nil {
		return err
	}

	var (
		chunkeys []string
		chunkf   event.StreamFilter
	)
	if s.ResumeWithLatestChunks {
		chunkf, err = resumeWithChunks(f, partkeys)
		if err != nil {
			return err
		}
		chunkeys, err = s.listObject(ctx, id.GlobalID(), FolderChunks, chunkf)
		if err != nil {
			return err
		}
	}
	if len(partkeys)+len(chunkeys) == 0 {
		return nil
	}

	inst, ctx := s.newInstance(ctx)

	envch := map[string]chan event.Envelope{}
	for _, part := range partkeys {
		envch[part] = make(chan event.Envelope, 1000)
	}
	if s.ResumeWithLatestChunks && len(chunkeys) > 0 {
		// to avoide goroutine race, make sure to initial chunks channel before spinning partition workers up
		envch["_chunks"] = inst.runUnmarchalChunks(
			ctx,
			inst.runLoadChunks(ctx, id, chunkeys),
		)
	}

	inst.runQueryObjects(ctx, partkeys, f, envch)
	inst.runProcess(ctx, id.String(), partkeys, envch, f, chunkf, h)

	return inst.g.Wait()
}

func (s *streamer) newInstance(ctx context.Context) (*streamInstance, context.Context) {
	g, ctx := errgroup.WithContext(ctx)

	return &streamInstance{
		streamer: s,
		g:        g,
	}, ctx
}

// streamInstance defines a session of work that handles operations such as replay stream, merge chunks...
// It mainly deals concurrent failures, and allow draining channels in downstream goroutines
type streamInstance struct {
	*streamer

	g *errgroup.Group

	errm sync.Mutex
	err  error
}

func (s *streamInstance) getErr() error {
	s.errm.Lock()
	defer s.errm.Unlock()
	return s.err
}

func (s *streamInstance) setErr(err error) error {
	s.errm.Lock()
	defer s.errm.Unlock()
	s.err = err

	return s.err
}

func (s *streamInstance) runLoadChunks(ctx context.Context, stmID event.StreamID, keys []string) (out chan []byte) {
	out = make(chan []byte)
	if len(keys) == 0 {
		close(out)
		return
	}

	var workPoolSize int
	if s1, s2 := len(keys), 20; s1 < s2 {
		workPoolSize = s1
	} else {
		workPoolSize = s2
	}
	work := make(chan loadChunkRequest, workPoolSize)

	results := make(map[string]chan []byte, len(keys))
	for _, key := range keys {
		results[key] = make(chan []byte)
	}

	for i := 0; i < workPoolSize; i++ {
		s.g.Go(func() error {
			downloader := s3manager.NewDownloader(s.svc, func(d *s3manager.Downloader) {
				// chunks are small pieces of events, no need to internally spin up many workers
				d.Concurrency = 1
			})
			for loop := true; loop; {
				select {
				case <-ctx.Done():
					return s.setErr(ctx.Err())
				default:
					req, ok := <-work
					if !ok {
						loop = false
						break
					}
					if err := s.getErr(); err != nil {
						return err
					}

					if err := func() error {
						defer close(req.result)
						buf := s3manager.NewWriteAtBuffer([]byte{})
						_, err := downloader.Download(ctx, buf, &s3.GetObjectInput{
							Key:    aws.String(req.key),
							Bucket: aws.String(s.bucket),
						})
						if err != nil {
							return err
						}
						req.result <- buf.Bytes()
						return nil
					}(); err != nil {
						return s.setErr(event.Err(ErrStreamLoadChunksFailed, stmID.String(), err))
					}
				}
			}
			return nil
		})
	}

	s.g.Go(func() error {
		defer close(work)
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return s.setErr(ctx.Err())
			case work <- loadChunkRequest{
				key, results[key],
			}:
			}
		}
		return nil
	})

	s.g.Go(func() error {
		defer close(out)
		for _, key := range keys {
			for loop := true; loop; {
				select {
				case <-ctx.Done():
					return s.setErr(ctx.Err())
				case r, ok := <-results[key]:
					if !ok {
						loop = false
						break
					}
					out <- r
				}
			}
		}
		return nil
	})
	return
}

func (s *streamInstance) runUnmarchalChunks(ctx context.Context, in chan []byte) (out chan event.Envelope) {
	out = make(chan event.Envelope)
	s.g.Go(func() error {
		defer close(out)
		for loop := true; loop; {
			select {
			case <-ctx.Done():
				if s.getErr() != nil {
					s.setErr(ctx.Err())
				}
			case b, ok := <-in:
				if !ok {
					loop = false
					break
				}
				if s.getErr() != nil {
					break // break select and continue the for loop
				}
				envs, err := s.Serializer.UnmarshalEventBatch(b)
				if err != nil {
					s.setErr(err)
					break
				}
				for _, env := range envs {
					if s.getErr() != nil {
						break
					}
					if env.Event() == nil {
						s.setErr(event.Err(event.ErrInvalidStream, env.StreamID(),
							"empty event data is streamed", "it's likely to be a lazily unmarshaling issue"))
						break
					}
					out <- env
				}
			}
		}
		return nil
	})
	return
}

func (s *streamInstance) runQueryObjects(ctx context.Context, keys []string, f event.StreamFilter, partch map[string]chan event.Envelope) {
	var workPoolSize int
	if s1, s2 := len(keys), 8; s1 < s2 {
		workPoolSize = s1
	} else {
		workPoolSize = s2
	}
	work := make(chan queryRequest, workPoolSize)

	s.g.Go(func() error {
		defer close(work)
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return s.setErr(ctx.Err())
			case work <- queryRequest{key, partch[key]}:
			}
		}
		return nil
	})
	for i := 0; i < workPoolSize; i++ {
		s.g.Go(func() error {
			for {
				req, ok := <-work
				if !ok {
					break
				}
				if err := s.queryObject(ctx, makeObjectQuery(s.Provider, f), req.key, req.result); err != nil {
					return s.setErr(err)
				}
			}
			return nil
		})
	}
}

// runProcess in a separate goroutine
// partitions are consumed in order,events loaded from chunks are consumed last
func (s *streamInstance) runProcess(ctx context.Context, stmID string, parts []string, ch map[string]chan event.Envelope, f, chunkf event.StreamFilter, h event.StreamHandler) {
	s.g.Go(func() error {
		f.Build()
		cur := event.NewCursor(stmID)

		for _, key := range parts {
			s.process(ctx, ch[key], cur, f, h)
		}

		if s.ResumeWithLatestChunks {
			chunkf.Build()
			chunkch, ok := ch["_chunks"]
			if ok {
				s.process(ctx, chunkch, cur, chunkf, h)
			}
		}

		return nil
	})
}

// process events in order from the given channel
// it gracefuly stops processing events if an error has occur, while continuing to drain the channel
// the err is found in the instance object
func (s *streamInstance) process(ctx context.Context, envCh chan event.Envelope, c *event.Cursor, f event.StreamFilter, h event.StreamHandler) {
	for loop := true; loop; {
		select {
		case <-ctx.Done():
			s.setErr(ctx.Err())
		case env, ok := <-envCh:
			if !ok {
				loop = false
				break
			}
			if s.getErr() != nil {
				break
			}
			ignore, err := event.ValidateEvent(env, c, func(v *event.Validation) {
				v.GlobalStream = true
				v.Filter = f
			})
			if err != nil {
				s.setErr(err)
				break
			}
			if ignore {
				break
			}
			if err := h(ctx, env); err != nil {
				s.setErr(err)
			}
		}
	}
}

func (s *streamInstance) runMergeChunks(ctx context.Context, stmID string, destkey string, count int, chunks chan []byte) {
	r, w := io.Pipe()

	s.g.Go(func() error {
		defer w.Close()
		if err := s.Serializer.Concat(ctx, count, chunks, func(b []byte) (err error) {
			if s.getErr() != nil {
				return
			}
			_, err = w.Write(b)
			return
		}); err != nil {
			s.setErr(event.Err(ErrStreamMergeChunksFailed, stmID, err))
		}
		return nil
	})

	// seems that uploader requires read all body to perform upload
	// see: https://github.com/aws/aws-sdk-go/issues/2228
	s.g.Go(func() error {
		uploader := s3manager.NewUploader(s.svc, func(u *s3manager.Uploader) {
			u.Concurrency = 1
		})
		if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(destkey),
			Body:        r,
			ContentType: aws.String(s.Serializer.ContentType()),
		}); err != nil {
			return event.Err(ErrStreamMergeChunksFailed, stmID, err)
		}

		return nil
	})
}
