package sqs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redaLaanait/storer/signal"
)

type SignalConfig struct {
	BufferSize int
}

type signalMgr struct {
	svc   ClientAPI
	queue string

	buff   []signal.Signal
	muBuff sync.RWMutex

	*SignalConfig
}

func NewSignalManager(svc ClientAPI, queue string, opts ...func(cfg *SignalConfig)) signal.Manager {
	if svc == nil {
		panic("signal manager invalid SQS client: nil value")
	}
	sigmng := &signalMgr{
		svc:          svc,
		queue:        queue,
		SignalConfig: &SignalConfig{BufferSize: 10},
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(sigmng.SignalConfig)
	}
	return sigmng
}

var _ signal.Sender = &signalMgr{}

func (s *signalMgr) Send(ctx context.Context, sig signal.Signal) error {
	if sig == nil {
		return signal.ErrSignalNotFound
	}
	s.appendSignal(sig)

	if s.bufferSize() >= s.SignalConfig.BufferSize {
		s.FlushBuffer(ctx, nil)
	}
	return nil
}

func (s *signalMgr) FlushBuffer(ctx context.Context, terr *error) error {
	// No need to flush if an error already occured + passed to flush method.
	// Flush call is likely done in a defer fashion
	if terr != nil && *terr != nil {
		return *terr
	}
	if terr == nil {
		terr = new(error)
	}

	s.muBuff.Lock()
	defer s.muBuff.Unlock()

	count := len(s.buff)
	if count == 0 {
		return nil
	}

	bsigs, err := s.marshalSignals(s.buff)
	if err != nil {
		*terr = fmt.Errorf("%w: %v", signal.ErrSendSignalFailed, err.Error())
		return *terr
	}

	entries := make([]types.SendMessageBatchRequestEntry, count)
	for i, bsig := range bsigs {
		sigid := s.buff[i].StreamID() + "_" + s.buff[i].SignalName() + "_" + strconv.Itoa(i) + "_at" + strconv.Itoa(int(time.Now().Unix()))
		bhash := md5.Sum(bsig)
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:                     aws.String(sigid),
			MessageGroupId:         aws.String(s.buff[i].StreamID()),
			MessageDeduplicationId: aws.String(hex.EncodeToString(bhash[:])),
			MessageBody:            aws.String(string(bsig)),
		}
	}

	if _, err := s.svc.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(s.queue),
	}); err != nil {
		*terr = fmt.Errorf("%w: %v", signal.ErrSendSignalFailed, err)
		return *terr
	}

	// clear buffer
	s.buff = make([]signal.Signal, 0)

	return nil
}

func (s *signalMgr) Receive(ctx context.Context, raws [][]byte, p signal.Processor) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %v", signal.ErrReceiveSignalFailed, err)
		}
	}()

	sigs, err := s.unmarshalSignals(raws)
	if err != nil {
		return err
	}
	for _, sig := range sigs {
		if err := p(ctx, sig); err != nil {
			return err
		}
	}

	return nil
}

func (s *signalMgr) marshalSignals(sigs []signal.Signal) ([][]byte, error) {
	bsigs := make([][]byte, len(sigs))
	for i, sig := range sigs {
		bsig, err := json.Marshal(sig)
		if err != nil {
			return nil, err
		}
		bsigs[i] = bsig
	}
	return bsigs, nil
}

func (s *signalMgr) unmarshalSignals(raws [][]byte) ([]signal.Signal, error) {
	sigs := make([]signal.Signal, len(raws))
	for i, raw := range raws {
		rawSig := signal.BaseSignal{}
		if err := json.Unmarshal(raw, &rawSig); err != nil {
			return nil, err
		}

		sig := signal.NewSignal(rawSig.SignalName())
		if err := json.Unmarshal(raw, sig); err != nil {
			return nil, err
		}
		sigs[i] = sig
	}
	return sigs, nil
}

func (s *signalMgr) appendSignal(sig signal.Signal) {
	s.muBuff.Lock()
	defer s.muBuff.Unlock()

	s.buff = append(s.buff, sig)
}

func (s *signalMgr) bufferSize() int {
	s.muBuff.Lock()
	defer s.muBuff.Unlock()

	return len(s.buff)
}
