package sqs

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/redaLaanait/storer/signal"
)

func TestSignalManager(t *testing.T) {
	ctx := context.Background()

	queue := "http://queue.url"

	sigMgr := NewSignalManager(&clientMock{}, queue)

	// helper gen funcs
	genSignal := func() signal.Signal {
		return signal.ActiveStreamSignal(
			strconv.Itoa(time.Now().Nanosecond()),
			time.Now().Add(-20*time.Second).Unix(),
			time.Now().Unix(),
		)
	}
	genSignals := func(count int) []signal.Signal {
		sigs := make([]signal.Signal, count)
		for i := 0; i < count; i++ {
			sigs[i] = signal.ActiveStreamSignal(
				strconv.Itoa(i),
				time.Now().Add(-20*time.Second).Unix(),
				time.Now().Unix(),
			)
		}
		return sigs
	}

	// test send empty signal
	if wantErr, err := signal.ErrSignalNotFound, sigMgr.Send(ctx, nil); !errors.Is(err, wantErr) {
		t.Fatalf("expect err be %v, got %v", wantErr, err)
	}

	// test send signal is buffered
	sig1 := genSignal()
	if err := sigMgr.Send(ctx, sig1); err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, got := 1, len(sigMgr.(*signalMgr).buff); want != got {
		t.Fatalf("expect %d, %d be equals", want, got)
	}

	// test flush buffered signals with failure
	sigMgr.(*signalMgr).svc = &clientMock{err: errors.New("sqs error")}
	var terr error
	sigMgr.FlushBuffer(ctx, &terr)
	if wantErr, err := signal.ErrSendSignalFailed, terr; !errors.Is(err, wantErr) {
		t.Fatalf("expect err be %v, got %v", wantErr, err)
	}

	// test flush send signals with success
	sigMgr.(*signalMgr).svc = &clientMock{}
	var terr2 error
	sigMgr.FlushBuffer(ctx, &terr2)
	if terr2 != nil {
		t.Fatalf("expect err be nil, got %v", terr2)
	}

	// test send buffered signals is flushed when buffer limit is reached
	bufferSize := 10
	sigMgr = NewSignalManager(&clientMock{}, queue, func(cfg *SignalConfig) {
		cfg.bufferSize = bufferSize
	})
	sigs := genSignals(bufferSize)
	for i, sig := range sigs {
		if err := sigMgr.Send(ctx, sig); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if i < bufferSize-1 {
			if want, got := i+1, len(sigMgr.(*signalMgr).buff); want != got {
				t.Fatalf("expect %d, %d be equals", want, got)
			}
		} else {
			// send the last signal must trigger the flush action
			if want, got := 0, len(sigMgr.(*signalMgr).buff); want != got {
				t.Fatalf("expect %d, %d be equals", want, got)
			}
		}
	}

	// test receive signals
	traces := sigMgr.(*signalMgr).svc.(*clientMock).traces[queue]
	// make sure traces exists and the count is the same as buffer size
	if wantl, l := bufferSize, len(traces); wantl != l {
		t.Fatalf("expect %d, %d be equals", wantl, l)
	}
	// extract last sent signals from traces
	raws := make([][]byte, bufferSize)
	for i, trace := range traces {
		raws[i] = []byte(aws.ToString(trace.MessageBody))
	}
	// use processor func + receive counter to assert data integrity
	receiveCount := 0
	if err := sigMgr.Receive(ctx, raws, func(ctx context.Context, sig signal.Signal) error {
		if want, got := signal.SigActiveStream, sig.SignalName(); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		if want, got := sigs[receiveCount], sig; !reflect.DeepEqual(want, got) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		receiveCount++
		return nil
	}); err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if wantl, l := bufferSize, receiveCount; wantl != l {
		t.Fatalf("expect %d, %d be equals", wantl, l)
	}
}
