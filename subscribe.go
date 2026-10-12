package whitewater

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	loadPageSize      = 512
	subscribeCapacity = 128

	emptyPollDelay = 250 * time.Millisecond
	retryDelay     = time.Second
)

// Load bulk-reads committed Feed history once, from start, delivering each
// page to sink until the current frontier is drained, and returns the
// number of records delivered. Use Client.Subscribe when the sink should
// keep receiving.
//
// StartNow() loads nothing (the frontier is empty at call time);
// StartCursor/StartTimestampNs resume from that point.
func (f *FeedClient) Load(
	ctx context.Context,
	feed string,
	start ReaderStart,
	sink func([]Record) error,
) (int64, error) {
	options := loadFirstPage(start)
	var loaded int64
	for {
		page, err := f.TemporaryFetch(ctx, feed, options)
		if err != nil {
			return loaded, err
		}
		if len(page.Records) == 0 {
			return loaded, nil
		}
		if err := sink(page.Records); err != nil {
			return loaded, err
		}
		loaded += int64(len(page.Records))
		options = TemporaryFetchOptions{After: page.NextCursor, Limit: loadPageSize}
	}
}

func loadFirstPage(start ReaderStart) TemporaryFetchOptions {
	switch start.Kind {
	case StartKindNow:
		return TemporaryFetchOptions{NewOnly: true}
	case StartKindCursor:
		return TemporaryFetchOptions{After: start.Cursor, Limit: loadPageSize}
	case StartKindTimestamp:
		nanos := start.TimestampNs
		return TemporaryFetchOptions{AfterEventTimeNs: &nanos, Limit: loadPageSize}
	default:
		return TemporaryFetchOptions{Limit: loadPageSize}
	}
}

// Subscription is a running feed subscription: a named Reader session
// polled on a background goroutine, invoking onRecords per fetched page
// and acknowledging durable progress after each delivered batch.
//
// Error policy: transient refusals (IsUnavailable, IsAmbiguous) retry
// after a short pause. A stale session — another client opened the same
// Reader — terminates the subscription rather than fencing back; any
// other failure also terminates. Terminal errors reach onError and
// Err().
type Subscription struct {
	done   chan struct{}
	err    error
	closed chan struct{}
	once   sync.Once
}

// Subscribe opens a durable subscription: it declares the named Reader
// when missing (creating it at start), opens a session, and invokes
// onRecords per fetched page in the background, acknowledging after each
// batch. The Subscription runs until Close or ctx cancellation; terminal
// errors reach onError (when non-nil) and Err().
func (c *Client) Subscribe(
	ctx context.Context,
	reader string,
	feed string,
	start ReaderStart,
	onRecords func([]Record) error,
	onError func(error),
) *Subscription {
	sub := &Subscription{done: make(chan struct{}), closed: make(chan struct{})}
	go sub.run(ctx, c, reader, feed, start, onRecords, onError)
	return sub
}

// Err reports the terminal error, when the loop stopped on a failure.
func (s *Subscription) Err() error {
	<-s.done
	return s.err
}

// Done closes when the subscription loop exits.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Close signals stop, releases the Reader session, and waits for the
// loop to exit.
func (s *Subscription) Close() {
	s.once.Do(func() { close(s.closed) })
	<-s.done
}

func (s *Subscription) run(
	ctx context.Context,
	client *Client,
	readerName string,
	feed string,
	start ReaderStart,
	onRecords func([]Record) error,
	onError func(error),
) {
	defer close(s.done)
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-s.closed:
			cancel()
		case <-loopCtx.Done():
		}
	}()

	if err := client.ensureReader(loopCtx, readerName, feed, start); err != nil {
		s.fail(err, onError)
		return
	}
	reader := client.Reader(readerName)
	var session ReaderSession
	for {
		var err error
		session, err = reader.Open(loopCtx, subscribeCapacity)
		if err == nil {
			break
		}
		if !retryable(err) || !sleepOrDone(loopCtx, retryDelay) {
			s.fail(err, onError)
			return
		}
	}
	defer func() {
		// Release on a fresh context — loopCtx may be cancelled already.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_, _ = reader.Close(closeCtx, session)
	}()
	for {
		page, err := reader.Fetch(loopCtx, session, subscribeCapacity)
		if err != nil {
			if !retryable(err) || !sleepOrDone(loopCtx, retryDelay) {
				s.fail(err, onError)
				return
			}
			continue
		}
		if len(page.Records) == 0 {
			if !sleepOrDone(loopCtx, emptyPollDelay) {
				return
			}
			continue
		}
		if err := onRecords(page.Records); err != nil {
			s.fail(err, onError)
			return
		}
		if page.DeliveredCursor != nil {
			next, err := reader.Ack(loopCtx, session, *page.DeliveredCursor)
			if err != nil {
				if !retryable(err) || !sleepOrDone(loopCtx, retryDelay) {
					s.fail(err, onError)
					return
				}
				continue
			}
			session = next
		}
	}
}

func (s *Subscription) fail(err error, onError func(error)) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Local cancellation after Close() is not a subscription failure.
		select {
		case <-s.closed:
			return
		default:
		}
	}
	s.err = err
	if onError != nil {
		onError(err)
	}
}

// ensureReader declares the Reader when missing; an existing Reader is
// reused as-is.
func (c *Client) ensureReader(
	ctx context.Context,
	name string,
	feed string,
	start ReaderStart,
) error {
	if _, err := c.CreateReader(ctx, name, feed, start); err != nil {
		if !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	return nil
}

func retryable(err error) bool {
	return IsUnavailable(err) || IsAmbiguous(err)
}

// sleepOrDone sleeps until d fires or ctx is done; false means stop.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
