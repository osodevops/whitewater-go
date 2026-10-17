package whitewater

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	consumerInitialBackoff = 250 * time.Millisecond
	consumerMaxBackoff     = 30 * time.Second
)

// ErrConsumerClosed marks a delivery abandoned because the consumer
// was closed mid-page -- internal, used only to unwind sends.
var ErrConsumerClosed = errors.New("whitewater: consumer closed")

// FeedConsumer is a permanent, supervised consumer bound to a Feed's
// named Reader -- the durable-Reader equivalent of Kafka's
// "for { consumer.poll }" loop with the plumbing removed:
//
//	consumer := feed.Consumer("enricher")
//	for msg := range consumer.Messages(ctx) {
//	    process(msg)
//	}
//	if err := consumer.Err(); err != nil { ... }
//
// or the handler form, which blocks until ctx cancellation or Close:
//
//	err := feed.Consumer("enricher").Run(ctx, func(m FeedMessage[K, V]) error {
//	    return process(m)
//	})
//
// Semantics:
//   - The named Reader's acked cursor is durable server-side -- a restart
//     (app or session) resumes where it stopped; no offset management.
//   - Each fetched page is delivered record-by-record; its delivered
//     cursor acks only after every record's delivery succeeded -- a
//     handler error (Run) or abandoned read (Messages) leaves the page
//     unacked so it redelivers (at-least-once). Poison pills are the
//     app's job -- route them to a DLQ feed.
//   - Transient failures retry with exponential backoff inside the
//     supervisor; a fenced session re-opens and resumes. Startup
//     refusals (bad names, denied auth) surface on Err()/Run's return --
//     fail fast, never loop.
//   - Close (or ctx cancellation) finishes the in-flight page, releases
//     the session, and lets Messages drain/Run return -- the SIGTERM
//     path. Close is safe to call from a signal handler or handler.
type FeedConsumer[K comparable, V any] struct {
	feed   *Feed[K, V]
	reader string

	start          ReaderStart
	capacity       int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	onError        func(error)

	mu      sync.Mutex
	lastErr error
	err     error

	closing  chan struct{}
	finished chan struct{}
	closeOne sync.Once
	started  bool
}

func newFeedConsumer[K comparable, V any](
	feed *Feed[K, V], reader string,
) *FeedConsumer[K, V] {
	return &FeedConsumer[K, V]{
		feed:           feed,
		reader:         reader,
		start:          StartBeginning(),
		capacity:       subscribeCapacity,
		initialBackoff: consumerInitialBackoff,
		maxBackoff:     consumerMaxBackoff,
		closing:        make(chan struct{}),
		finished:       make(chan struct{}),
	}
}

// Reader returns the declared Reader's name -- its durable identity.
func (c *FeedConsumer[K, V]) Reader() string { return c.reader }

// StartAt sets where the Reader starts on first declaration (default
// BEGINNING); ignored once the Reader exists -- it resumes from durable
// progress.
func (c *FeedConsumer[K, V]) StartAt(start ReaderStart) *FeedConsumer[K, V] {
	c.start = start
	return c
}

// Capacity sets records fetched per page (default 128).
func (c *FeedConsumer[K, V]) Capacity(capacity int) *FeedConsumer[K, V] {
	c.capacity = capacity
	return c
}

// Backoff sets restart backoff bounds (default 250ms -> 30s).
func (c *FeedConsumer[K, V]) Backoff(
	initial, max time.Duration,
) *FeedConsumer[K, V] {
	c.initialBackoff = initial
	c.maxBackoff = max
	return c
}

// OnError registers a sink for supervised failures -- plumbing errors
// and handler errors alike. Visibility only; the loop continues.
func (c *FeedConsumer[K, V]) OnError(fn func(error)) *FeedConsumer[K, V] {
	c.onError = fn
	return c
}

// LastError returns the last supervised failure, when any.
func (c *FeedConsumer[K, V]) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

// Err returns the terminal error -- a startup refusal or a silent
// subscription death the supervisor refused to restart. Nil when the
// consumer ended cleanly (Close or ctx cancellation). Valid after the
// Messages channel closes or Run returns.
func (c *FeedConsumer[K, V]) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Messages delivers decoded records on a channel -- read until it
// closes (Close or ctx done), then check Err. The page acks after all
// its records are sent; abandoning the channel mid-page leaves it
// unacked for redelivery.
func (c *FeedConsumer[K, V]) Messages(
	ctx context.Context,
) <-chan FeedMessage[K, V] {
	out := make(chan FeedMessage[K, V])
	if !c.claimStart() {
		close(out)
		return out
	}
	go c.supervise(ctx, func(ctx context.Context, msg FeedMessage[K, V]) error {
		select {
		case out <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closing:
			return ErrConsumerClosed
		}
	}, out)
	return out
}

// Run delivers decoded records to handler one at a time, blocking
// until Close or ctx cancellation. A handler error leaves the page
// unacked and the consumer redelivers after backoff; Run returns the
// terminal error only for startup refusals -- everything else is
// supervised forever.
func (c *FeedConsumer[K, V]) Run(
	ctx context.Context,
	handler func(FeedMessage[K, V]) error,
) error {
	if !c.claimStart() {
		return fmt.Errorf("whitewater: consumer %q already started", c.reader)
	}
	go c.supervise(ctx, func(_ context.Context, msg FeedMessage[K, V]) error {
		return handler(msg)
	}, nil)
	<-c.finished
	return c.Err()
}

// Close signals stop: the in-flight page finishes and acks, the
// session releases, the Messages channel drains and Run returns. It
// only signals -- Wait/Done await termination. Safe from anywhere,
// including inside a handler.
func (c *FeedConsumer[K, V]) Close() {
	c.closeOne.Do(func() { close(c.closing) })
}

// Wait blocks until the supervised loop has fully stopped.
func (c *FeedConsumer[K, V]) Wait() {
	<-c.finished
}

// Done closes when the supervised loop has fully stopped.
func (c *FeedConsumer[K, V]) Done() <-chan struct{} { return c.finished }

// claimStart claims the single supervise lifetime -- a second Messages/Run
// would fence the same Reader against itself.
func (c *FeedConsumer[K, V]) claimStart() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return false
	}
	c.started = true
	return true
}

func (c *FeedConsumer[K, V]) stopped() bool {
	select {
	case <-c.closing:
		return true
	default:
		return false
	}
}

// supervise runs subscription lifecycles forever until closed: a death
// with a retryable cause re-opens after backoff; Refused (fix the
// request) or a death with no cause (programming error) fail fast.
func (c *FeedConsumer[K, V]) supervise(
	ctx context.Context,
	deliver func(context.Context, FeedMessage[K, V]) error,
	out chan<- FeedMessage[K, V],
) {
	if out != nil {
		defer close(out)
	}
	defer close(c.finished)

	backoff := c.initialBackoff
	for {
		if c.stopped() || ctx.Err() != nil {
			return
		}
		var iterErr error
		sub := c.feed.subscribeDecoded(
			ctx, c.reader, c.start, c.capacity,
			func(messages []FeedMessage[K, V]) error {
				for _, message := range messages {
					if err := deliver(ctx, message); err != nil {
						iterErr = err
						return err
					}
				}
				return nil
			},
			nil)
		select {
		case <-sub.Done():
		case <-c.closing:
			// Graceful drain: Close signals the subscription's own
			// stop so the in-flight page finishes and acks -- the
			// request ctx must not die underneath it.
			sub.Close()
		}
		sub.Close()
		if c.stopped() || ctx.Err() != nil {
			return
		}
		err := sub.Err()
		if err == nil {
			err = iterErr
		}
		switch {
		case err == nil:
			// Died with nothing to supervise.
			c.setErr(fmt.Errorf(
				"subscription %q terminated without an error to supervise",
				c.reader))
			return
		case iterErr == nil && (IsRefused(err) || !isWireError(err)):
			// Refusals and local (validation/decode/programming) errors
			// mean "fix the request" -- fail fast, never restart.
			c.setErr(err)
			return
		}
		c.setLast(err)
		if c.onError != nil {
			func() {
				defer func() { _ = recover() }() // error sinks never break
				c.onError(err)                   // supervision
			}()
		}
		if !c.sleep(ctx, backoff) {
			return
		}
		if backoff < c.maxBackoff {
			backoff *= 2
			if backoff > c.maxBackoff {
				backoff = c.maxBackoff
			}
		}
	}
}

func (c *FeedConsumer[K, V]) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-c.closing:
		return false
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *FeedConsumer[K, V]) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *FeedConsumer[K, V]) setLast(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastErr = err
}

// isWireError reports whether err came back off the wire as a typed
// *Error (so its Kind is meaningful) rather than a local failure.
func isWireError(err error) bool {
	var target *Error
	return errors.As(err, &target)
}
