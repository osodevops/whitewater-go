package whitewater

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// FeedMessage is one committed Feed record with its key and payload
// already decoded through the bound codecs — the Record fields are
// identical; only the payloads are typed. Times are signed 64-bit
// Unix epoch nanoseconds; Cursor is opaque — pass it back verbatim.
type FeedMessage[K comparable, V any] struct {
	Cursor       Cursor
	MessageID    string
	ProducerID   string
	Sequence     int64
	EventTimeNs  int64
	IngestTimeNs int64
	Key          *K // nil when the record carries no key
	Value        V
	Metadata     map[string][]byte
}

// MetadataText decodes one Metadata entry as UTF-8 ("" when absent).
func (m FeedMessage[K, V]) MetadataText(name string) string {
	if m.Metadata == nil {
		return ""
	}
	return string(m.Metadata[name])
}

// FeedEvent is a typed record to append: request identity, key,
// payload, optional event time and Metadata. The bound codecs
// serialize Key/Value at write time — application code never touches
// bytes:
//
//	writer.AppendEvent(ctx, whitewater.FeedEvent[string, Order]{
//	    RequestID: rid, Key: &key, Value: order,
//	    Metadata:  whitewater.TextMetadata(map[string]string{"tenant": "acme"}),
//	})
type FeedEvent[K comparable, V any] struct {
	RequestID   string // empty generates a fresh identity
	Key         *K     // nil appends a keyless record
	Value       V
	EventTimeNs *int64
	Metadata    map[string][]byte
}

// TextMetadata encodes a text Metadata map to wire bytes.
func TextMetadata(metadata map[string]string) map[string][]byte {
	if metadata == nil {
		return nil
	}
	encoded := make(map[string][]byte, len(metadata))
	for name, value := range metadata {
		encoded[name] = []byte(value)
	}
	return encoded
}

// FeedPage is a decoded temporary-fetch page: messages plus the opaque
// continuation frontier.
type FeedPage[K comparable, V any] struct {
	Records    []FeedMessage[K, V]
	NextCursor *Cursor
}

// FeedReaderPage is one bounded page of committed records, decoded.
// Acknowledge DeliveredCursor — not the last record's cursor — to
// durably commit progress.
type FeedReaderPage[K comparable, V any] struct {
	Records             []FeedMessage[K, V]
	DeliveredCursor     *Cursor
	AcknowledgedCursor  *Cursor
	RecommendedCapacity int
	RetryAfterMs        int
}

// FeedWriter is a session-bound Writer with the feed's codecs
// applied — the session epoch and request identities stay inside the
// handle:
//
//	writer.Append(ctx, "order-1", order)
//	writer.AppendEvent(ctx, FeedEvent{Key: &k, Value: o, RequestID: rid})
type FeedWriter[K comparable, V any] struct {
	session    *WriterSessionClient
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// SessionEpoch is the session's fencing epoch.
func (w *FeedWriter[K, V]) SessionEpoch() int64 { return w.session.sessionEpoch }

// UseCompression enables wire compression on subsequent appends.
func (w *FeedWriter[K, V]) UseCompression(enabled bool) *FeedWriter[K, V] {
	return &FeedWriter[K, V]{
		session:  w.session.UseCompression(enabled),
		keyCodec: w.keyCodec, valueCodec: w.valueCodec,
	}
}

// Append encodes one record through the bound codecs; request identity
// and event time auto-generate. For a keyless record or pinned
// identity use AppendEvent.
func (w *FeedWriter[K, V]) Append(ctx context.Context, key K, value V) (AppendResult, error) {
	return w.AppendEvent(ctx, FeedEvent[K, V]{Key: &key, Value: value})
}

// AppendEvent appends one typed event — pin RequestID for
// caller-driven retries (identical retries dedupe server-side).
func (w *FeedWriter[K, V]) AppendEvent(
	ctx context.Context, event FeedEvent[K, V],
) (AppendResult, error) {
	input, err := w.wire(event)
	if err != nil {
		return AppendResult{}, err
	}
	return w.session.Append(ctx, input)
}

// AppendAll issues one epoch-fenced batch; per-record results plus
// server batching guidance.
func (w *FeedWriter[K, V]) AppendAll(
	ctx context.Context, events []FeedEvent[K, V],
) (AppendBatchResult, error) {
	inputs := make([]AppendInput, 0, len(events))
	for _, event := range events {
		input, err := w.wire(event)
		if err != nil {
			return AppendBatchResult{}, err
		}
		inputs = append(inputs, input)
	}
	return w.session.AppendBatch(ctx, inputs)
}

func (w *FeedWriter[K, V]) wire(event FeedEvent[K, V]) (AppendInput, error) {
	key, err := encodeOrNil(w.keyCodec, event.Key)
	if err != nil {
		return AppendInput{}, fmt.Errorf("encode key: %w", err)
	}
	payload, err := w.valueCodec.Encode(event.Value)
	if err != nil {
		return AppendInput{}, fmt.Errorf("encode payload: %w", err)
	}
	return AppendInput{
		RequestID: event.RequestID, Key: key, Payload: payload,
		EventTimeNs: event.EventTimeNs, Metadata: event.Metadata,
	}, nil
}

// FeedReader is a session-bound named Reader with the feed's codecs
// applied — Fetch returns a bounded page of decoded messages; Ack
// commits the page's delivered Cursor durably:
//
//	reader, _ := orders.Reader(ctx, "audit", 64)
//	defer reader.Close(ctx)
//	page, _ := reader.Fetch(ctx, 0)
//	for _, m := range page.Records { handle(m.Value) }
//	reader.Ack(ctx)
type FeedReader[K comparable, V any] struct {
	client     *ReaderSessionClient
	session    ReaderSession
	keyCodec   Codec[K]
	valueCodec Codec[V]
	lastPage   *FeedReaderPage[K, V]
}

// SessionEpoch is the session's fencing epoch.
func (r *FeedReader[K, V]) SessionEpoch() int64 { return r.session.SessionEpoch }

// Fetch returns up to limit committed records after the frontier,
// decoded through the bound codecs.
func (r *FeedReader[K, V]) Fetch(ctx context.Context, limit int) (FeedReaderPage[K, V], error) {
	page, err := r.client.Fetch(ctx, r.session, limit)
	if err != nil {
		return FeedReaderPage[K, V]{}, err
	}
	decoded, err := decodeFeedReaderPage[K, V](page, r.keyCodec, r.valueCodec)
	if err != nil {
		return FeedReaderPage[K, V]{}, err
	}
	r.lastPage = &decoded
	return decoded, nil
}

// Ack durably commits the last fetched page's delivered Cursor. Call
// after processing a page; anything unacknowledged may replay.
func (r *FeedReader[K, V]) Ack(ctx context.Context) (ReaderSession, error) {
	if r.lastPage == nil || r.lastPage.DeliveredCursor == nil {
		return r.session, nil
	}
	return r.AckCursor(ctx, *r.lastPage.DeliveredCursor)
}

// AckCursor durably commits an explicit Cursor.
func (r *FeedReader[K, V]) AckCursor(ctx context.Context, cursor Cursor) (ReaderSession, error) {
	session, err := r.client.Ack(ctx, r.session, cursor)
	if err != nil {
		return session, err
	}
	r.session = session
	return session, nil
}

// ForEach delivers every record to handler — one at a time — acking
// each fetched page only after all its records have been handled. The
// pump paces empty fetches on the server's retry_after_ms and returns
// ctx.Err() on cancellation. A handler returning false or an error
// exits WITHOUT acking the in-flight page so its tail replays —
// at-least-once delivery, never acknowledge undelivered records.
func (r *FeedReader[K, V]) ForEach(
	ctx context.Context, handler func(FeedMessage[K, V]) (bool, error),
) error {
	for {
		page, err := r.Fetch(ctx, 0)
		if err != nil {
			return err
		}
		if len(page.Records) == 0 {
			wait := page.RetryAfterMs
			if wait < 1 {
				wait = 1
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(wait) * time.Millisecond):
			}
			continue
		}
		for _, message := range page.Records {
			keep, err := handler(message)
			if err != nil || !keep {
				return err
			}
		}
		if _, err := r.Ack(ctx); err != nil {
			return err
		}
	}
}

// Close releases the session fence; durable acknowledged progress
// survives.
func (r *FeedReader[K, V]) Close(ctx context.Context) error {
	if r.session.SessionEpoch == 0 {
		return nil
	}
	_, err := r.client.Close(ctx, r.session)
	r.session.SessionEpoch = 0
	return err
}

// Feed is a typed facade over one Feed — the immutable temporal
// history on the Nodes. The bound codecs serialize on write and
// deserialize on read, so application code stays in its own types:
//
//	orders, _ := whitewater.FeedOf(client, "orders.events",
//	    whitewater.StringCodec(), whitewater.JSONCodec[Order]())
//	writer, _ := orders.Writer(ctx, "ingest")
//	writer.Append(ctx, "order-1", Order{ID: "order-1", Total: 42})
type Feed[K comparable, V any] struct {
	client     *Client
	feedClient *FeedClient
	name       string
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// FeedOf binds a Feed under explicit codecs. name must be lowercase
// dotted segments.
func FeedOf[K comparable, V any](
	client *Client,
	name string,
	keyCodec Codec[K],
	valueCodec Codec[V],
) (*Feed[K, V], error) {
	dotted, err := client.nameFor(name, true)
	if err != nil {
		return nil, err
	}
	return &Feed[K, V]{
		client: client, feedClient: client.Feeds, name: dotted,
		keyCodec: keyCodec, valueCodec: valueCodec,
	}, nil
}

// FeedFor binds a Feed with string keys and a CodecFor[V] payload
// (JSON for object types) — the common case:
//
//	orders, _ := whitewater.FeedFor[Order](client, "orders.events")
func FeedFor[V any](client *Client, name string) (*Feed[string, V], error) {
	return FeedOf(client, name, StringCodec(), CodecFor[V]())
}

// Feed binds the default facade: string keys + raw byte payloads —
// the escape hatch to wire bytes.
func (c *Client) Feed(name string) (*Feed[string, string], error) {
	return FeedOf(c, name, StringCodec(), StringCodec())
}

// Name is the Feed's dotted name.
func (f *Feed[K, V]) Name() string { return f.name }

// UseCompression enables wire compression on fetches and ad-hoc
// appends through this facade.
func (f *Feed[K, V]) UseCompression(enabled bool) *Feed[K, V] {
	copy := *f
	copy.feedClient = f.feedClient.UseCompression(enabled)
	return &copy
}

// RebindFeed rebinds the same feed under different codecs — no wire
// call: RebindFeed(orders, StringCodec(), BytesCodec()) reads raw.
func RebindFeed[K comparable, V any, K2 comparable, V2 any](
	f *Feed[K, V], keyCodec Codec[K2], valueCodec Codec[V2],
) *Feed[K2, V2] {
	return &Feed[K2, V2]{
		client: f.client, feedClient: f.feedClient, name: f.name,
		keyCodec: keyCodec, valueCodec: valueCodec,
	}
}

// Records returns committed records from the beginning or after an
// opaque after Cursor, decoded through the bound codecs.
func (f *Feed[K, V]) Records(
	ctx context.Context, limit int, after *Cursor,
) ([]FeedMessage[K, V], error) {
	records, err := f.feedClient.Records(ctx, f.name, limit, after)
	if err != nil {
		return nil, err
	}
	return decodeFeedMessages[K, V](records, f.keyCodec, f.valueCodec)
}

// Fetch is an anonymous temporary Reader fetch: decoded records plus
// the opaque continuation frontier. Retains no server-side progress.
func (f *Feed[K, V]) Fetch(
	ctx context.Context, options TemporaryFetchOptions,
) (FeedPage[K, V], error) {
	page, err := f.feedClient.TemporaryFetch(ctx, f.name, options)
	if err != nil {
		return FeedPage[K, V]{}, err
	}
	messages, err := decodeFeedMessages[K, V](page.Records, f.keyCodec, f.valueCodec)
	if err != nil {
		return FeedPage[K, V]{}, err
	}
	return FeedPage[K, V]{Records: messages, NextCursor: page.NextCursor}, nil
}

// Load bulk-reads committed history once, from start, delivering each
// decoded page to sink until the frontier is drained.
func (f *Feed[K, V]) Load(
	ctx context.Context,
	start ReaderStart,
	sink func([]FeedMessage[K, V]) error,
) (int64, error) {
	return f.feedClient.Load(ctx, f.name, start, func(records []Record) error {
		messages, err := decodeFeedMessages[K, V](records, f.keyCodec, f.valueCodec)
		if err != nil {
			return err
		}
		return sink(messages)
	})
}

// Writer returns a session-bound Writer on this Feed: declares the
// Writer when missing (reusing an existing one), opens a session, and
// binds the feed's codecs so writer.Append(key, value) just works.
func (f *Feed[K, V]) Writer(ctx context.Context, writerName string) (*FeedWriter[K, V], error) {
	if _, err := f.client.CreateWriter(ctx, writerName, f.name); err != nil {
		if !strings.Contains(err.Error(), "exists") {
			return nil, err
		}
	}
	epoch, err := f.openWriterSession(ctx, writerName)
	if err != nil {
		return nil, err
	}
	return &FeedWriter[K, V]{
		session:  f.client.Writer(writerName, epoch),
		keyCodec: f.keyCodec, valueCodec: f.valueCodec,
	}, nil
}

// Append is an ad-hoc single append with a caller-owned writer
// identity — codecs serialize key/payload. For durable named
// producers prefer Writer.
func (f *Feed[K, V]) Append(
	ctx context.Context,
	writerSessionID string,
	writerEpoch int64,
	sequence int64,
	key K,
	value V,
) (AppendResult, error) {
	keyBytes, err := f.keyCodec.Encode(key)
	if err != nil {
		return AppendResult{}, fmt.Errorf("encode key: %w", err)
	}
	payload, err := f.valueCodec.Encode(value)
	if err != nil {
		return AppendResult{}, fmt.Errorf("encode payload: %w", err)
	}
	return f.feedClient.Append(ctx, f.name, FeedAppend{
		RequestID: newUUID(), WriterSessionID: writerSessionID,
		WriterEpoch: writerEpoch, Sequence: sequence,
		Key: keyBytes, Payload: payload,
	})
}

// Reader returns a session-bound named Reader on this Feed: declares
// the Reader (at BEGINNING) when missing, opens a session with the
// bounded fetch capacity, and returns decoded messages.
func (f *Feed[K, V]) Reader(
	ctx context.Context, readerName string, capacity int,
) (*FeedReader[K, V], error) {
	if err := f.client.ensureReader(ctx, readerName, f.name, StartBeginning()); err != nil {
		return nil, err
	}
	sessionClient := f.client.Reader(readerName)
	session, err := sessionClient.Open(ctx, capacity)
	if err != nil {
		return nil, err
	}
	return &FeedReader[K, V]{
		client: sessionClient, session: session,
		keyCodec: f.keyCodec, valueCodec: f.valueCodec,
	}, nil
}

// Subscribe is a running subscription on this Feed: a named Reader
// polled in the background, delivering decoded batches and
// acknowledging durable progress after each.
func (f *Feed[K, V]) Subscribe(
	ctx context.Context,
	reader string,
	start ReaderStart,
	onRecords func([]FeedMessage[K, V]) error,
	onError func(error),
) *Subscription {
	return f.subscribeDecoded(
		ctx, reader, start, subscribeCapacity, onRecords, onError)
}

func (f *Feed[K, V]) subscribeDecoded(
	ctx context.Context,
	reader string,
	start ReaderStart,
	capacity int,
	onRecords func([]FeedMessage[K, V]) error,
	onError func(error),
) *Subscription {
	return f.client.subscribe(ctx, reader, f.name, start, capacity,
		func(records []Record) error {
			messages, err := decodeFeedMessages[K, V](records, f.keyCodec, f.valueCodec)
			if err != nil {
				return err
			}
			return onRecords(messages)
		},
		onError)
}

// Consumer binds a permanent, supervised consumer to this Feed's named
// Reader — the durable-Reader equivalent of Kafka's poll loop with the
// plumbing removed. Iterate a channel of decoded records or run a
// per-record handler; open/fetch/ack/reopen/backoff stay internal:
//
//	consumer := feed.Consumer("enricher")
//	for msg := range consumer.Messages(ctx) {
//	    process(msg)
//	}
//	if err := consumer.Err(); err != nil { ... }
//
// See FeedConsumer for the full contract.
func (f *Feed[K, V]) Consumer(reader string) *FeedConsumer[K, V] {
	return newFeedConsumer(f, reader)
}

func (f *Feed[K, V]) openWriterSession(ctx context.Context, writerName string) (int64, error) {
	execution, err := f.client.OpenWriterSession(ctx, writerName)
	if err != nil {
		return 0, err
	}
	if len(execution.Results) == 0 {
		return 0, fmt.Errorf("open_writer_session returned no results for %s", writerName)
	}
	var data struct {
		SessionEpoch int64 `json:"session_epoch"`
	}
	if err := json.Unmarshal(execution.Results[0].Data, &data); err != nil || data.SessionEpoch == 0 {
		return 0, fmt.Errorf("open_writer_session returned no session_epoch for %s", writerName)
	}
	return data.SessionEpoch, nil
}

func decodeFeedMessages[K comparable, V any](
	records []Record, keyCodec Codec[K], valueCodec Codec[V],
) ([]FeedMessage[K, V], error) {
	messages := make([]FeedMessage[K, V], 0, len(records))
	for _, record := range records {
		key, err := decodeOrNil(keyCodec, record.Key)
		if err != nil {
			return nil, err
		}
		value, err := valueCodec.Decode(record.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
		messages = append(messages, FeedMessage[K, V]{
			Cursor: record.Cursor, MessageID: record.MessageID,
			ProducerID: record.ProducerID, Sequence: record.Sequence,
			EventTimeNs: record.EventTimeNs, IngestTimeNs: record.IngestTimeNs,
			Key: key, Value: value, Metadata: record.Metadata,
		})
	}
	return messages, nil
}

func decodeFeedReaderPage[K comparable, V any](
	page ReaderPage, keyCodec Codec[K], valueCodec Codec[V],
) (FeedReaderPage[K, V], error) {
	messages, err := decodeFeedMessages[K, V](page.Records, keyCodec, valueCodec)
	if err != nil {
		return FeedReaderPage[K, V]{}, err
	}
	return FeedReaderPage[K, V]{
		Records: messages, DeliveredCursor: page.DeliveredCursor,
		AcknowledgedCursor:  page.AcknowledgedCursor,
		RecommendedCapacity: page.RecommendedCapacity,
		RetryAfterMs:        page.RetryAfterMs,
	}, nil
}
