package whitewater

import (
	"context"
	"encoding/base64"
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

// MaxNamedKeys bounds the named secondary keys a RecordStore declares;
// it matches the server Index field budget.
const MaxNamedKeys = 10

// StoreEntry is a stored row: its primary key and the record value.
type StoreEntry struct {
	PrimaryKey []byte
	Record     Record
}

// RecordStore is a named, thread-safe in-memory store of Feed records —
// the target of FeedClient.Load and a typical sink for Client.Subscribe:
//
//	store := whitewater.NewRecordStore("employee-store").
//	    AddKey("passport").AddKey("department")
//	_, _ = client.LoadStore(store).FromFeed(ctx, "hr.employees",
//	    whitewater.StartBeginning(),
//	    func(s *whitewater.RecordStore, r whitewater.Record) {
//	        s.Upsert(r).WithKey(r.Key).WithNamedKey("passport", r.Metadata["pp"])
//	    })
//	sub := client.Subscribe(ctx, "hr-view", "hr.employees",
//	    whitewater.StartNow(), store.PutAll, nil)
//	defer sub.Close()
//
// The primary map keys rows on the key chosen by Upsert.WithKey (the
// record's own Key via Put/PutAll, the Cursor for unkeyed records). Each
// declared named key maintains a separate postings map — conceptually
// employee-store.passport — of value -> primary keys, so named lookups
// are not unique and return every matching row.
type RecordStore struct {
	name string
	mu   sync.RWMutex
	rows map[string]Record
	// declared named keys (bounded); namedIndex[key][valueID] = primary ids.
	declaredKeys []string
	namedIndex   map[string]map[string]map[string]struct{}
}

// NewRecordStore returns an empty named store.
func NewRecordStore(name string) *RecordStore {
	return &RecordStore{
		name:       name,
		rows:       map[string]Record{},
		namedIndex: map[string]map[string]map[string]struct{}{},
	}
}

// AddKey declares a named secondary key (at most MaxNamedKeys) and
// returns the store for chaining.
func (s *RecordStore) AddKey(namedKey string) *RecordStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if namedKey == "" {
		panic("named key is required")
	}
	if _, declared := s.namedIndex[namedKey]; !declared {
		if len(s.declaredKeys) >= MaxNamedKeys {
			panic("a RecordStore supports at most 10 named keys")
		}
		s.declaredKeys = append(s.declaredKeys, namedKey)
		s.namedIndex[namedKey] = map[string]map[string]struct{}{}
	}
	return s
}

// NamedKeys returns the declared named keys in declaration order.
func (s *RecordStore) NamedKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.declaredKeys...)
}

// Name returns the store's name.
func (s *RecordStore) Name() string { return s.name }

// Upsert starts a row write: choose the primary key and named-key values.
func (s *RecordStore) Upsert(record Record) *Upsert {
	return &Upsert{store: s, record: record, primaryID: recordStoreKey(record), named: map[string][]byte{}}
}

// Put inserts or replaces the row for the record's key.
func (s *RecordStore) Put(record Record) {
	s.commitUpsert(record, recordStoreKey(record), nil)
}

// PutAll inserts every record — shaped for use as a subscribe/load sink.
func (s *RecordStore) PutAll(records []Record) error {
	for _, record := range records {
		s.Put(record)
	}
	return nil
}

// Get returns the latest record for a key, when one has been loaded.
func (s *RecordStore) Get(key []byte) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.rows[base64.StdEncoding.EncodeToString(key)]
	return record, ok
}

// GetString is Get for a UTF-8 primary key.
func (s *RecordStore) GetString(key string) (Record, bool) {
	return s.Get([]byte(key))
}

// GetBy returns every row whose named key equals value — named keys are
// not unique. An undeclared named key returns nil.
func (s *RecordStore) GetBy(namedKey string, value []byte) []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	postings, declared := s.namedIndex[namedKey]
	if !declared {
		return nil
	}
	var rows []Record
	for id := range postings[base64.StdEncoding.EncodeToString(value)] {
		if record, ok := s.rows[id]; ok {
			rows = append(rows, record)
		}
	}
	return rows
}

// GetByString is GetBy for a UTF-8 named-key value.
func (s *RecordStore) GetByString(namedKey, value string) []Record {
	return s.GetBy(namedKey, []byte(value))
}

// Delete removes the row for a key plus its named-key postings.
func (s *RecordStore) Delete(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := base64.StdEncoding.EncodeToString(key)
	if _, ok := s.rows[id]; !ok {
		return false
	}
	delete(s.rows, id)
	s.removePostings(id)
	return true
}

// Clear empties the primary map and every named-key index.
func (s *RecordStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = map[string]Record{}
	for key := range s.namedIndex {
		s.namedIndex[key] = map[string]map[string]struct{}{}
	}
}

// Copy writes every row into target, re-attaching named-key postings;
// missing named keys are declared on the target.
func (s *RecordStore) Copy(target *RecordStore) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, key := range s.declaredKeys {
		target.AddKey(key)
	}
	for id, record := range s.rows {
		target.commitUpsert(record, id, s.namedValuesOf(id))
	}
}

// ForEach applies fn to a stable snapshot of every row.
func (s *RecordStore) ForEach(fn func(StoreEntry)) {
	for _, entry := range s.entries() {
		fn(entry)
	}
}

// ForEachParallel applies fn to every row spread across workers
// goroutines — real parallelism for CPU-bound projections.
func (s *RecordStore) ForEachParallel(workers int, fn func(StoreEntry)) {
	if workers < 1 {
		panic("workers must be >= 1")
	}
	entries := s.entries()
	if len(entries) == 0 {
		return
	}
	if workers > len(entries) {
		workers = len(entries)
	}
	jobs := make(chan StoreEntry, len(entries))
	for _, entry := range entries {
		jobs <- entry
	}
	close(jobs)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for entry := range jobs {
				fn(entry)
			}
		}()
	}
	wg.Wait()
}

// Entries returns a stable snapshot of every stored row.
func (s *RecordStore) Entries() []StoreEntry { return s.entries() }

// Len reports the row count.
func (s *RecordStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

func (s *RecordStore) entries() []StoreEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]StoreEntry, 0, len(s.rows))
	for id, record := range s.rows {
		key, _ := base64.StdEncoding.DecodeString(id)
		entries = append(entries, StoreEntry{PrimaryKey: key, Record: record})
	}
	return entries
}

// Upsert is the row-level write builder returned by RecordStore.Upsert.
type Upsert struct {
	store     *RecordStore
	record    Record
	primaryID string
	named     map[string][]byte
}

// WithKey chooses the row's primary key.
func (u *Upsert) WithKey(key []byte) *Upsert {
	id := base64.StdEncoding.EncodeToString(key)
	if id != u.primaryID {
		u.store.mu.Lock()
		delete(u.store.rows, u.primaryID)
		u.store.removePostings(u.primaryID)
		u.store.mu.Unlock()
		u.primaryID = id
	}
	u.store.commitUpsert(u.record, u.primaryID, u.named)
	return u
}

// WithNamedKey adds a named-key value; it must have been declared via
// RecordStore.AddKey.
func (u *Upsert) WithNamedKey(namedKey string, value []byte) *Upsert {
	u.named[namedKey] = append([]byte(nil), value...)
	u.store.commitUpsert(u.record, u.primaryID, u.named)
	return u
}

func (s *RecordStore) commitUpsert(record Record, primaryID string, named map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removePostings(primaryID)
	for key, value := range named {
		postings, declared := s.namedIndex[key]
		if !declared {
			continue
		}
		valueID := base64.StdEncoding.EncodeToString(value)
		ids, ok := postings[valueID]
		if !ok {
			ids = map[string]struct{}{}
			postings[valueID] = ids
		}
		ids[primaryID] = struct{}{}
	}
	s.rows[primaryID] = record
}

func (s *RecordStore) namedValuesOf(primaryID string) map[string][]byte {
	named := map[string][]byte{}
	for key, postings := range s.namedIndex {
		for valueID, ids := range postings {
			if _, ok := ids[primaryID]; ok {
				value, _ := base64.StdEncoding.DecodeString(valueID)
				named[key] = value
			}
		}
	}
	return named
}

// removePostings strips every named-key posting for primaryID. Callers
// hold s.mu.
func (s *RecordStore) removePostings(primaryID string) {
	for _, postings := range s.namedIndex {
		for valueID, ids := range postings {
			delete(ids, primaryID)
			if len(ids) == 0 {
				delete(postings, valueID)
			}
		}
	}
}

func recordStoreKey(record Record) string {
	if record.Key != nil {
		return base64.StdEncoding.EncodeToString(record.Key)
	}
	return base64.StdEncoding.EncodeToString([]byte("cursor:" + string(record.Cursor)))
}

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

// LoadInto bulk-reads committed Feed history into store.
func (f *FeedClient) LoadInto(
	ctx context.Context,
	feed string,
	start ReaderStart,
	store *RecordStore,
) (int64, error) {
	return f.Load(ctx, feed, start, store.PutAll)
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

// LoadStore bulk-materializes Feed history into store — returned by
// Client.LoadStore; the FromFeed projection picks each row's primary key
// and named-key values via store.Upsert.
type LoadStore struct {
	client *Client
	store  *RecordStore
}

// LoadStore returns the bulk materialization operation for store.
func (c *Client) LoadStore(store *RecordStore) *LoadStore {
	return &LoadStore{client: c, store: store}
}

// FromFeed reads committed history of feed from start, invoking
// projection per record until the frontier is drained; returns the
// number of records loaded.
//
//	_, err := client.LoadStore(store).FromFeed(ctx, "hr.employees",
//	    whitewater.StartBeginning(),
//	    func(s *whitewater.RecordStore, r whitewater.Record) {
//	        s.Upsert(r).WithKey(r.Key).WithNamedKey("department", []byte("eng"))
//	    })
func (l *LoadStore) FromFeed(
	ctx context.Context,
	feed string,
	start ReaderStart,
	projection func(*RecordStore, Record),
) (int64, error) {
	return l.client.Feeds.Load(ctx, feed, start, func(records []Record) error {
		for _, record := range records {
			projection(l.store, record)
		}
		return nil
	})
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
