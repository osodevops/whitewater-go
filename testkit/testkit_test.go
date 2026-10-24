package testkit_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
	"github.com/osodevops/whitewater-go/testkit"
)

// The suite drives the fake through the public client API exactly the
// way application tests do — NewClient against Endpoints, typed Feed
// and Store facades, and the byte-level facades for the dedup checks.

func newFeed(
	t *testing.T, client *whitewater.Client, name string,
) *whitewater.Feed[string, []byte] {
	t.Helper()
	if _, err := client.CreateFeed(context.Background(), name); err != nil {
		t.Fatalf("CreateFeed: %v", err)
	}
	feed, err := client.Feed(name)
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	return feed
}

// append -> fetch -> ack commits durable progress: a reopened session
// must not redeliver acknowledged records.
func TestAppendFetchAckNoRedelivery(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()
	feed := newFeed(t, client, "orders.events")

	writer, err := feed.Writer(ctx, "ingest")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := writer.Append(ctx, "order-1", []byte("created")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := writer.Append(ctx, "order-2", []byte("paid")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	reader, err := feed.Reader(ctx, "audit", 64)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	page, err := reader.Fetch(ctx, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(page.Records) != 2 || *page.Records[0].Key != "order-1" ||
		string(page.Records[1].Value) != "paid" {
		t.Fatalf("page = %+v", page.Records)
	}
	if page.DeliveredCursor == nil {
		t.Fatal("page carried no delivered cursor")
	}
	if _, err := reader.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := reader.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopened session: acknowledged work does not redeliver, but a
	// record appended afterwards arrives next.
	reopened, err := feed.Reader(ctx, "audit", 64)
	if err != nil {
		t.Fatalf("Reader reopen: %v", err)
	}
	defer reopened.Close(ctx)
	if _, err := writer.Append(ctx, "order-3", []byte("shipped")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	page, err = reopened.Fetch(ctx, 0)
	if err != nil {
		t.Fatalf("Fetch after reopen: %v", err)
	}
	if len(page.Records) != 1 || *page.Records[0].Key != "order-3" {
		t.Fatalf("reopened page = %+v, want only order-3", page.Records)
	}
}

// An unacknowledged page must redeliver after the session closes.
func TestUnackedCloseRedelivers(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()
	feed := newFeed(t, client, "orders.events")

	writer, err := feed.Writer(ctx, "ingest")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	for _, key := range []string{"order-1", "order-2"} {
		if _, err := writer.Append(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	reader, err := feed.Reader(ctx, "audit", 64)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	page, err := reader.Fetch(ctx, 0)
	if err != nil || len(page.Records) != 2 {
		t.Fatalf("first fetch = %+v err=%v", page.Records, err)
	}
	// No ack: close mid-page and reopen under a new session.
	if err := reader.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := feed.Reader(ctx, "audit", 64)
	if err != nil {
		t.Fatalf("Reader reopen: %v", err)
	}
	defer reopened.Close(ctx)
	page, err = reopened.Fetch(ctx, 0)
	if err != nil {
		t.Fatalf("Fetch after reopen: %v", err)
	}
	if len(page.Records) != 2 || *page.Records[0].Key != "order-1" {
		t.Fatalf("redelivered page = %+v, want the unacked records", page.Records)
	}
}

// An identical retry under the same request_id replays the original
// commit; a tampered retry conflicts.
func TestAppendDeduplicatesByRequestID(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := whitewater.NewClient("test-key", []string{fake.Endpoints()}, 5*time.Second)
	ctx := context.Background()
	feed := newFeed(t, client, "orders.events")

	writer, err := feed.Writer(ctx, "ingest")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	key := "order-1"
	event := whitewater.FeedEvent[string, []byte]{
		RequestID: "stable-request-1", Key: &key, Value: []byte("created"),
	}
	first, err := writer.AppendEvent(ctx, event)
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	retry, err := writer.AppendEvent(ctx, event)
	if err != nil {
		t.Fatalf("AppendEvent retry: %v", err)
	}
	if !retry.Deduplicated || retry.MessageID != first.MessageID ||
		retry.Cursor != first.Cursor {
		t.Fatalf("retry = %+v, want deduplicated replay of %+v", retry, first)
	}

	// Same request_id, different payload: the wire reports a conflict.
	event.Value = []byte("forged")
	if _, err := writer.AppendEvent(ctx, event); err == nil ||
		!whitewater.IsConflict(err) {
		t.Fatalf("tampered retry err = %v, want conflict", err)
	}

	// The feed itself still holds exactly the one committed record.
	records, err := feed.Records(ctx, 10, nil)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(records) != 1 || records[0].MessageID != first.MessageID {
		t.Fatalf("records = %+v", records)
	}
}

// A reopened Writer session fences the old epoch: appends on it get
// the stale_epoch shape the SDK maps to KindStaleSession.
func TestStaleWriterEpochRefused(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()
	feed := newFeed(t, client, "orders.events")

	stale, err := feed.Writer(ctx, "ingest")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := stale.Append(ctx, "order-1", []byte("v1")); err != nil {
		t.Fatalf("Append on live epoch: %v", err)
	}
	current, err := feed.Writer(ctx, "ingest") // reopens; bumps the epoch
	if err != nil {
		t.Fatalf("Writer reopen: %v", err)
	}
	if current.SessionEpoch() == stale.SessionEpoch() {
		t.Fatal("reopen must mint a new session epoch")
	}
	if _, err := stale.Append(ctx, "order-2", []byte("v2")); err == nil ||
		!whitewater.IsStaleSession(err) {
		t.Fatalf("stale append err = %v, want KindStaleSession", err)
	}
	if _, err := current.Append(ctx, "order-2", []byte("v2")); err != nil {
		t.Fatalf("Append on current epoch: %v", err)
	}
}

// The StateStore surface round-trips: mutate applies, lookup/get-by/
// iterate read it back, delete and clear empty it, and mutation_id
// dedups.
func TestStoreRoundTrip(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()

	spec, err := whitewater.NewStateStore("accounts.employees")
	if err != nil {
		t.Fatal(err)
	}
	spec.AddKey("department", false)
	if _, err := client.StateStores.Define(ctx, spec, ""); err != nil {
		t.Fatalf("Define: %v", err)
	}
	store, err := client.StateStore("accounts.employees")
	if err != nil {
		t.Fatalf("StateStore: %v", err)
	}

	put, err := store.Put(ctx, []byte("emp-1"), []byte("alice"),
		map[string]any{"department": "eng"}, "mutation-1")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Deduplicated {
		t.Fatal("first put deduplicated")
	}
	if _, err := store.Put(ctx, []byte("emp-2"), []byte("bob"),
		map[string]any{"department": "eng"}, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	retry, err := store.Put(ctx, []byte("emp-1"), []byte("alice"),
		map[string]any{"department": "eng"}, "mutation-1")
	if err != nil {
		t.Fatalf("Put retry: %v", err)
	}
	if !retry.Deduplicated || retry.Sequence != put.Sequence {
		t.Fatalf("retry = %+v, want deduplicated replay of %+v", retry, put)
	}

	row, err := store.Get(ctx, []byte("emp-1"), false, 0)
	if err != nil || row == nil || string(row.Payload) != "alice" {
		t.Fatalf("Get = %+v err=%v", row, err)
	}
	byDept, err := store.GetBy(ctx, "department", "eng", 0, false, 0)
	if err != nil || len(byDept.Rows) != 2 {
		t.Fatalf("GetBy = %+v err=%v", byDept, err)
	}
	var keys []string
	if err := store.Iterate(ctx, 100, func(row whitewater.StoreRow) error {
		keys = append(keys, string(row.Key))
		return nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if len(keys) != 2 || keys[0] != "emp-1" || keys[1] != "emp-2" {
		t.Fatalf("iterated keys = %v", keys)
	}

	if _, err := store.Delete(ctx, []byte("emp-2"), ""); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if row, err := store.Get(ctx, []byte("emp-2"), false, 0); err != nil || row != nil {
		t.Fatalf("Get after delete = %+v err=%v", row, err)
	}
	if _, err := store.Clear(ctx, "clear-1"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if err := store.Iterate(ctx, 100, func(whitewater.StoreRow) error {
		t.Fatal("row survived clear")
		return nil
	}); err != nil {
		t.Fatalf("Iterate after clear: %v", err)
	}
	if row, err := store.Get(ctx, []byte("emp-1"), false, 0); err != nil || row != nil {
		t.Fatalf("Get after clear = %+v err=%v", row, err)
	}
}

// Temporary fetch and the committed-records read see the same log the
// named Reader serves.
func TestFeedRecordsAndTemporaryFetch(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()
	feed := newFeed(t, client, "orders.events")

	writer, err := feed.Writer(ctx, "ingest")
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	for _, key := range []string{"order-1", "order-2", "order-3"} {
		if _, err := writer.Append(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	records, err := feed.Records(ctx, 10, nil)
	if err != nil || len(records) != 3 {
		t.Fatalf("Records = %+v err=%v", records, err)
	}
	page, err := feed.Fetch(ctx, whitewater.TemporaryFetchOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(page.Records) != 2 || page.NextCursor == nil {
		t.Fatalf("first page = %+v", page)
	}
	rest, err := feed.Fetch(ctx, whitewater.TemporaryFetchOptions{After: page.NextCursor})
	if err != nil {
		t.Fatalf("Fetch after cursor: %v", err)
	}
	if len(rest.Records) != 1 || *rest.Records[0].Key != "order-3" {
		t.Fatalf("continuation = %+v", rest.Records)
	}
}

// Health answers the compat check; unrouted paths answer the
// SDK-decodable 404 envelope (mapped to Refused).
func TestHealthAndUnknownRoute(t *testing.T) {
	fake := testkit.NewRiverbed(t)
	defer fake.Close()
	client := fake.Client()
	ctx := context.Background()

	ok, err := client.Healthy(ctx)
	if err != nil || !ok {
		t.Fatalf("Healthy = %v err=%v", ok, err)
	}

	// The fake does not model index queries: the 404 envelope still
	// decodes to a typed Refused through the public API.
	if _, err := client.QueryIndex(
		ctx, "phantom", whitewater.IndexExact("x"), 1, false); err == nil ||
		!whitewater.IsRefused(err) {
		t.Fatalf("QueryIndex err = %v, want Refused", err)
	}

	// The raw 404 body carries the protocol error envelope.
	response, err := http.Get(fake.Endpoints() + "/v1/definitely/not/a/route")
	if err != nil {
		t.Fatalf("GET unknown route: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.StatusCode)
	}
	if !strings.Contains(response.Header.Get("Content-Type"), "json") {
		t.Fatalf("content-type = %q", response.Header.Get("Content-Type"))
	}
}
