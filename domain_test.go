package whitewater

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The strongly-typed Domain facade: materialized catalog views scoped
// by space_id, relative Feed resolution, and the fluent
// Reader/Writer builders.
func TestDomain(t *testing.T) {
	const shopID = "11111111-1111-1111-1111-111111111111"
	const otherID = "22222222-2222-2222-2222-222222222222"
	const ordersFeedID = "33333333-3333-3333-3333-333333333333"
	const returnsFeedID = "44444444-4444-4444-4444-444444444444"
	const metricsFeedID = "55555555-5555-5555-5555-555555555555"

	var commandsBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/admin/commands":
			commandsBody, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte(`{
			  "request_id": "00000000-0000-0000-0000-000000000000",
			  "revision": 9,
			  "authority": "control_plane",
			  "results": [
			    {"statement":"describe","message":"ok","data":{"space_id":"` + shopID + `","name":"shop","status":"active","created_at_ns":0,"history_policy":null}},
			    {"statement":"show","message":"ok","data":[
			      {"feed_id":"` + ordersFeedID + `","name":"shop.orders","space_id":"` + shopID + `","status":"active"},
			      {"feed_id":"` + returnsFeedID + `","name":"shop.returns","space_id":"` + shopID + `","status":"active"},
			      {"feed_id":"` + metricsFeedID + `","name":"other.metrics","space_id":"` + otherID + `","status":"active"}
			    ]},
			    {"statement":"show","message":"ok","data":[
			      {"reader_id":"r1","name":"shop.audit","feed_id":"` + ordersFeedID + `","status":"active"},
			      {"reader_id":"r2","name":"other.audit","feed_id":"` + metricsFeedID + `","status":"active"}
			    ]},
			    {"statement":"show","message":"ok","data":[
			      {"writer_id":"w1","name":"shop.ingest","feed_id":"` + ordersFeedID + `","status":"active"}
			    ]},
			    {"statement":"show","message":"ok","data":[
			      {"subscription_id":"s1","name":"shop.pipeline","space_id":"` + shopID + `","status":"active"},
			      {"subscription_id":"s2","name":"other.pipeline","space_id":"` + otherID + `","status":"active"}
			    ]},
			    {"statement":"show","message":"ok","data":[]},
			    {"statement":"show","message":"ok","data":[]},
			    {"statement":"show","message":"ok","data":[]}
			  ]
			}`))
		case r.Method == "POST" && r.URL.Path == "/v1/admin/wcl":
			_, _ = w.Write([]byte(`{"request_id":"00000000-0000-0000-0000-000000000000","revision":9,"authority":"control_plane","results":[{"statement":"x","message":"ok","data":{}}]}`))
		case r.Method == "POST" && r.URL.Path == "/v1/readers/open":
			_, _ = w.Write([]byte(`{"reader_id":"r9","feed_id":"` + ordersFeedID + `","session_epoch":7,"capacity":256,"delivered_cursor":null,"acknowledged_cursor":null}`))
		case r.Method == "POST" && r.URL.Path == "/v1/readers/fetch":
			_, _ = w.Write([]byte(`{"records":[],"delivered_cursor":null,"acknowledged_cursor":null,"recommended_capacity":256,"retry_after_ms":0}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	ctx := context.Background()

	domain, err := client.GetDomain(ctx, "shop")
	if err != nil {
		t.Fatal(err)
	}

	// One batched read: describe + 7 shows.
	var batch struct {
		Commands []map[string]any `json:"commands"`
	}
	if err := json.Unmarshal(commandsBody, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Commands) != 8 {
		t.Fatalf("snapshot batch = %d commands, want 8", len(batch.Commands))
	}
	if batch.Commands[0]["command"] != "describe" || batch.Commands[1]["command"] != "show" {
		t.Fatalf("unexpected batch shape: %v", batch.Commands)
	}

	feeds, err := domain.Feeds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(feeds, ",") != "shop.orders,shop.returns" {
		t.Fatalf("feeds = %v", feeds)
	}
	filtered, err := domain.FeedsMatching(ctx, "ord")
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0] != "shop.orders" {
		t.Fatalf("filtered = %v", filtered)
	}
	readers, _ := domain.Readers(ctx)
	if len(readers) != 1 || readers[0] != "shop.audit" {
		t.Fatalf("readers = %v", readers)
	}
	writers, _ := domain.Writers(ctx)
	if len(writers) != 1 || writers[0] != "shop.ingest" {
		t.Fatalf("writers = %v", writers)
	}
	subs, _ := domain.Subscriptions(ctx)
	if len(subs) != 1 || subs[0] != "shop.pipeline" {
		t.Fatalf("subscriptions = %v", subs)
	}
	if id, _ := domain.ID(ctx); id != shopID {
		t.Fatalf("domain id = %s", id)
	}

	// Relative Feed resolution — pure name composition, no I/O.
	feed, err := domain.Feed("orders")
	if err != nil {
		t.Fatal(err)
	}
	if feed.Name() != "shop.orders" {
		t.Fatalf("resolved = %s", feed.Name())
	}
	qualified, err := domain.Feed("shop.returns")
	if err != nil {
		t.Fatal(err)
	}
	if qualified.Name() != "shop.returns" {
		t.Fatalf("qualified = %s", qualified.Name())
	}
	if _, err := domain.Feed("BAD.NAME"); err == nil {
		t.Fatal("malformed name must fail locally")
	}

	// Fluent Reader builder — declares + opens + binds codecs.
	reader, err := NewReaderBuilder(client, domain, feed).
		ReaderID("shop.fulfillment").
		StartPoint(StartBeginning()).
		Capacity(256).
		Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reader.SessionEpoch() != 7 {
		t.Fatalf("epoch = %d", reader.SessionEpoch())
	}
	if _, err := reader.Fetch(ctx, 0); err != nil {
		t.Fatal(err)
	}

	// Cross-domain feeds refuse locally — the check lands at Open.
	other, err := client.Feed("other.metrics")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewReaderBuilder(client, domain, other).ReaderID("x").Open(ctx); err == nil {
		t.Fatal("a feed outside the Domain must refuse")
	}
	if _, err := NewWriterBuilder(client, domain, other).WriterID("x").Open(ctx); err == nil {
		t.Fatal("a feed outside the Domain must refuse")
	}

	// ReaderID is required.
	if _, err := NewReaderBuilder(client, domain, feed).Open(ctx); err == nil {
		t.Fatal("open without ReaderID must fail")
	}
	if _, err := NewWriterBuilder(client, domain, feed).Open(ctx); err == nil {
		t.Fatal("open without WriterID must fail")
	}
}
