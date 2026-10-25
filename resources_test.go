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

// The typed resource methods are WCL builders: each must serialize to
// exactly one statement on POST /v1/admin/wcl, and invalid names must
// fail locally before any HTTP call.
func TestWclBuilders(t *testing.T) {
	var capturedPath, capturedScript string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		capturedScript, _ = body["script"].(string)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"request_id":"00000000-0000-0000-0000-000000000000","results":[]}`))
	}))
	defer server.Close()

	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	ctx := context.Background()

	assertScript := func(want string) {
		t.Helper()
		if capturedPath != "/v1/admin/wcl" {
			t.Fatalf("path = %s, want /v1/admin/wcl", capturedPath)
		}
		if capturedScript != want {
			t.Fatalf("script = %q, want %q", capturedScript, want)
		}
	}

	if _, err := client.CreateDomain(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE DOMAIN orders")

	if _, err := client.CreateFeed(ctx, "orders.events"); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE FEED orders.events")

	if _, err := client.CreateWriter(ctx, "checkout", "orders.events"); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE WRITER checkout TO orders.events")

	if _, err := client.CreateReader(ctx, "audit", "orders.events", StartBeginning()); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE READER audit FROM orders.events START AT BEGINNING")

	if _, err := client.CreateReader(ctx, "live", "orders.events", StartNow()); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE READER live FROM orders.events START AT NOW")

	if _, err := client.CreateReader(
		ctx, "rewind", "orders.events", StartTimestampNs(1700000000000000000),
	); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE READER rewind FROM orders.events START AT TIMESTAMP 1700000000000000000")

	if _, err := client.CreateReader(
		ctx, "resumed", "orders.events", StartCursor(Cursor("abc'def")),
	); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE READER resumed FROM orders.events START AT CURSOR 'abc''def'")

	if _, err := client.CreateSubscription(ctx, "team", "orders.events", StartNow()); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE SUBSCRIPTION team FROM orders.events START AT NOW")

	if _, err := client.CreateSubscription(ctx, "orders.sub", "orders.events", StartBeginning()); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE SUBSCRIPTION orders.sub FROM orders.events START AT BEGINNING")

	if _, err := client.CreateRole(ctx, "ops"); err != nil {
		t.Fatal(err)
	}
	assertScript("CREATE ROLE ops")

	if _, err := client.OpenWriterSession(ctx, "checkout"); err != nil {
		t.Fatal(err)
	}
	assertScript("OPEN WRITER SESSION checkout")

	if _, err := client.SeekReader(ctx, "audit", StartNow()); err != nil {
		t.Fatal(err)
	}
	assertScript("SEEK READER audit TO NOW")

	if _, err := client.DescribeFeed(ctx, "orders.events"); err != nil {
		t.Fatal(err)
	}
	assertScript("DESCRIBE FEED orders.events")

	if _, err := client.DescribeSubscription(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	assertScript("DESCRIBE SUBSCRIPTION team")

	if _, err := client.RenameDomain(ctx, "orders", "billing"); err != nil {
		t.Fatal(err)
	}
	assertScript("RENAME DOMAIN orders TO billing")

	if _, err := client.RenameFeed(ctx, "orders.events", "orders.accepted"); err != nil {
		t.Fatal(err)
	}
	assertScript("RENAME FEED orders.events TO orders.accepted")

	if _, err := client.DropWriter(ctx, "checkout"); err != nil {
		t.Fatal(err)
	}
	assertScript("DROP WRITER checkout")

	if _, err := client.ListFeeds(ctx); err != nil {
		t.Fatal(err)
	}
	assertScript("SHOW FEEDS")

	if _, err := client.ListDomains(ctx); err != nil {
		t.Fatal(err)
	}
	assertScript("SHOW DOMAINS")
}

func TestWclBuildersRejectInvalidNamesLocally(t *testing.T) {
	client := NewClient("test-key", []string{"http://127.0.0.1:1"}, time.Second)
	ctx := context.Background()

	cases := []func() error{
		func() error { _, err := client.CreateFeed(ctx, "UPPER.case"); return err },
		func() error { _, err := client.CreateFeed(ctx, ".leading"); return err },
		func() error { _, err := client.CreateDomain(ctx, "two words"); return err },
		func() error { _, err := client.CreateWriter(ctx, "bad;name", "orders.events"); return err },
		func() error { _, err := client.CreateReader(ctx, "a'b", "orders.events", StartNow()); return err },
		func() error {
			_, err := client.CreateSubscription(ctx, "orders-sub", "orders.events", StartNow())
			return err
		},
		func() error { _, err := client.RenameDomain(ctx, "orders", "INJECTION; DROP"); return err },
	}
	for i, call := range cases {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "invalid") {
			t.Fatalf("case %d: expected local validation error, got %v", i, err)
		}
	}
}
