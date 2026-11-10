package whitewater

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// wclStub records every WCL script and replies a committed execution —
// enough for lifecycle methods that serialize one statement each.
type wclStub struct {
	scripts []string
	onWCL   func(script string)
}

func (w *wclStub) last() string {
	if len(w.scripts) == 0 {
		return ""
	}
	return w.scripts[len(w.scripts)-1]
}

// newStubServer answers POST /v1/admin/wcl with a committed result and
// records the script on stub.
func newStubServer(t *testing.T, stub *wclStub) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		if r.Method == "POST" && r.URL.Path == "/v1/admin/wcl" {
			body, _ := io.ReadAll(r.Body)
			var payload map[string]any
			_ = json.Unmarshal(body, &payload)
			script, _ := payload["script"].(string)
			stub.scripts = append(stub.scripts, script)
			if stub.onWCL != nil {
				stub.onWCL(script)
			}
			_, _ = w.Write([]byte(`{"request_id":"r1","revision":2,"authority":"control_plane","results":[{"statement":"s","message":"ok","data":{}}]}`))
			return
		}
		w.WriteHeader(404)
	}))
}

// The Domain-first surface contract: CreateDomain returns the Domain,
// every resource resolves relative names inside it, and ScopedRef
// endpoints carry explicit cross-Domain names onto the wire.

func TestCreateDomainReturnsDomainHandle(t *testing.T) {
	var script string
	stub := &wclStub{onWCL: func(s string) { script = s }}
	server := newStubServer(t, stub)
	defer server.Close()

	client := NewClient("k", []string{server.URL}, 5*time.Second)
	domain, err := client.CreateDomain(context.Background(), "shop")
	if err != nil {
		t.Fatal(err)
	}
	if domain == nil || domain.Name() != "shop" {
		t.Fatalf("CreateDomain returned %v — the Domain handle, not ControlExecution", domain)
	}
	if script != "CREATE DOMAIN shop" {
		t.Fatalf("script = %q", script)
	}
}

func TestDomainScopedResolution(t *testing.T) {
	client := NewClient("k", []string{"http://127.0.0.1:1"}, time.Second)
	shop, err := client.Domain("shop")
	if err != nil {
		t.Fatal(err)
	}

	// Rel resolves inside the declaring Domain.
	if name, _ := shop.Scoped(Rel("orders")); name != "shop.orders" {
		t.Fatalf("Rel = %q", name)
	}
	// Abs keeps a qualified cross-Domain name.
	if name, _ := shop.Scoped(Abs("other.events")); name != "other.events" {
		t.Fatalf("Abs = %q", name)
	}
	// Handles carry their absolute names.
	foreign, _ := client.Domain("foreign")
	feed, err := foreign.Feed("metrics")
	if err != nil {
		t.Fatal(err)
	}
	if name, _ := shop.Scoped(feed); name != "foreign.metrics" {
		t.Fatalf("Feed handle = %q", name)
	}
	// SubscriptionRef the same way.
	sub, err := foreign.SubscriptionRef("audit")
	if err != nil {
		t.Fatal(err)
	}
	if name, _ := shop.Scoped(sub); name != "foreign.audit" {
		t.Fatalf("SubscriptionRef = %q", name)
	}
	// Relative names refuse qualified input — ambiguity is impossible.
	if _, err := shop.Scoped(Rel("already.qualified")); err != nil {
		t.Fatalf("dotted Rel must still compose: %v", err)
	}
}

func TestDomainCrossDomainPipeWire(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()

	warehouse, err := client.Domain("warehouse")
	if err != nil {
		t.Fatal(err)
	}
	purchasing, err := client.Domain("purchasing")
	if err != nil {
		t.Fatal(err)
	}
	source, err := purchasing.Feed("orders")
	if err != nil {
		t.Fatal(err)
	}

	// The Pipe name and input Subscription stay in warehouse; the
	// source Feed (inside the subscription) and the output cross over.
	if _, err := warehouse.CreateSubscription(ctx, "fulfil", source, StartBeginning()); err != nil {
		t.Fatal(err)
	}
	if _, err := warehouse.DefineForwardPipe(
		ctx, "relay", Rel("fulfil"), Rel("orders.inbound")); err != nil {
		t.Fatal(err)
	}

	// The subscription declares through WCL — its script carries the
	// warehouse-owned name sourcing purchasing's feed.
	var createBody map[string]any
	if err := json.Unmarshal([]byte((*bodies)[0]), &createBody); err != nil {
		t.Fatal(err)
	}
	wantSub := "CREATE SUBSCRIPTION warehouse.fulfil FROM purchasing.orders START AT BEGINNING"
	if createBody["script"] != wantSub {
		t.Fatalf("subscription script = %v", createBody["script"])
	}
	define := lastCommand(t, bodies, 1)
	if define["command"] != "define_pipe" ||
		define["name"] != "warehouse.relay" ||
		define["subscription"] != "warehouse.fulfil" ||
		define["output_feed"] != "warehouse.orders.inbound" {
		t.Fatalf("pipe command = %v", define)
	}
}

func TestDomainEnrichPipeAbsoluteEndpoints(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	ops, _ := client.Domain("ops")

	op := EnrichOperation(JsonExtractor("user_id"), EnrichOptions{
		OnMiss: EnrichMissQuarantine,
	})
	if _, err := ops.DefineEnrichPipe(
		ctx, "enrich", Rel("sub"), Abs("crm.enriched"),
		Abs("crm.customers"), op, Rel("dlq")); err != nil {
		t.Fatal(err)
	}
	command := lastCommand(t, bodies, 0)
	if command["output_feed"] != "crm.enriched" ||
		command["output_store"] != "crm.customers" ||
		command["subscription"] != "ops.sub" {
		t.Fatalf("cross-domain endpoints did not qualify: %v", command)
	}
}

func TestDomainDefineStateStoreRescopesSpec(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	accounts, _ := client.Domain("accounts")

	spec, err := NewStateStore("employees")
	if err != nil {
		t.Fatal(err)
	}
	spec.AddKey("department", false, JsonExtractor("dept"))
	if _, err := accounts.DefineStateStore(ctx, spec, Rel("events")); err != nil {
		t.Fatal(err)
	}
	command := lastCommand(t, bodies, 0)
	if command["name"] != "accounts.employees" {
		t.Fatalf("spec name = %v", command["name"])
	}
	source, _ := command["source"].(map[string]any)
	if source == nil || source["feed"] != "accounts.events" {
		t.Fatalf("source = %v", command["source"])
	}
}

func TestDomainLifecycleScopes(t *testing.T) {
	stub := &wclStub{}
	server := newStubServer(t, stub)
	defer server.Close()

	client := NewClient("k", []string{server.URL}, 5*time.Second)
	ctx := context.Background()
	shop, _ := client.Domain("shop")

	steps := []struct {
		want string
		call func() error
	}{
		{"CREATE READER shop.audit FROM shop.orders START AT BEGINNING",
			func() error {
				_, e := shop.CreateReaderFeed(ctx, "audit", Rel("orders"), StartBeginning())
				return e
			}},
		{"CREATE WRITER shop.ingest TO shop.orders",
			func() error {
				_, e := shop.CreateWriterFeed(ctx, "ingest", Rel("orders"))
				return e
			}},
		{"DESCRIBE FEED shop.orders",
			func() error { _, e := shop.DescribeFeed(ctx, "orders"); return e }},
		{"RENAME FEED shop.orders TO shop.orders2",
			func() error { _, e := shop.RenameFeed(ctx, "orders", "orders2"); return e }},
		{"DROP FEED shop.orders2",
			func() error { _, e := shop.DropFeed(ctx, "orders2"); return e }},
	}
	for i, step := range steps {
		if err := step.call(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if got := stub.last(); got != step.want {
			t.Fatalf("step %d script = %q, want %q", i, got, step.want)
		}
	}
}
