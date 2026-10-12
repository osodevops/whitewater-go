package whitewater

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// storeStub routes each StateStore endpoint to a scripted responder and
// records every call.
type storeStub struct {
	mu     sync.Mutex
	calls  []string
	bodies []map[string]any
	routes map[string]string
	pages  map[string][]string
}

func (s *storeStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.calls = append(s.calls, r.URL.Path)
		s.bodies = append(s.bodies, body)
		var response string
		if queued := s.pages[r.URL.Path]; len(queued) > 0 {
			response = queued[0]
			s.pages[r.URL.Path] = queued[1:]
		} else if fixed, ok := s.routes[r.URL.Path]; ok {
			response = fixed
		} else {
			response = `{}`
		}
		s.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(response))
	}
}

func (s *storeStub) bodiesFor(path string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for i, call := range s.calls {
		if call == path {
			out = append(out, s.bodies[i])
		}
	}
	return out
}

const commandsOK = `{
  "request_id": "00000000-0000-7000-8000-000000000006",
  "revision": 9,
  "authority": "control_plane",
  "results": [{"statement": "s", "message": "ok", "data": {}}]
}`

const mutationOK = `{
  "store_id": "00000000-0000-7000-8000-000000000007",
  "mutation_id": "00000000-0000-7000-8000-000000000008",
  "sequence": 3,
  "deduplicated": false
}`

func b64(data string) string {
	return base64.StdEncoding.EncodeToString([]byte(data))
}

func TestDefineEmitsNamedKeysAndBindsTheFacade(t *testing.T) {
	stub := &storeStub{routes: map[string]string{"/v1/admin/commands": commandsOK}}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	spec, err := NewStateStore("accounts.employees")
	if err != nil {
		t.Fatal(err)
	}
	spec.AddKey("passport", true).AddKey("department", false)
	store, err := client.StateStores.Define(context.Background(), spec, "")
	if err != nil {
		t.Fatal(err)
	}

	bodies := stub.bodiesFor("/v1/admin/commands")
	if len(bodies) != 1 {
		t.Fatalf("command calls = %d, want 1", len(bodies))
	}
	commands, _ := bodies[0]["commands"].([]any)
	command, _ := commands[0].(map[string]any)
	if command["command"] != "define_state_store" || command["name"] != "accounts.employees" {
		t.Fatalf("unexpected command: %v", command)
	}
	source, _ := command["source"].(map[string]any)
	if source["kind"] != "manual" {
		t.Fatalf("source = %v, want manual", source)
	}
	keys, _ := command["named_keys"].([]any)
	if len(keys) != 2 {
		t.Fatalf("named_keys = %v", keys)
	}
	passport, _ := keys[0].(map[string]any)
	if passport["name"] != "passport" || passport["unique"] != true {
		t.Fatalf("passport key = %v", passport)
	}
	// The defined spec came back bound.
	if _, err := store.bound(); err != nil {
		t.Fatal(err)
	}
}

func TestDefineFeedSourcedNamedKeysNeedExtractors(t *testing.T) {
	stub := &storeStub{}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	// A feed-sourced named key without a projection is a declaration bug.
	spec, _ := NewStateStore("accounts.s")
	spec.AddKey("k", false)
	if _, err := client.StateStores.Define(
		context.Background(), spec, "accounts.events"); err == nil ||
		!strings.Contains(err.Error(), "require an extractor") {
		t.Fatalf("expected extractor refusal, got %v", err)
	}
	// An extractor on a manual store is refused — Puts carry the values.
	manual, _ := NewStateStore("accounts.m")
	manual.AddKey("k", false, MetadataExtractor("pp"))
	if _, err := client.StateStores.Define(
		context.Background(), manual, ""); err == nil ||
		!strings.Contains(err.Error(), "manual store") {
		t.Fatalf("expected manual-store extractor refusal, got %v", err)
	}
}

func TestDefineFeedSourcedEmitsExtractors(t *testing.T) {
	stub := &storeStub{routes: map[string]string{"/v1/admin/commands": commandsOK}}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	spec, _ := NewStateStore("accounts.tagged")
	spec.AddKey("passport", true, MetadataExtractor("pp"))
	spec.AddKey("department", false, JsonExtractor("dept"))
	if _, err := client.StateStores.Define(
		context.Background(), spec, "accounts.events"); err != nil {
		t.Fatal(err)
	}
	bodies := stub.bodiesFor("/v1/admin/commands")
	if len(bodies) != 1 {
		t.Fatalf("command calls = %d, want 1", len(bodies))
	}
	command := bodies[0]["commands"].([]any)[0].(map[string]any)
	if command["source"].(map[string]any)["kind"] != "feed" {
		t.Fatalf("source = %v, want feed", command["source"])
	}
	named := command["named_keys"].([]any)
	passport := named[0].(map[string]any)["extractor"].(map[string]any)
	if passport["kind"] != "metadata" || passport["name"] != "pp" {
		t.Fatalf("passport extractor = %v", passport)
	}
	dept := named[1].(map[string]any)["extractor"].(map[string]any)
	if dept["kind"] != "projection" || dept["extractor"] != "json" ||
		dept["field"] != "dept" {
		t.Fatalf("department extractor = %v", dept)
	}
}

func TestPutGetByDeleteCarryWireShapes(t *testing.T) {
	stub := &storeStub{routes: map[string]string{
		"/v1/state-stores/mutate": mutationOK,
		"/v1/state-stores/lookup": `{
		  "found": true,
		  "payload_base64": "` + b64("profile-1") + `",
		  "applied_cursor": "00000000000000000003",
		  "applied_seq": 7, "committed_seq": 7, "behind": false,
		  "served_by": "storage-1"
		}`,
		"/v1/state-stores/get-by": `{
		  "rows": [
		    {"key_base64": "` + b64("user-1") + `", "payload_base64": "` + b64("p1") + `", "applied_cursor": "00000000000000000003"},
		    {"key_base64": "` + b64("user-2") + `", "payload_base64": "` + b64("p2") + `", "applied_cursor": "00000000000000000004"}
		  ],
		  "applied_seq": 7, "committed_seq": 7, "behind": false,
		  "served_by": "storage-1"
		}`,
	}}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	store, err := client.StateStore("accounts.employees")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	result, err := store.Put(ctx, []byte("user-1"), []byte("profile-1"),
		map[string]any{"passport": "P111", "department": "eng", "score": 42},
		"")
	if err != nil {
		t.Fatal(err)
	}
	if result.Sequence != 3 || result.Deduplicated {
		t.Fatalf("result = %+v", result)
	}
	mutates := stub.bodiesFor("/v1/state-stores/mutate")
	op, _ := mutates[0]["op"].(map[string]any)
	if op["kind"] != "put_row" {
		t.Fatalf("op = %v", op)
	}
	named, _ := op["named"].([]any)
	byName := map[string]any{}
	for _, item := range named {
		entry, _ := item.(map[string]any)
		byName[entry["name"].(string)] = entry["values"]
	}
	passport, _ := byName["passport"].([]any)
	p0, _ := passport[0].(map[string]any)
	if p0["kind"] != "text" || p0["value"] != "P111" {
		t.Fatalf("passport value = %v", p0)
	}
	score, _ := byName["score"].([]any)
	s0, _ := score[0].(map[string]any)
	if s0["kind"] != "i64" {
		t.Fatalf("score value = %v", s0)
	}

	found, err := store.Get(ctx, []byte("user-1"), true, 250)
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || string(found.Payload) != "profile-1" {
		t.Fatalf("row = %+v", found)
	}
	if found.Freshness == nil || found.Freshness.AppliedSeq != 7 {
		t.Fatalf("freshness = %+v", found.Freshness)
	}
	lookups := stub.bodiesFor("/v1/state-stores/lookup")
	if lookups[0]["strict"] != true || lookups[0]["wait_ms"] != float64(250) {
		t.Fatalf("lookup = %v", lookups[0])
	}

	rows, err := store.GetBy(ctx, "department", "eng", 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 2 || rows.Freshness.ServedBy != "storage-1" {
		t.Fatalf("rows = %+v", rows)
	}
	getBys := stub.bodiesFor("/v1/state-stores/get-by")
	if getBys[0]["named_key"] != "department" {
		t.Fatalf("get-by = %v", getBys[0])
	}

	if _, err := store.Delete(ctx, []byte("user-1"), ""); err != nil {
		t.Fatal(err)
	}
	op, _ = stub.bodiesFor("/v1/state-stores/mutate")[1]["op"].(map[string]any)
	if op["kind"] != "delete_row" {
		t.Fatalf("delete op = %v", op)
	}
}

func TestIteratePagesUntilExhaustedThenClearAndDrop(t *testing.T) {
	stub := &storeStub{
		pages: map[string][]string{
			"/v1/state-stores/iterate": {
				`{"rows": [{"key_base64": "` + b64("a") + `", "payload_base64": "` + b64("1") + `", "applied_seq": 9}],
				  "next_key_base64": "` + b64("a") + `", "pinned_seq": 9, "served_by": "storage-2"}`,
				`{"rows": [{"key_base64": "` + b64("b") + `", "payload_base64": "` + b64("2") + `", "applied_seq": 9}],
				  "next_key_base64": null, "pinned_seq": 9, "served_by": "storage-2"}`,
			},
		},
		routes: map[string]string{
			"/v1/state-stores/clear": mutationOK,
			"/v1/admin/commands":     commandsOK,
		},
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	store, _ := client.StateStore("accounts.employees")
	ctx := context.Background()

	var keys []string
	err := store.Iterate(ctx, 100, func(row StoreRow) error {
		keys = append(keys, string(row.Key))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("keys = %v", keys)
	}
	pages := stub.bodiesFor("/v1/state-stores/iterate")
	if _, has := pages[0]["after_key_base64"]; has {
		t.Fatal("first page must not carry after_key_base64")
	}
	if pages[1]["after_key_base64"] != b64("a") {
		t.Fatalf("continuation = %v", pages[1]["after_key_base64"])
	}

	if _, err := store.Clear(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	commands := stub.bodiesFor("/v1/admin/commands")
	drop, _ := commands[0]["commands"].([]any)
	entry, _ := drop[0].(map[string]any)
	if entry["command"] != "drop" || entry["kind"] != "state_store" {
		t.Fatalf("drop command = %v", entry)
	}
}
