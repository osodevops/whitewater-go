package whitewater

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const sessionJSON = `{
  "reader_id": "00000000-0000-7000-8000-000000000004",
  "feed_id": "00000000-0000-7000-8000-000000000005",
  "session_epoch": 3,
  "capacity": 128,
  "delivered_cursor": null,
  "acknowledged_cursor": null
}`

const wclOK = `{
  "request_id": "00000000-0000-7000-8000-000000000006",
  "revision": 9,
  "authority": "control_plane",
  "results": [{"statement": "s", "message": "ok", "data": {}}]
}`

func recordJSON(cursor, key string) map[string]any {
	return map[string]any{
		"cursor":          cursor,
		"message_id":      "00000000-0000-7000-8000-000000000002",
		"producer_id":     "00000000-0000-7000-8000-000000000003",
		"sequence":        7,
		"event_time_ns":   "1697040000123456789",
		"ingest_time_ns":  "1697040000223456789",
		"key_base64":      key,
		"payload_base64":  "dmFs",
		"metadata_base64": map[string]string{},
	}
}

// subscribeStub routes each endpoint to a scripted responder and records
// every call.
type subscribeStub struct {
	mu             sync.Mutex
	calls          []string
	bodies         []map[string]any
	fetchPages     []string
	temporaryPages []string
}

func (s *subscribeStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.calls = append(s.calls, r.URL.Path)
		s.bodies = append(s.bodies, body)
		var response string
		switch r.URL.Path {
		case "/v1/admin/wcl":
			response = wclOK
		case "/v1/readers/open", "/v1/readers/ack", "/v1/readers/close":
			response = sessionJSON
		case "/v1/readers/fetch":
			if len(s.fetchPages) > 0 {
				response = s.fetchPages[0]
				s.fetchPages = s.fetchPages[1:]
			} else {
				response = `{"records": [], "delivered_cursor": null}`
			}
		case "/v1/readers/temporary/fetch":
			if len(s.temporaryPages) > 0 {
				response = s.temporaryPages[0]
				s.temporaryPages = s.temporaryPages[1:]
			} else {
				response = `{"records": [], "next_cursor": null}`
			}
		default:
			response = `{}`
		}
		s.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(response))
	}
}

func (s *subscribeStub) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if call == path {
			n++
		}
	}
	return n
}

func (s *subscribeStub) temporaryBodies() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []map[string]any
	for i, call := range s.calls {
		if call == "/v1/readers/temporary/fetch" {
			result = append(result, s.bodies[i])
		}
	}
	return result
}

func temporaryPage(next string, cursorKeys ...[2]string) string {
	records := make([]map[string]any, 0, len(cursorKeys))
	for _, pair := range cursorKeys {
		records = append(records, recordJSON(pair[0], pair[1]))
	}
	nextJSON := "null"
	if next != "" {
		nextJSON = `"` + next + `"`
	}
	out, _ := json.Marshal(records)
	return `{"records": ` + string(out) + `, "next_cursor": ` + nextJSON + `}`
}

func TestLoadPagesUntilEmptyAndFillsTheStore(t *testing.T) {
	stub := &subscribeStub{
		temporaryPages: []string{
			temporaryPage("c1", [2]string{"r1", "azE="}),
			temporaryPage("c2", [2]string{"r2", "azI="}),
		},
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	var received []Record
	loaded, err := client.Feeds.Load(
		context.Background(), "hr.employees", StartBeginning(),
		func(records []Record) error {
			received = append(received, records...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 2 || len(received) != 2 {
		t.Fatalf("loaded=%d received=%d, want 2/2", loaded, len(received))
	}
	bodies := stub.temporaryBodies()
	if len(bodies) != 3 {
		t.Fatalf("temporary fetches = %d, want 3", len(bodies))
	}
	if _, has := bodies[0]["after"]; has {
		t.Fatalf("first page must read from the beginning, got %v", bodies[0])
	}
	if bodies[1]["after"] != "c1" {
		t.Fatalf("continuation after = %v, want c1", bodies[1]["after"])
	}
}

func TestLoadSeeksByEventTimeThenContinuesOnCursor(t *testing.T) {
	stub := &subscribeStub{
		temporaryPages: []string{temporaryPage("c1", [2]string{"r1", "azE="})},
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	var sink [][]Record
	_, err := client.Feeds.Load(
		context.Background(), "hr.employees", StartTimestampNs(1697040000000000000),
		func(records []Record) error {
			sink = append(sink, records)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	bodies := stub.temporaryBodies()
	if bodies[0]["after_event_time_ns"] != "1697040000000000000" {
		t.Fatalf("after_event_time_ns = %v", bodies[0]["after_event_time_ns"])
	}
	if _, has := bodies[0]["after"]; has {
		t.Fatal("event-time seek must not combine after")
	}
	if bodies[1]["after"] != "c1" {
		t.Fatalf("continuation after = %v, want c1", bodies[1]["after"])
	}
	if len(sink) != 1 {
		t.Fatalf("sink pages = %d, want 1", len(sink))
	}
}

func TestSubscribeOpensDeliversAcksAndCloses(t *testing.T) {
	stub := &subscribeStub{
		fetchPages: []string{
			`{"records": [` + mustRecord("r1") + `], "delivered_cursor": "d1"}`,
		},
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	delivered := make(chan []Record, 1)
	sub := client.Subscribe(context.Background(), "hr.view", "hr.employees",
		StartBeginning(),
		func(records []Record) error {
			delivered <- records
			return nil
		}, nil)
	select {
	case batch := <-delivered:
		if len(batch) != 1 {
			t.Fatalf("batch = %d, want 1", len(batch))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never received the page")
	}
	// The ack posts after the handler returns — wait for it before close.
	deadline := time.Now().Add(5 * time.Second)
	for stub.count("/v1/readers/ack") == 0 && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	sub.Close()

	if stub.count("/v1/admin/wcl") != 1 {
		t.Fatalf("wcl calls = %d, want 1 (reader auto-provisioned)", stub.count("/v1/admin/wcl"))
	}
	if stub.count("/v1/readers/ack") == 0 {
		t.Fatal("delivered cursor never acked")
	}
	if stub.count("/v1/readers/close") == 0 {
		t.Fatal("session never released")
	}
}

func mustRecord(cursor string) string {
	out, _ := json.Marshal(recordJSON(cursor, "azE="))
	return string(out)
}
