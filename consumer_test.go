package whitewater

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// consumerStub holds a pending page: once queued it keeps answering
// fetches until an ack lands -- exactly like the server redelivering an
// unacknowledged page.
type consumerStub struct {
	mu          sync.Mutex
	calls       []string
	bodies      []map[string]any
	pending     string
	queuedPages []string
	wclStatus   int
	wclBody     string
	openStatus  int
	openBody    string
	opens       int
	delivered   chan struct{}
}

func newConsumerStub() *consumerStub {
	return &consumerStub{
		wclStatus: 200, wclBody: wclOK,
		openStatus: 200, openBody: sessionJSON,
		delivered: make(chan struct{}, 16),
	}
}

func (s *consumerStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.calls = append(s.calls, r.URL.Path)
		s.bodies = append(s.bodies, body)
		status, response := 200, `{}`
		switch r.URL.Path {
		case "/v1/admin/wcl":
			status, response = s.wclStatus, s.wclBody
		case "/v1/readers/open":
			s.opens++
			status, response = s.openStatus, s.openBody
		case "/v1/readers/fetch":
			switch {
			case s.pending != "":
				response = s.pending // unacked: redeliver
			case len(s.queuedPages) > 0:
				s.pending = s.queuedPages[0]
				s.queuedPages = s.queuedPages[1:]
				response = s.pending
			default:
				response = `{"records": [], "delivered_cursor": null}`
			}
			select {
			case s.delivered <- struct{}{}:
			default:
			}
		case "/v1/readers/ack":
			s.pending = ""
			response = sessionJSON
		case "/v1/readers/close":
			response = sessionJSON
		}
		s.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(response))
	}
}

func (s *consumerStub) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c == path {
			n++
		}
	}
	return n
}

func (s *consumerStub) waitDelivered(t *testing.T) {
	t.Helper()
	select {
	case <-s.delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("no page delivered")
	}
}

func (s *consumerStub) waitFor(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.count(path) < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.count(path) < n {
		t.Fatalf("%s calls = %d, want >= %d", path, s.count(path), n)
	}
}

func consumerPage(delivered string, cursors ...string) string {
	records := ""
	for i, c := range cursors {
		if i > 0 {
			records += ","
		}
		records += mustRecord(c)
	}
	return `{"records": [` + records + `], "delivered_cursor": "` + delivered + `"}`
}

func TestConsumerMessagesDeliversAcksAndCloses(t *testing.T) {
	stub := newConsumerStub()
	stub.queuedPages = []string{consumerPage("c2", "c1", "c2")}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := feed.Consumer("enricher")
	messages := consumer.Messages(ctx)

	var got []FeedMessage[string, []byte]
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case m, ok := <-messages:
			if !ok {
				t.Fatal("channel closed early")
			}
			got = append(got, m)
		case <-deadline:
			t.Fatal("timed out waiting for records")
		}
	}
	stub.waitFor(t, "/v1/readers/ack", 1)
	cancel()
	consumer.Close()
	consumer.Wait()

	if string(got[0].Value) != "val" || got[1].Cursor != "c2" {
		t.Fatalf("unexpected messages: %+v", got)
	}
	if stub.count("/v1/readers/ack") != 1 {
		t.Fatalf("acks = %d, want 1", stub.count("/v1/readers/ack"))
	}
	if stub.count("/v1/readers/close") == 0 {
		t.Fatal("session never released")
	}
	if err := consumer.Err(); err != nil {
		t.Fatalf("clean close returned err %v", err)
	}
}

func TestConsumerRunHandlerFailureRedeliversPage(t *testing.T) {
	stub := newConsumerStub()
	stub.queuedPages = []string{consumerPage("c1", "c1")}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}

	attempts := 0
	var errs []error
	consumer := feed.Consumer("enricher").Backoff(5*time.Millisecond, 20*time.Millisecond)
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(context.Background(), func(FeedMessage[string, []byte]) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient handler bug")
			}
			consumer.Close() // signals only -- safe inside a handler
			return nil
		})
	}()
	stub.waitFor(t, "/v1/readers/ack", 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after clean close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned after Close")
	}
	_ = errs

	if attempts < 2 {
		t.Fatalf("attempts = %d, want redelivery", attempts)
	}
	if stub.count("/v1/readers/ack") != 1 {
		t.Fatalf("acks = %d, want exactly 1 (only the replay acks)",
			stub.count("/v1/readers/ack"))
	}
	if stub.opens < 2 {
		t.Fatalf("opens = %d, want supervised restart", stub.opens)
	}
}

func TestConsumerStaleSessionReopens(t *testing.T) {
	stub := newConsumerStub()
	stub.openStatus = 409
	stub.openBody = `{"error": "fenced", "code": "stale_epoch"}`
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			stub.mu.Lock()
			if stub.opens > 0 {
				stub.openStatus = 200
				stub.openBody = sessionJSON
				stub.queuedPages = append(stub.queuedPages,
					consumerPage("c1", "c1"))
				stub.mu.Unlock()
				return
			}
			stub.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := feed.Consumer("enricher").Backoff(5*time.Millisecond, 20*time.Millisecond)
	messages := consumer.Messages(ctx)
	select {
	case m := <-messages:
		if string(m.Value) != "val" {
			t.Fatalf("value = %q", m.Value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never recovered from the stale session")
	}
	cancel()
	consumer.Close()
	consumer.Wait()
	if stub.opens < 2 {
		t.Fatalf("opens = %d, want >= 2", stub.opens)
	}
}

func TestConsumerStartupRefusalFailsFast(t *testing.T) {
	stub := newConsumerStub()
	stub.wclStatus = 400
	stub.wclBody = `{"error": "refused: denied", "code": "refused"}`
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}

	consumer := feed.Consumer("enricher").Backoff(5*time.Millisecond, 20*time.Millisecond)
	err = consumer.Run(context.Background(), func(FeedMessage[string, []byte]) error {
		t.Fatal("handler should never run")
		return nil
	})
	if err == nil || !IsRefused(err) {
		t.Fatalf("Run err = %v, want Refused", err)
	}
	if stub.count("/v1/readers/open") != 0 {
		t.Fatal("opened a session after a refused reader")
	}
}

func TestConsumerInvalidReaderNameFailsFastNotLoops(t *testing.T) {
	stub := newConsumerStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}

	consumer := feed.Consumer("Bad Name!")
	err = consumer.Run(context.Background(), func(FeedMessage[string, []byte]) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected a fail-fast error for an invalid name")
	}
	if len(stub.calls) != 0 {
		t.Fatalf("calls = %v, want none -- local validation should die first",
			stub.calls)
	}
}

func TestConsumerDoubleStartRefused(t *testing.T) {
	stub := newConsumerStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	feed, err := FeedOf(client, "users.update.request", StringCodec(), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	consumer := feed.Consumer("enricher")
	ctx, cancel := context.WithCancel(context.Background())
	consumer.Messages(ctx)
	if err := consumer.Run(ctx, func(FeedMessage[string, []byte]) error {
		return nil
	}); err == nil {
		t.Fatal("second start should be refused")
	}
	cancel()
	consumer.Close()
}

func TestStoreFindFindByGetOrDefault(t *testing.T) {
	stub := &storeStub{routes: map[string]string{
		"/v1/state-stores/lookup": `{
		  "found": true,
		  "payload_base64": "` + b64(`"alice"`) + `",
		  "applied_cursor": "00000000000000000003",
		  "applied_seq": 7, "committed_seq": 7, "behind": false,
		  "served_by": "storage-1"
		}`,
		"/v1/state-stores/get-by": `{
		  "rows": [
		    {"key_base64": "` + b64("a@x.io") + `", "payload_base64": "` + b64(`"alice"`) + `", "applied_cursor": "00000000000000000003"}
		  ],
		  "applied_seq": 7, "committed_seq": 7, "behind": false,
		  "served_by": "storage-1"
		}`,
	}}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)
	store, err := StoreOf(client, "users.profiles", StringCodec(), JSONCodec[string]())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if v, found, err := store.Find(ctx, "a@x.io", false, 0); err != nil ||
		!found || v != "alice" {
		t.Fatalf("Find = %q,%v,%v", v, found, err)
	}
	if v, found, err := store.FindBy(ctx, "user_id", "u-1", false, 0); err != nil ||
		!found || v != "alice" {
		t.Fatalf("FindBy = %q,%v,%v", v, found, err)
	}

	// Misses: found:false / empty rows -> zero value + false, fallback.
	stub.routes["/v1/state-stores/lookup"] =
		`{"found": false, "applied_seq": 7, "committed_seq": 7}`
	stub.routes["/v1/state-stores/get-by"] =
		`{"rows": [], "applied_seq": 7, "committed_seq": 7}`
	if _, found, err := store.Find(ctx, "ghost", false, 0); err != nil || found {
		t.Fatalf("Find miss = %v,%v", found, err)
	}
	if _, found, err := store.FindBy(ctx, "user_id", "ghost", false, 0); err != nil || found {
		t.Fatalf("FindBy miss = %v,%v", found, err)
	}
	if v, err := store.GetOrDefault(ctx, "ghost", "fallback"); err != nil ||
		v != "fallback" {
		t.Fatalf("GetOrDefault = %q,%v", v, err)
	}
}
