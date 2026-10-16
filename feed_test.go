package whitewater

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Order is the JSON-decoded payload shape used across typed-feed tests.
type orderPayload struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

const appendOK = `{
  "message_id": "00000000-0000-7000-8000-000000000009",
  "cursor": "c1",
  "deduplicated": false,
  "feedback": {"recommended_batch_count": 8, "pressure": 0.1}
}`

const wclSessionOK = `{
  "request_id": "00000000-0000-7000-8000-000000000010",
  "revision": 1, "authority": "control_plane",
  "results": [{"statement": "s", "message": "ok", "data": {"session_epoch": 42}}]
}`

const batchOK = `{
  "results": [{"message_id": "00000000-0000-7000-8000-000000000011", "cursor": "c1", "deduplicated": false}],
  "feedback": {"recommended_batch_count": 8}
}`

// feedStub routes every feed/writer/reader/store endpoint to a scripted
// responder and records calls.
type feedStub struct {
	mu             sync.Mutex
	calls          []string
	bodies         []map[string]any
	routes         map[string]string
	fetchPages     []string
	temporaryPages []string
	recordsJSON    string
}

func newFeedStub() *feedStub {
	return &feedStub{routes: map[string]string{
		"/v1/admin/wcl":            wclSessionOK,
		"/v1/writers/append":       appendOK,
		"/v1/writers/append-batch": batchOK,
		"/v1/feeds/append":         appendOK,
		"/v1/readers/open":         sessionJSON,
		"/v1/readers/ack":          sessionJSON,
		"/v1/readers/close":        sessionJSON,
		"/v1/state-stores/mutate":  mutationOK,
		"/v1/state-stores/clear":   mutationOK,
	}}
}

func (s *feedStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Method == http.MethodPost {
			raw := make([]byte, 0)
			buf := [1024]byte{}
			for {
				n, err := r.Body.Read(buf[:])
				raw = append(raw, buf[:n]...)
				if err != nil {
					break
				}
			}
			_ = json.Unmarshal(raw, &body)
		}
		s.mu.Lock()
		s.calls = append(s.calls, r.Method+" "+r.URL.Path)
		s.bodies = append(s.bodies, body)
		var response string
		switch {
		case r.URL.Path == "/v1/feeds/records":
			response = s.recordsJSON
		case r.URL.Path == "/v1/readers/fetch" && len(s.fetchPages) > 0:
			response = s.fetchPages[0]
			s.fetchPages = s.fetchPages[1:]
		case r.URL.Path == "/v1/readers/fetch":
			response = `{"records": [], "delivered_cursor": null}`
		case r.URL.Path == "/v1/readers/temporary/fetch" && len(s.temporaryPages) > 0:
			response = s.temporaryPages[0]
			s.temporaryPages = s.temporaryPages[1:]
		case r.URL.Path == "/v1/readers/temporary/fetch":
			response = `{"records": [], "next_cursor": null}`
		case r.URL.Path == "/v1/state-stores/lookup":
			response = s.routes[r.URL.Path]
		case r.URL.Path == "/v1/state-stores/get-by":
			response = s.routes[r.URL.Path]
		case r.URL.Path == "/v1/state-stores/iterate":
			response = s.routes[r.URL.Path]
		default:
			if fixed, ok := s.routes[r.URL.Path]; ok {
				response = fixed
			} else {
				response = `{}`
			}
		}
		s.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(response))
	}
}

func (s *feedStub) bodiesFor(path string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for i, call := range s.calls {
		if strings.HasSuffix(call, " "+path) {
			out = append(out, s.bodies[i])
		}
	}
	return out
}

func (s *feedStub) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if strings.HasSuffix(call, " "+path) {
			n++
		}
	}
	return n
}

func jsonRecord(cursor, key, payload string) map[string]any {
	record := recordJSON(cursor, key)
	record["payload_base64"] = b64(payload)
	return record
}

func TestFeedOfValidatesName(t *testing.T) {
	client := NewClient("test-key", []string{"http://127.0.0.1:1"}, time.Second)
	if _, err := FeedFor[orderPayload](client, "Bad Name"); err == nil {
		t.Fatal("expected a name refusal")
	}
	if _, err := FeedFor[orderPayload](client, "orders.events"); err != nil {
		t.Fatal(err)
	}
}

func TestFeedRecordsDecodesJSONAndPreservesFields(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("order-1"), `{"id":"order-1","total":42}`)
	record["metadata_base64"] = map[string]string{"tenant": b64("acme")}
	out, _ := json.Marshal([]map[string]any{record})
	stub.recordsJSON = string(out)
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, err := FeedFor[orderPayload](client, "orders.events")
	if err != nil {
		t.Fatal(err)
	}
	messages, err := orders.Records(context.Background(), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("records = %d, want 1", len(messages))
	}
	message := messages[0]
	if message.Key == nil || *message.Key != "order-1" {
		t.Fatalf("key = %v, want order-1", message.Key)
	}
	if message.Value.ID != "order-1" || message.Value.Total != 42 {
		t.Fatalf("value = %+v", message.Value)
	}
	if string(message.Cursor) != "c1" || message.Sequence != 7 ||
		message.EventTimeNs != 1697040000123456789 {
		t.Fatalf("envelope not preserved: %+v", message)
	}
	if message.MetadataText("tenant") != "acme" {
		t.Fatalf("metadata = %v", message.Metadata)
	}
}

func TestFeedFetchDecodesTemporaryPage(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("o1"), `{"id":"o1","total":1}`)
	out, _ := json.Marshal([]map[string]any{record})
	stub.temporaryPages = []string{
		`{"records": ` + string(out) + `, "next_cursor": "c9"}`,
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	page, err := orders.Fetch(context.Background(), TemporaryFetchOptions{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].Value.Total != 1 {
		t.Fatalf("page = %+v", page)
	}
	if page.NextCursor == nil || string(*page.NextCursor) != "c9" {
		t.Fatalf("next cursor = %v", page.NextCursor)
	}
}

func TestFeedLoadDeliversDecodedBatches(t *testing.T) {
	stub := newFeedStub()
	first := jsonRecord("c1", b64("k1"), `{"id":"a","total":1}`)
	second := jsonRecord("c2", b64("k2"), `{"id":"b","total":2}`)
	page1, _ := json.Marshal([]map[string]any{first, second})
	stub.temporaryPages = []string{
		`{"records": ` + string(page1) + `, "next_cursor": "c2"}`,
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	var seen []orderPayload
	loaded, err := orders.Load(
		context.Background(), StartBeginning(),
		func(messages []FeedMessage[string, orderPayload]) error {
			for _, m := range messages {
				seen = append(seen, m.Value)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 2 || len(seen) != 2 || seen[0].ID != "a" || seen[1].ID != "b" {
		t.Fatalf("loaded = %d seen = %v", loaded, seen)
	}
}

func TestFeedWriterDeclaresOpensAndEncodes(t *testing.T) {
	stub := newFeedStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	writer, err := orders.Writer(context.Background(), "ingest")
	if err != nil {
		t.Fatal(err)
	}
	if writer.SessionEpoch() != 42 {
		t.Fatalf("epoch = %d, want 42", writer.SessionEpoch())
	}
	if _, err := writer.Append(
		context.Background(), "order-1", orderPayload{ID: "order-1", Total: 7}); err != nil {
		t.Fatal(err)
	}

	wcl := stub.bodiesFor("/v1/admin/wcl")
	if len(wcl) != 2 ||
		wcl[0]["script"] != "CREATE WRITER ingest TO orders.events" ||
		wcl[1]["script"] != "OPEN WRITER SESSION ingest" {
		t.Fatalf("wcl = %v", wcl)
	}
	appends := stub.bodiesFor("/v1/writers/append")
	if len(appends) != 1 {
		t.Fatalf("append calls = %d", len(appends))
	}
	body := appends[0]
	if body["writer"] != "ingest" || body["session_epoch"] != float64(42) {
		t.Fatalf("append body = %v", body)
	}
	if body["key_base64"] != b64("order-1") {
		t.Fatalf("key_base64 = %v", body["key_base64"])
	}
	var decoded orderPayload
	raw, _ := decodeB64Field(body, "payload_base64")
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Total != 7 {
		t.Fatalf("payload decoded = %+v err=%v", decoded, err)
	}
	if body["request_id"] == nil || body["request_id"] == "" {
		t.Fatal("request_id missing")
	}
}

func decodeB64Field(body map[string]any, name string) ([]byte, error) {
	value, _ := body[name].(string)
	return base64.StdEncoding.DecodeString(value)
}

func TestFeedWriterAppendAllAndKeylessEvent(t *testing.T) {
	stub := newFeedStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	writer, _ := orders.Writer(context.Background(), "ingest")
	eventTime := int64(1697040000000000000)
	key := "order-9"
	result, err := writer.AppendAll(context.Background(), []FeedEvent[string, orderPayload]{
		{RequestID: "rid-1", Key: &key, Value: orderPayload{ID: "order-9", Total: 9},
			EventTimeNs: &eventTime, Metadata: TextMetadata(map[string]string{"tenant": "acme"})},
		{Value: orderPayload{ID: "keyless", Total: 0}}, // nil Key → keyless record
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 {
		t.Fatalf("results = %v", result)
	}
	batches := stub.bodiesFor("/v1/writers/append-batch")
	if len(batches) != 1 {
		t.Fatalf("batch calls = %d", len(batches))
	}
	records, _ := batches[0]["records"].([]any)
	if len(records) != 2 {
		t.Fatalf("batch records = %v", batches[0])
	}
	first, _ := records[0].(map[string]any)
	if first["request_id"] != "rid-1" || first["event_time_ns"] != "1697040000000000000" {
		t.Fatalf("first record = %v", first)
	}
	metadata, _ := first["metadata_base64"].(map[string]any)
	if metadata["tenant"] != b64("acme") {
		t.Fatalf("metadata = %v", first["metadata_base64"])
	}
	second, _ := records[1].(map[string]any)
	if _, hasKey := second["key_base64"]; hasKey {
		t.Fatalf("keyless record carried a key: %v", second)
	}
}

func TestFeedReaderFetchesDecodesAcks(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("k1"), `{"id":"o1","total":3}`)
	out, _ := json.Marshal([]map[string]any{record})
	stub.fetchPages = []string{
		`{"records": ` + string(out) + `, "delivered_cursor": "c1",
		  "acknowledged_cursor": null, "recommended_capacity": 8}`,
	}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	reader, err := orders.Reader(context.Background(), "audit", 64)
	if err != nil {
		t.Fatal(err)
	}
	if reader.SessionEpoch() != 3 {
		t.Fatalf("epoch = %d, want 3", reader.SessionEpoch())
	}
	page, err := reader.Fetch(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].Value.ID != "o1" {
		t.Fatalf("page = %+v", page)
	}
	if page.DeliveredCursor == nil || string(*page.DeliveredCursor) != "c1" {
		t.Fatalf("delivered = %v", page.DeliveredCursor)
	}
	if _, err := reader.Ack(context.Background()); err != nil {
		t.Fatal(err)
	}
	acks := stub.bodiesFor("/v1/readers/ack")
	if len(acks) != 1 || acks[0]["cursor"] != "c1" {
		t.Fatalf("acks = %v", acks)
	}
	if err := reader.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stub.count("/v1/readers/close") != 1 {
		t.Fatal("reader close did not post")
	}
}

func TestFeedAdHocAppendEncodes(t *testing.T) {
	stub := newFeedStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	if _, err := orders.Append(
		context.Background(), "ws-1", 7, 3, "order-2",
		orderPayload{ID: "order-2", Total: 11}); err != nil {
		t.Fatal(err)
	}
	bodies := stub.bodiesFor("/v1/feeds/append")
	if len(bodies) != 1 {
		t.Fatalf("ad-hoc appends = %d", len(bodies))
	}
	body := bodies[0]
	if body["feed"] != "orders.events" || body["writer_session_id"] != "ws-1" ||
		body["writer_epoch"] != float64(7) || body["sequence"] != float64(3) {
		t.Fatalf("append body = %v", body)
	}
	if body["key_base64"] != b64("order-2") {
		t.Fatalf("key_base64 = %v", body["key_base64"])
	}
}

func TestRebindFeedReadsRawWithoutWireCall(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("k1"), "raw-bytes")
	out, _ := json.Marshal([]map[string]any{record})
	stub.recordsJSON = string(out)
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	raw := RebindFeed(orders, StringCodec(), BytesCodec())
	messages, err := raw.Records(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(messages[0].Value) != "raw-bytes" {
		t.Fatalf("value = %q", messages[0].Value)
	}
	if raw.Name() != "orders.events" {
		t.Fatalf("name = %s", raw.Name())
	}
}

func TestFuncCodecRoundTripsAndFeedsThrough(t *testing.T) {
	// A trivial custom codec: payloads wrapped in <v>...</v> "xml-ish".
	xmlish := FuncCodec(
		func(s string) ([]byte, error) {
			return []byte("<v>" + s + "</v>"), nil
		},
		func(b []byte) (string, error) {
			text := string(b)
			return strings.TrimSuffix(strings.TrimPrefix(text, "<v>"), "</v>"), nil
		})
	stub := newFeedStub()
	record := jsonRecord("c1", b64("k1"), "<v>hello</v>")
	out, _ := json.Marshal([]map[string]any{record})
	stub.recordsJSON = string(out)
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	feed, err := FeedOf(client, "orders.events", StringCodec(), xmlish)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := feed.Records(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Value != "hello" {
		t.Fatalf("value = %q", messages[0].Value)
	}
}

func TestDecodeFailureSurfaces(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("k1"), "not-json")
	out, _ := json.Marshal([]map[string]any{record})
	stub.recordsJSON = string(out)
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	orders, _ := FeedFor[orderPayload](client, "orders.events")
	if _, err := orders.Records(
		context.Background(), 1, nil); err == nil ||
		!strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestDefaultFeedBindsStringKeysAndRawPayloads(t *testing.T) {
	stub := newFeedStub()
	record := jsonRecord("c1", b64("k1"), "raw-payload")
	out, _ := json.Marshal([]map[string]any{record})
	stub.recordsJSON = string(out)
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	feed, err := client.Feed("orders.events")
	if err != nil {
		t.Fatal(err)
	}
	messages, err := feed.Records(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if *messages[0].Key != "k1" || string(messages[0].Value) != "raw-payload" {
		t.Fatalf("message = %+v", messages[0])
	}
}

func TestCodecForInfersByType(t *testing.T) {
	if _, ok := CodecFor[string]().(stringCodec); !ok {
		t.Fatal("string should map to StringCodec")
	}
	if _, ok := CodecFor[[]byte]().(bytesCodec); !ok {
		t.Fatal("[]byte should map to BytesCodec")
	}
	if _, ok := CodecFor[orderPayload]().(jsonCodec[orderPayload]); !ok {
		t.Fatal("struct should map to JSONCodec")
	}
	encoded, _ := StringCodec().Encode("héllo")
	decoded, _ := StringCodec().Decode(encoded)
	if decoded != "héllo" {
		t.Fatalf("utf8 round trip = %q", decoded)
	}
	raw, _ := BytesCodec().Encode([]byte{1, 2, 3})
	if len(raw) != 3 || raw[0] != 1 {
		t.Fatalf("bytes round trip = %v", raw)
	}
}

// --- typed Store ---

const storeLookupOK = `{
  "found": true,
  "key_base64": "dXNlci0x",
  "payload_base64": "eyJpZCI6InVzZXItMSIsInRvdGFsIjo0Mn0=",
  "applied_cursor": "c9",
  "freshness": {"committed_seq": 9, "behind": false, "served_by": "n1"}
}`

const storeGetByOK = `{
  "rows": [{"key_base64": "dXNlci0x",
            "payload_base64": "eyJpZCI6InVzZXItMSIsInRvdGFsIjo0Mn0=",
            "applied_cursor": "c9"}],
  "freshness": {"committed_seq": 9, "behind": false, "served_by": "n1"}
}`

const storeIterateOK = `{
  "rows": [{"key_base64": "dXNlci0x",
            "payload_base64": "eyJpZCI6InVzZXItMSIsInRvdGFsIjo0Mn0="}],
  "next_key_base64": null,
  "pinned_seq": 5, "served_by": "n1"
}`

func TestStoreTypedPutGetAndNamedLookup(t *testing.T) {
	stub := newFeedStub()
	stub.routes["/v1/state-stores/lookup"] = storeLookupOK
	stub.routes["/v1/state-stores/get-by"] = storeGetByOK
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	store, err := StoreFor[orderPayload](client, "accounts.employees")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(
		context.Background(), "user-1", orderPayload{ID: "user-1", Total: 42},
		map[string]any{"passport": "P111"}, ""); err != nil {
		t.Fatal(err)
	}
	mutations := stub.bodiesFor("/v1/state-stores/mutate")
	if len(mutations) != 1 {
		t.Fatalf("mutations = %d", len(mutations))
	}
	op, _ := mutations[0]["op"].(map[string]any)
	if op["key_base64"] != b64("user-1") {
		t.Fatalf("op = %v", op)
	}
	var payload orderPayload
	raw, _ := decodeB64Field(op, "payload_base64")
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Total != 42 {
		t.Fatalf("payload = %v", op["payload_base64"])
	}

	row, err := store.Get(context.Background(), "user-1", false, 0)
	if err != nil || row == nil {
		t.Fatalf("get = %v err=%v", row, err)
	}
	if row.Key != "user-1" || row.Value.Total != 42 {
		t.Fatalf("row = %+v", row)
	}
	if row.AppliedCursor == nil || *row.AppliedCursor != "c9" {
		t.Fatalf("applied cursor = %v", row.AppliedCursor)
	}

	result, err := store.GetBy(context.Background(), "passport", "P111", 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Value.ID != "user-1" {
		t.Fatalf("get-by = %+v", result)
	}
}

func TestStoreTypedIterateAndDeleteAndRebind(t *testing.T) {
	stub := newFeedStub()
	stub.routes["/v1/state-stores/iterate"] = storeIterateOK
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, 5*time.Second)

	store, err := StoreFor[orderPayload](client, "accounts.employees")
	if err != nil {
		t.Fatal(err)
	}
	var visited []string
	if err := store.Iterate(
		context.Background(), 64,
		func(row Row[string, orderPayload]) error {
			visited = append(visited, row.Key)
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 1 || visited[0] != "user-1" {
		t.Fatalf("iterated = %v", visited)
	}
	if _, err := store.Delete(context.Background(), "user-1", ""); err != nil {
		t.Fatal(err)
	}
	mutations := stub.bodiesFor("/v1/state-stores/mutate")
	last := mutations[len(mutations)-1]
	op, _ := last["op"].(map[string]any)
	if op["kind"] != "delete_row" || op["key_base64"] != b64("user-1") {
		t.Fatalf("delete op = %v", op)
	}

	// Rebind to raw payloads — no wire call to rebind.
	rawStore := RebindStore(store, StringCodec(), BytesCodec())
	page, err := rawStore.Page(context.Background(), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || string(page.Rows[0].Value) == "" {
		t.Fatalf("raw page = %+v", page)
	}
}

func TestStoreEncodeFailureSurfaces(t *testing.T) {
	failing := FuncCodec(
		func(v orderPayload) ([]byte, error) { return nil, errTestCodec },
		func(b []byte) (orderPayload, error) { return orderPayload{}, errTestCodec })
	client := NewClient("test-key", []string{"http://127.0.0.1:1"}, time.Second)
	store, err := StoreOf(client, "accounts.employees", StringCodec(), failing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(
		context.Background(), "k", orderPayload{}, nil, ""); err == nil ||
		!strings.Contains(err.Error(), "encode") {
		t.Fatalf("expected encode error, got %v", err)
	}
}

var errTestCodec = errors.New("codec blew up")
