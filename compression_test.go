package whitewater

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCompressionCanBeToggledWithoutChangingPayloadsOrOriginalClients(t *testing.T) {
	result := `{"message_id":"00000000-0000-4000-8000-000000000001","cursor":"opaque","deduplicated":false}`
	stub := &storeStub{routes: map[string]string{
		"/v1/feeds/append":         result,
		"/v1/writers/append":       result,
		"/v1/writers/append-batch": `{"results":[` + result + `],"feedback":{}}`,
	}}
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	client := NewClient("test-key", []string{server.URL}, time.Second)
	ctx := context.Background()
	feed := client.Feeds
	writer := client.Writer("orders.writer", 7)
	input := AppendInput{RequestID: newUUID(), Key: []byte("key"), Payload: []byte{0, 255}}
	record := FeedAppend{RequestID: input.RequestID, WriterSessionID: newUUID(), WriterEpoch: 1, Sequence: 1, Key: input.Key, Payload: input.Payload}
	for _, enabled := range []bool{false, true, false, true} {
		feed = feed.UseCompression(enabled)
		if _, err := feed.Append(ctx, "orders.events", record); err != nil {
			t.Fatal(err)
		}
		writer = writer.UseCompression(enabled)
		if _, err := writer.Append(ctx, input); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.AppendBatch(ctx, []AppendInput{input}); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/v1/feeds/append", "/v1/writers/append", "/v1/writers/append-batch"} {
			bodies := stub.bodiesFor(path)
			body := bodies[len(bodies)-1]
			if path == "/v1/writers/append-batch" {
				body = body["records"].([]any)[0].(map[string]any)
			}
			compressed, _ := body["use_compression"].(bool)
			if compressed != enabled {
				t.Fatalf("%s compression = %v; want %v", path, compressed, enabled)
			}
			if body["payload_base64"] != "AP8=" {
				t.Fatalf("payload changed: %v", body)
			}
			if body["request_id"] != input.RequestID {
				t.Fatalf("request identity changed: %v", body)
			}
		}
	}
	if _, err := client.Feeds.Append(ctx, "orders.events", record); err != nil {
		t.Fatal(err)
	}
	bodies := stub.bodiesFor("/v1/feeds/append")
	if _, exists := bodies[len(bodies)-1]["use_compression"]; exists {
		t.Fatal("original client was modified")
	}
}
