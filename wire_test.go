package whitewater

import (
	"encoding/json"
	"math"
	"testing"
)

func decode(t *testing.T, raw string) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		t.Fatalf("fixture json: %v", err)
	}
	return object
}

func TestAppendRequestWireShape(t *testing.T) {
	eventTime := int64(math.MinInt64)
	body := encodeAppendRequest(
		"019a0000-0000-7000-8000-000000000001", "orders.wr", 7,
		[]byte("key-1"), []byte("payload"), &eventTime,
		map[string][]byte{"source": []byte("unit")})
	if body["request_id"] != "019a0000-0000-7000-8000-000000000001" {
		t.Errorf("request_id = %v", body["request_id"])
	}
	if body["session_epoch"] != int64(7) {
		t.Errorf("session_epoch = %v", body["session_epoch"])
	}
	if body["key_base64"] != "a2V5LTE=" {
		t.Errorf("key_base64 = %v", body["key_base64"])
	}
	if body["payload_base64"] != "cGF5bG9hZA==" {
		t.Errorf("payload_base64 = %v", body["payload_base64"])
	}
	// i64 nanoseconds serialize as a decimal string, never a JSON number.
	if body["event_time_ns"] != "-9223372036854775808" {
		t.Errorf("event_time_ns = %v", body["event_time_ns"])
	}
	metadata := body["metadata_base64"].(map[string]any)
	if metadata["source"] != "dW5pdA==" {
		t.Errorf("metadata.source = %v", metadata["source"])
	}
}

func TestRecordDecode(t *testing.T) {
	record := decodeRecord(decode(t, `{
		"cursor": "AAAABBBBCCCCDDDD",
		"message_id": "019a0000-0000-7000-8000-000000000002",
		"producer_id": "019a0000-0000-7000-8000-000000000003",
		"sequence": 42,
		"event_time_ns": "9223372036854775807",
		"ingest_time_ns": "-5",
		"key_base64": "3q2+7w==",
		"payload_base64": "AAEC",
		"metadata_base64": {"m": "eA=="}
	}`))
	if record.Cursor != "AAAABBBBCCCCDDDD" {
		t.Errorf("cursor = %q", record.Cursor)
	}
	if record.EventTimeNs != math.MaxInt64 {
		t.Errorf("event_time_ns = %d", record.EventTimeNs)
	}
	if record.IngestTimeNs != -5 {
		t.Errorf("ingest_time_ns = %d", record.IngestTimeNs)
	}
	if string(record.Key) != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("key = %x", record.Key)
	}
	if string(record.Metadata["m"]) != "x" {
		t.Errorf("metadata.m = %x", record.Metadata["m"])
	}
}

func TestFeedbackDecodeHotKeyDefault(t *testing.T) {
	flagged := decodeFeedback(decode(t, `{
		"recommended_batch_count": 8, "recommended_batch_bytes": 65536,
		"pressure": 0.9, "retry_after_ms": 50,
		"max_frame_bytes": 67108864, "hot_key": true
	}`))
	if !flagged.HotKey {
		t.Error("expected hot_key true")
	}
	cold := decodeFeedback(decode(t, `{"recommended_batch_count": 100}`))
	if cold.HotKey {
		t.Error("expected hot_key default false")
	}
}
