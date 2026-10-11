package whitewater

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

// Wire codec: the only place request/response shapes are interpreted.
// Binary fields are base64; signed 64-bit nanoseconds are decimal
// strings; metadata maps to metadata_base64 entries.

func encodeAppendRequest(
	requestID, writer string, sessionEpoch int64,
	key, payload []byte, eventTimeNs *int64, metadata map[string][]byte,
) map[string]any {
	metadataWire := make(map[string]any, len(metadata))
	for name, value := range metadata {
		metadataWire[name] = base64.StdEncoding.EncodeToString(value)
	}
	body := map[string]any{
		"request_id":      requestID,
		"writer":          writer,
		"session_epoch":   sessionEpoch,
		"payload_base64":  base64.StdEncoding.EncodeToString(payload),
		"metadata_base64": metadataWire,
	}
	if key != nil {
		body["key_base64"] = base64.StdEncoding.EncodeToString(key)
	}
	if eventTimeNs != nil {
		body["event_time_ns"] = strconv.FormatInt(*eventTimeNs, 10)
	}
	return body
}

func decodeBody(raw json.RawMessage) (map[string]any, error) {
	var object map[string]any
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return object, nil
}

func stringField(object map[string]any, name string) string {
	value, _ := object[name].(string)
	return value
}

func intField(object map[string]any, name string) int64 {
	switch value := object[name].(type) {
	case float64:
		return int64(value)
	case string:
		parsed, _ := strconv.ParseInt(value, 10, 64)
		return parsed
	}
	return 0
}

func doubleField(object map[string]any, name string) float64 {
	value, _ := object[name].(float64)
	return value
}

func boolField(object map[string]any, name string) bool {
	value, _ := object[name].(bool)
	return value
}

func cursorField(object map[string]any, name string) *Cursor {
	if value, ok := object[name].(string); ok {
		cursor := Cursor(value)
		return &cursor
	}
	return nil
}

func decodeBytes(object map[string]any, name string) []byte {
	value, _ := object[name].(string)
	if value == "" {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil
	}
	return decoded
}

func decodeFeedback(object map[string]any) ServerFeedback {
	return ServerFeedback{
		RecommendedBatchCount: int(intField(object, "recommended_batch_count")),
		RecommendedBatchBytes: int(intField(object, "recommended_batch_bytes")),
		Pressure:              doubleField(object, "pressure"),
		RetryAfterMs:          int(intField(object, "retry_after_ms")),
		MaxFrameBytes:         intField(object, "max_frame_bytes"),
		HotKey:                boolField(object, "hot_key"),
	}
}

func decodeAppendResult(object map[string]any) AppendResult {
	feedback, _ := object["feedback"].(map[string]any)
	return AppendResult{
		MessageID:    stringField(object, "message_id"),
		Cursor:       Cursor(stringField(object, "cursor")),
		Deduplicated: boolField(object, "deduplicated"),
		Feedback:     decodeFeedback(feedback),
	}
}

func decodeRecord(object map[string]any) Record {
	metadata := map[string][]byte{}
	if raw, ok := object["metadata_base64"].(map[string]any); ok {
		for name, value := range raw {
			if encoded, ok := value.(string); ok {
				if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
					metadata[name] = decoded
				}
			}
		}
	}
	return Record{
		Cursor:       Cursor(stringField(object, "cursor")),
		MessageID:    stringField(object, "message_id"),
		ProducerID:   stringField(object, "producer_id"),
		Sequence:     intField(object, "sequence"),
		EventTimeNs:  intField(object, "event_time_ns"),
		IngestTimeNs: intField(object, "ingest_time_ns"),
		Key:          decodeBytes(object, "key_base64"),
		Payload:      decodeBytes(object, "payload_base64"),
		Metadata:     metadata,
	}
}

func decodeRecords(raw any) []Record {
	array, ok := raw.([]any)
	if !ok {
		return nil
	}
	records := make([]Record, 0, len(array))
	for _, item := range array {
		if object, ok := item.(map[string]any); ok {
			records = append(records, decodeRecord(object))
		}
	}
	return records
}

func decodeReaderSession(object map[string]any) ReaderSession {
	return ReaderSession{
		ReaderID:           stringField(object, "reader_id"),
		FeedID:             stringField(object, "feed_id"),
		SessionEpoch:       intField(object, "session_epoch"),
		Capacity:           int(intField(object, "capacity")),
		DeliveredCursor:    cursorField(object, "delivered_cursor"),
		AcknowledgedCursor: cursorField(object, "acknowledged_cursor"),
	}
}

func decodeReaderPage(object map[string]any) ReaderPage {
	return ReaderPage{
		Records:             decodeRecords(object["records"]),
		DeliveredCursor:     cursorField(object, "delivered_cursor"),
		AcknowledgedCursor:  cursorField(object, "acknowledged_cursor"),
		RecommendedCapacity: int(intField(object, "recommended_capacity")),
		RetryAfterMs:        int(intField(object, "retry_after_ms")),
	}
}

func decodeMemberPage(object map[string]any) MemberPage {
	positions := map[string]string{}
	if raw, ok := object["positions"].(map[string]any); ok {
		for name, value := range raw {
			if cursor, ok := value.(string); ok {
				positions[name] = cursor
			}
		}
	}
	return MemberPage{
		Records:        decodeRecords(object["records"]),
		Cursor:         cursorField(object, "cursor"),
		Positions:      positions,
		OwnershipEpoch: intField(object, "ownership_epoch"),
	}
}
