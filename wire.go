package whitewater

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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

func decodeBodyList(raw json.RawMessage) ([]any, error) {
	var list []any
	if len(raw) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return list, nil
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

func encodeFeedAppendRequest(feed string, record FeedAppend) map[string]any {
	metadata := make(map[string]any, len(record.Metadata))
	for name, value := range record.Metadata {
		metadata[name] = base64.StdEncoding.EncodeToString(value)
	}
	body := map[string]any{
		"request_id":        record.RequestID,
		"feed":              feed,
		"writer_session_id": record.WriterSessionID,
		"writer_epoch":      record.WriterEpoch,
		"sequence":          record.Sequence,
		"key_base64":        base64.StdEncoding.EncodeToString(record.Key),
		"payload_base64":    base64.StdEncoding.EncodeToString(record.Payload),
		"metadata_base64":   metadata,
	}
	if record.EventTimeNs != nil {
		body["event_time_ns"] = strconv.FormatInt(*record.EventTimeNs, 10)
	}
	return body
}

func decodeTemporaryFetchPage(object map[string]any) TemporaryFetchPage {
	return TemporaryFetchPage{
		Records:    decodeRecords(object["records"]),
		NextCursor: cursorField(object, "next_cursor"),
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

// encodeIndexValue maps one typed scalar to the wire IndexValue shape;
// float values are refused — the wire carries exact i64/text/bytes.
func encodeIndexValue(value any) (map[string]any, error) {
	switch v := value.(type) {
	case nil:
		return map[string]any{"kind": "null"}, nil
	case bool:
		return map[string]any{"kind": "bool", "value": v}, nil
	case int:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case int8:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case int16:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case int32:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case int64:
		return map[string]any{"kind": "i64", "value": v}, nil
	case uint:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case uint8:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case uint16:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case uint32:
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("StateStore i64 value overflows: %d", v)
		}
		return map[string]any{"kind": "i64", "value": int64(v)}, nil
	case string:
		return map[string]any{"kind": "text", "value": v}, nil
	case []byte:
		return map[string]any{
			"kind":         "bytes",
			"value_base64": base64.StdEncoding.EncodeToString(v),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported StateStore value type %T", value)
	}
}

// encodeIndexValues maps a scalar or []any to the typed values list a
// named-key lookup or posting carries (one value per component).
func encodeIndexValues(value any) ([]map[string]any, error) {
	list, isList := value.([]any)
	if !isList {
		list = []any{value}
	}
	values := make([]map[string]any, 0, len(list))
	for _, item := range list {
		encoded, err := encodeIndexValue(item)
		if err != nil {
			return nil, err
		}
		values = append(values, encoded)
	}
	return values, nil
}

// encodeNamedValues maps {"department": "eng", "composite": []any{v1,
// v2}} to the mutation's named entries.
func encodeNamedValues(named map[string]any) ([]map[string]any, error) {
	names := make([]string, 0, len(named))
	for name := range named {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := make([]map[string]any, 0, len(names))
	for _, name := range names {
		values, err := encodeIndexValues(named[name])
		if err != nil {
			return nil, err
		}
		entries = append(entries, map[string]any{"name": name, "values": values})
	}
	return entries, nil
}

func decodeMutationResult(raw json.RawMessage) (MutationResult, error) {
	object, err := decodeBody(raw)
	if err != nil {
		return MutationResult{}, err
	}
	return MutationResult{
		StoreID:      stringField(object, "store_id"),
		MutationID:   stringField(object, "mutation_id"),
		Sequence:     intField(object, "sequence"),
		Deduplicated: boolField(object, "deduplicated"),
	}, nil
}

func decodeStoreFreshness(object map[string]any) StoreFreshness {
	return StoreFreshness{
		AppliedSeq:   intField(object, "applied_seq"),
		CommittedSeq: intField(object, "committed_seq"),
		Behind:       boolField(object, "behind"),
		ServedBy:     stringField(object, "served_by"),
	}
}

func decodeStoreRows(object map[string]any, freshness *StoreFreshness) []StoreRow {
	raw, ok := object["rows"].([]any)
	if !ok {
		return nil
	}
	rows := make([]StoreRow, 0, len(raw))
	for _, item := range raw {
		node, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key, _ := base64.StdEncoding.DecodeString(stringField(node, "key_base64"))
		payload, _ := base64.StdEncoding.DecodeString(stringField(node, "payload_base64"))
		row := StoreRow{Key: key, Payload: payload, Freshness: freshness}
		if cursor := stringField(node, "applied_cursor"); cursor != "" {
			row.AppliedCursor = &cursor
		}
		rows = append(rows, row)
	}
	return rows
}
