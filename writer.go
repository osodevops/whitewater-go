package whitewater

import "context"

// AppendInput is one record in a Writer append or batch.
type AppendInput struct {
	// RequestID deduplicates the record on the server; zero value
	// generates a fresh UUID.
	RequestID   string
	Key         []byte
	Payload     []byte
	EventTimeNs *int64
	Metadata    map[string][]byte
}

// WriterSessionClient is one named-Writer session: idempotent appends
// fenced by sessionEpoch.
type WriterSessionClient struct {
	transport    *transport
	writer       string
	sessionEpoch int64
}

// Append issues one idempotent append; the same RequestID dedups.
func (w *WriterSessionClient) Append(
	ctx context.Context, input AppendInput,
) (AppendResult, error) {
	requestID := input.RequestID
	if requestID == "" {
		requestID = newUUID()
	}
	raw, err := w.transport.post(ctx, "/v1/writers/append", encodeAppendRequest(
		requestID, w.writer, w.sessionEpoch,
		input.Key, input.Payload, input.EventTimeNs, input.Metadata))
	if err != nil {
		return AppendResult{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return AppendResult{}, err
	}
	return decodeAppendResult(body), nil
}

// AppendBatch issues one epoch-fenced batch; each record carries its
// own RequestID.
func (w *WriterSessionClient) AppendBatch(
	ctx context.Context, records []AppendInput,
) (AppendBatchResult, error) {
	wire := make([]map[string]any, 0, len(records))
	for _, input := range records {
		requestID := input.RequestID
		if requestID == "" {
			requestID = newUUID()
		}
		wire = append(wire, encodeAppendRequest(
			requestID, w.writer, w.sessionEpoch,
			input.Key, input.Payload, input.EventTimeNs, input.Metadata))
	}
	raw, err := w.transport.post(ctx, "/v1/writers/append-batch",
		map[string]any{"records": wire})
	if err != nil {
		return AppendBatchResult{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return AppendBatchResult{}, err
	}
	var results []AppendResult
	if array, ok := body["results"].([]any); ok {
		for _, item := range array {
			if object, ok := item.(map[string]any); ok {
				results = append(results, decodeAppendResult(object))
			}
		}
	}
	feedback, _ := body["feedback"].(map[string]any)
	return AppendBatchResult{Results: results, Feedback: decodeFeedback(feedback)}, nil
}
