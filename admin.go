package whitewater

import (
	"context"
	"encoding/json"
)

// AdminClient executes WCL scripts and reports Node health.
type AdminClient struct {
	transport *transport
}

// WCL executes script with a fresh request id.
func (a *AdminClient) WCL(ctx context.Context, script string) (ControlExecution, error) {
	return a.WCLWithID(ctx, script, newUUID())
}

// WCLWithID executes script under a caller-chosen request id; a retry
// with the same id deduplicates on the server.
func (a *AdminClient) WCLWithID(
	ctx context.Context, script, requestID string,
) (ControlExecution, error) {
	raw, err := a.transport.post(ctx, "/v1/admin/wcl", map[string]any{
		"request_id": requestID,
		"script":     script,
	})
	if err != nil {
		return ControlExecution{}, err
	}
	return a.decodeExecution(raw)
}

// Commands executes typed control commands, e.g.
// [{"command": "show", "kind": "feeds"}].
func (a *AdminClient) Commands(
	ctx context.Context, commands []map[string]any,
) (ControlExecution, error) {
	raw, err := a.transport.post(ctx, "/v1/admin/commands", map[string]any{
		"request_id": newUUID(),
		"commands":   commands,
	})
	if err != nil {
		return ControlExecution{}, err
	}
	return a.decodeExecution(raw)
}

func (a *AdminClient) decodeExecution(raw json.RawMessage) (ControlExecution, error) {
	body, err := decodeBody(raw)
	if err != nil {
		return ControlExecution{}, err
	}
	var results []StatementResult
	if array, ok := body["results"].([]any); ok {
		for _, item := range array {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			var data json.RawMessage
			if payload, present := object["data"]; present {
				data, _ = json.Marshal(payload)
			}
			results = append(results, StatementResult{
				Statement: stringField(object, "statement"),
				Message:   stringField(object, "message"),
				Data:      data,
			})
		}
	}
	return ControlExecution{
		RequestID: stringField(body, "request_id"),
		Revision:  intField(body, "revision"),
		Authority: stringField(body, "authority"),
		Warning:   stringField(body, "warning"),
		Results:   results,
	}, nil
}

// Healthy reports the contacted Node's liveness.
func (a *AdminClient) Healthy(ctx context.Context) (bool, error) {
	raw, err := a.transport.get(ctx, "/health")
	if err != nil {
		return false, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return false, err
	}
	return stringField(body, "status") == "ok", nil
}

