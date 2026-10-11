package whitewater

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

// Client is the entry point: resource management (WCL, typed commands,
// health) plus the Writer, Reader, Subscription-member, and Feed facades.
type Client struct {
	transport *transport
	Feeds     *FeedClient
}

// NewClient builds a client against one or more Node endpoints.
// Requests round-robin; a non-2xx maps to a typed *Error.
func NewClient(apiKey string, endpoints []string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	transport := newTransport(endpoints, apiKey, timeout)
	return &Client{
		transport: transport,
		Feeds:     &FeedClient{transport: transport},
	}
}

// Writer binds a named-Writer session at sessionEpoch (from WCL
// OPEN WRITER SESSION).
func (c *Client) Writer(name string, sessionEpoch int64) *WriterSessionClient {
	return &WriterSessionClient{
		transport: c.transport, writer: name, sessionEpoch: sessionEpoch,
	}
}

// Reader binds a named Reader with its independent durable cursor.
func (c *Client) Reader(name string) *ReaderSessionClient {
	return &ReaderSessionClient{transport: c.transport, reader: name}
}

// SubscriptionMember binds a cooperating member of a named Subscription.
func (c *Client) SubscriptionMember(subscription string) *SubscriptionMemberClient {
	return &SubscriptionMemberClient{transport: c.transport, subscription: subscription}
}

// newUUID generates a random RFC 4122 UUIDv4 — zero-dependency because
// the SDK takes no external packages.
func newUUID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(fmt.Sprintf("uuid: %v", err))
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

// WCL executes script with a fresh request id.
func (c *Client) WCL(ctx context.Context, script string) (ControlExecution, error) {
	return c.WCLWithID(ctx, script, newUUID())
}

// WCLWithID executes script under a caller-chosen request id; a retry
// with the same id deduplicates on the server.
func (c *Client) WCLWithID(
	ctx context.Context, script, requestID string,
) (ControlExecution, error) {
	raw, err := c.transport.post(ctx, "/v1/admin/wcl", map[string]any{
		"request_id": requestID,
		"script":     script,
	})
	if err != nil {
		return ControlExecution{}, err
	}
	return c.decodeExecution(raw)
}

// Commands executes typed control commands, e.g.
// [{"command": "show", "kind": "feeds"}].
func (c *Client) Commands(
	ctx context.Context, commands []map[string]any,
) (ControlExecution, error) {
	raw, err := c.transport.post(ctx, "/v1/admin/commands", map[string]any{
		"request_id": newUUID(),
		"commands":   commands,
	})
	if err != nil {
		return ControlExecution{}, err
	}
	return c.decodeExecution(raw)
}

func (c *Client) decodeExecution(raw json.RawMessage) (ControlExecution, error) {
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
func (c *Client) Healthy(ctx context.Context) (bool, error) {
	raw, err := c.transport.get(ctx, "/health")
	if err != nil {
		return false, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return false, err
	}
	return stringField(body, "status") == "ok", nil
}
