package whitewater

import (
	"crypto/rand"
	"fmt"
	"time"
)

// Client is the entry point: one transport, four capability facades.
type Client struct {
	transport *transport
	Admin     *AdminClient
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
		Admin:     &AdminClient{transport: transport},
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
