package whitewater

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// transport round-robins the configured Node endpoints, attaches the
// bearer key, and maps (status, code) pairs to typed *Error values.
type transport struct {
	endpoints []string
	client    *http.Client
	apiKey    string
	next      atomic.Uint32
}

func newTransport(endpoints []string, apiKey string, timeout time.Duration) *transport {
	trimmed := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		trimmed = append(trimmed, strings.TrimRight(endpoint, "/"))
	}
	return &transport{
		endpoints: trimmed,
		apiKey:    apiKey,
		client:    &http.Client{Timeout: timeout},
	}
}

func (t *transport) endpoint() string {
	return t.endpoints[int(t.next.Add(1))%len(t.endpoints)]
}

func (t *transport) get(ctx context.Context, path string) (json.RawMessage, error) {
	return t.roundTrip(ctx, http.MethodGet, path, nil)
}

func (t *transport) post(ctx context.Context, path string, body any) (json.RawMessage, error) {
	return t.roundTrip(ctx, http.MethodPost, path, body)
}

func (t *transport) roundTrip(
	ctx context.Context, method, path string, body any,
) (json.RawMessage, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(
		ctx, method, t.endpoint()+path, reader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := t.client.Do(request)
	if err != nil {
		return nil, errorOf(KindUnavailable, 0, "",
			"Node unreachable: "+err.Error())
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, errorOf(KindUnavailable, 0, "",
			"response read failed: "+err.Error())
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return json.RawMessage(raw), nil
	}
	var object map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &object)
	}
	return nil, mapError(response.StatusCode, object)
}
