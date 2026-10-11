package whitewater

import (
	"context"
	"net/url"
	"strconv"
)

// FeedClient covers ad-hoc Feed operations: committed record reads,
// anonymous temporary fetches, and single-shot appends with a
// caller-supplied writer identity — no named session required.
type FeedClient struct {
	transport *transport
}

// Records returns committed record history via GET /v1/feeds/records.
func (f *FeedClient) Records(
	ctx context.Context, feed string, limit int, after *Cursor,
) ([]Record, error) {
	path := "/v1/feeds/records?feed=" + url.QueryEscape(feed)
	if limit > 0 {
		path += "&limit=" + strconv.Itoa(limit)
	}
	if after != nil {
		path += "&after=" + url.QueryEscape(string(*after))
	}
	raw, err := f.transport.get(ctx, path)
	if err != nil {
		return nil, err
	}
	body, err := decodeBodyList(raw)
	if err != nil {
		return nil, err
	}
	return decodeRecords(body), nil
}

// TemporaryFetch issues an anonymous temporary Reader fetch: a bounded
// page after a Cursor, NewOnly positioning, or Tail + WaitMs long-poll.
func (f *FeedClient) TemporaryFetch(
	ctx context.Context, feed string, options TemporaryFetchOptions,
) (TemporaryFetchPage, error) {
	body := map[string]any{"feed": feed}
	if options.After != nil {
		body["after"] = string(*options.After)
	}
	if options.Limit > 0 {
		body["limit"] = options.Limit
	}
	if options.NewOnly {
		body["new_only"] = true
	}
	if options.Tail {
		body["tail"] = true
	}
	if options.WaitMs > 0 {
		body["wait_ms"] = options.WaitMs
	}
	if options.AfterEventTimeNs != nil {
		body["after_event_time_ns"] = strconv.FormatInt(*options.AfterEventTimeNs, 10)
	}
	raw, err := f.transport.post(ctx, "/v1/readers/temporary/fetch", body)
	if err != nil {
		return TemporaryFetchPage{}, err
	}
	decoded, err := decodeBody(raw)
	if err != nil {
		return TemporaryFetchPage{}, err
	}
	return decodeTemporaryFetchPage(decoded), nil
}

// Append issues an ad-hoc append; identical RequestID retries dedupe.
func (f *FeedClient) Append(
	ctx context.Context, feed string, record FeedAppend,
) (AppendResult, error) {
	if record.RequestID == "" {
		record.RequestID = newUUID()
	}
	raw, err := f.transport.post(
		ctx, "/v1/feeds/append", encodeFeedAppendRequest(feed, record))
	if err != nil {
		return AppendResult{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return AppendResult{}, err
	}
	return decodeAppendResult(body), nil
}
