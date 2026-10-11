package whitewater

import "context"

// ReaderSessionClient is one named Reader: open/fetch/ack/close with a
// durable cursor. Delivery is at-least-once; ack a page's delivered
// cursor to commit progress.
type ReaderSessionClient struct {
	transport *transport
	reader    string
}

// Open resumes the Reader's durable cursor under a new session epoch.
func (r *ReaderSessionClient) Open(
	ctx context.Context, capacity int,
) (ReaderSession, error) {
	raw, err := r.transport.post(ctx, "/v1/readers/open", map[string]any{
		"request_id": newUUID(),
		"reader":     r.reader,
		"capacity":   capacity,
	})
	if err != nil {
		return ReaderSession{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return ReaderSession{}, err
	}
	return decodeReaderSession(body), nil
}

// Fetch returns up to limit records after the delivered cursor.
func (r *ReaderSessionClient) Fetch(
	ctx context.Context, session ReaderSession, limit int,
) (ReaderPage, error) {
	body := map[string]any{
		"request_id":    newUUID(),
		"reader":        r.reader,
		"session_epoch": session.SessionEpoch,
	}
	if limit > 0 {
		body["limit"] = limit
	}
	raw, err := r.transport.post(ctx, "/v1/readers/fetch", body)
	if err != nil {
		return ReaderPage{}, err
	}
	decoded, err := decodeBody(raw)
	if err != nil {
		return ReaderPage{}, err
	}
	return decodeReaderPage(decoded), nil
}

// Ack durably commits progress at a page's delivered cursor.
func (r *ReaderSessionClient) Ack(
	ctx context.Context, session ReaderSession, cursor Cursor,
) (ReaderSession, error) {
	raw, err := r.transport.post(ctx, "/v1/readers/ack", map[string]any{
		"request_id":    newUUID(),
		"reader":        r.reader,
		"session_epoch": session.SessionEpoch,
		"cursor":        string(cursor),
	})
	if err != nil {
		return ReaderSession{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return ReaderSession{}, err
	}
	return decodeReaderSession(body), nil
}

// Close ends the session epoch.
func (r *ReaderSessionClient) Close(
	ctx context.Context, session ReaderSession,
) (ReaderSession, error) {
	raw, err := r.transport.post(ctx, "/v1/readers/close", map[string]any{
		"request_id":    newUUID(),
		"reader":        r.reader,
		"session_epoch": session.SessionEpoch,
	})
	if err != nil {
		return ReaderSession{}, err
	}
	body, err := decodeBody(raw)
	if err != nil {
		return ReaderSession{}, err
	}
	return decodeReaderSession(body), nil
}
