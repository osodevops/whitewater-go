package testkit

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Data plane: Writer appends (single + batch), ad-hoc Feed appends,
// committed record reads, named Reader sessions, and anonymous
// temporary fetches. Records commit to the feed's log once; cursors
// are opaque minted strings; dedup and epoch fencing behave like the
// protocol contract.

// appendReply is one committed (or replayed) append result.
type appendReply struct {
	messageID    string
	cursor       string
	deduplicated bool
}

// wireError is a refusal/conflict result the commit path returns
// instead of writing the response itself.
type wireError struct {
	status int
	code   string
	msg    string
}

func (e *wireError) write(w http.ResponseWriter) {
	writeError(w, e.status, e.code, "%s", e.msg)
}

func wireErr(status int, code, format string, args ...any) *wireError {
	return &wireError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

// writerFeedback is the canned ServerFeedback every append answers.
var writerFeedback = map[string]any{
	"recommended_batch_count": 64,
	"recommended_batch_bytes": 1048576,
	"pressure":                0.0,
	"retry_after_ms":          0,
	"max_frame_bytes":         67108864,
	"hot_key":                 false,
}

// appendResultBody renders the WriterAppendResponse document.
func appendResultBody(result *appendReply) map[string]any {
	return map[string]any{
		"message_id":   result.messageID,
		"cursor":       result.cursor,
		"deduplicated": result.deduplicated,
		"feedback":     writerFeedback,
	}
}

// dedupReplay resolves request_id against a dedup table: the stored
// result replays on an identical retry, tampered reuse conflicts, and
// an unseen id returns nil/nil so the caller commits.
func dedupReplay(
	dedup map[string]*appendDedup, requestID, fp string,
) (*appendReply, *wireError) {
	seen := dedup[requestID]
	if seen == nil {
		return nil, nil
	}
	if seen.fingerprint == fp {
		return &appendReply{
			messageID: seen.messageID, cursor: seen.cursor, deduplicated: true,
		}, nil
	}
	return nil, wireErr(http.StatusConflict, "request_id.conflict",
		"request_id %q reused with different content", requestID)
}

// commitAppend applies one append: request_id dedup first, then the
// record commits to the feed log and the dedup table remembers it.
// seqCounter is the producer's per-identity ordinal when the wire body
// does not pin a sequence (named Writers); nil takes body["sequence"].
func (r *Riverbed) commitAppend(
	feed *fakeFeed, producerID string, seqCounter *int64,
	dedup map[string]*appendDedup, body map[string]any,
) (*appendReply, *wireError) {
	requestID := strOf(body, "request_id")
	if requestID == "" {
		return nil, wireErr(http.StatusBadRequest, "refused", "request_id is required")
	}
	fp := fingerprint(recordFields(body))
	if reply, err := dedupReplay(dedup, requestID, fp); reply != nil || err != nil {
		return reply, err
	}
	var sequence int64
	if seqCounter != nil {
		*seqCounter++
		sequence = *seqCounter
	} else {
		sequence = int64Of(body["sequence"])
	}
	record := r.storeRecord(feed, producerID, sequence, body)
	dedup[requestID] = &appendDedup{
		fingerprint: fp, messageID: record.messageID, cursor: record.cursor}
	return &appendReply{messageID: record.messageID, cursor: record.cursor}, nil
}

// storeRecord mints cursor/message_id, stamps ingest time, and commits
// the record to the feed log.
func (r *Riverbed) storeRecord(
	feed *fakeFeed, producerID string, sequence int64, body map[string]any,
) *fakeRecord {
	now := time.Now().UnixNano()
	eventTime := now
	if raw, present := body["event_time_ns"]; present {
		eventTime = int64Of(raw)
	}
	metadata := map[string]string{}
	if raw, ok := body["metadata_base64"].(map[string]any); ok {
		for name, value := range raw {
			if encoded, ok := value.(string); ok {
				metadata[name] = encoded
			}
		}
	}
	record := &fakeRecord{
		cursor:       r.mintCursor(),
		messageID:    newUUID(),
		producerID:   producerID,
		sequence:     sequence,
		eventTimeNs:  eventTime,
		ingestTimeNs: now,
		payloadB64:   strOf(body, "payload_base64"),
		metadataB64:  metadata,
	}
	if key, present := body["key_base64"]; present {
		if encoded, ok := key.(string); ok && encoded != "" {
			record.keyBase64 = encoded
			record.hasKey = true
		}
	}
	feed.byCursor[record.cursor] = len(feed.records)
	feed.records = append(feed.records, record)
	return record
}

// writerAppend handles POST /v1/writers/append: one epoch-fenced
// idempotent append through a named Writer session.
func (r *Riverbed) writerAppend(w http.ResponseWriter, body map[string]any) {
	writer := r.writers[strOf(body, "writer")]
	if writer == nil {
		refused(w, "writer %q not found", strOf(body, "writer"))
		return
	}
	if writer.epoch == 0 || int64Of(body["session_epoch"]) != writer.epoch {
		staleEpoch(w, "writer %q session epoch was superseded by epoch %d",
			strOf(body, "writer"), writer.epoch)
		return
	}
	feed := r.feeds[writer.feed]
	if feed == nil {
		refused(w, "feed %q not found", writer.feed)
		return
	}
	reply, err := r.commitAppend(feed, writer.id, &writer.seq, writer.dedup, body)
	if err != nil {
		err.write(w)
		return
	}
	writeJSON(w, http.StatusOK, appendResultBody(reply))
}

// writerAppendBatch handles POST /v1/writers/append-batch: each record
// commits independently under its own request_id; the first refusal
// fails the request.
func (r *Riverbed) writerAppendBatch(w http.ResponseWriter, body map[string]any) {
	records, _ := body["records"].([]any)
	if len(records) == 0 {
		refused(w, "append-batch carries no records")
		return
	}
	outcomes := make([]any, 0, len(records))
	for _, item := range records {
		record, ok := item.(map[string]any)
		if !ok {
			refused(w, "batch record is not an object")
			return
		}
		writer := r.writers[strOf(record, "writer")]
		if writer == nil {
			refused(w, "writer %q not found", strOf(record, "writer"))
			return
		}
		if writer.epoch == 0 || int64Of(record["session_epoch"]) != writer.epoch {
			staleEpoch(w, "writer %q session epoch was superseded",
				strOf(record, "writer"))
			return
		}
		feed := r.feeds[writer.feed]
		if feed == nil {
			refused(w, "feed %q not found", writer.feed)
			return
		}
		reply, err := r.commitAppend(feed, writer.id, &writer.seq, writer.dedup, record)
		if err != nil {
			err.write(w)
			return
		}
		outcomes = append(outcomes, appendResultBody(reply))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"results":  outcomes,
		"feedback": writerFeedback,
	})
}

// feedAppend handles POST /v1/feeds/append: the caller-owned
// (writer_session_id, writer_epoch, sequence) identity fences ordering
// — a stale sequence is a 409, never healed server-side.
func (r *Riverbed) feedAppend(w http.ResponseWriter, body map[string]any) {
	feed := r.feeds[strOf(body, "feed")]
	if feed == nil {
		refused(w, "feed %q not found", strOf(body, "feed"))
		return
	}
	sessionID := strOf(body, "writer_session_id")
	if sessionID == "" {
		refused(w, "writer_session_id is required")
		return
	}
	lane := feed.adHoc[sessionID]
	if lane == nil {
		lane = &adHocWriter{dedup: map[string]*appendDedup{}}
		feed.adHoc[sessionID] = lane
	}
	requestID := strOf(body, "request_id")
	fp := fingerprint(recordFields(body))
	if reply, err := dedupReplay(lane.dedup, requestID, fp); reply != nil || err != nil {
		if err != nil {
			err.write(w)
		} else {
			writeJSON(w, http.StatusOK, appendResultBody(reply))
		}
		return
	}
	epoch := int64Of(body["writer_epoch"])
	if epoch < lane.epoch {
		staleEpoch(w, "writer epoch %d was superseded by %d", epoch, lane.epoch)
		return
	}
	if epoch > lane.epoch {
		lane.epoch = epoch
		lane.lastSeq = 0
	}
	sequence := int64Of(body["sequence"])
	if sequence <= lane.lastSeq {
		conflict(w, "writer.stale_sequence",
			"sequence %d is not after committed %d", sequence, lane.lastSeq)
		return
	}
	record := r.storeRecord(feed, sessionID, sequence, body)
	lane.lastSeq = sequence
	lane.dedup[requestID] = &appendDedup{
		fingerprint: fp, messageID: record.messageID, cursor: record.cursor}
	writeJSON(w, http.StatusOK, appendResultBody(
		&appendReply{messageID: record.messageID, cursor: record.cursor}))
}

// feedRecords handles GET /v1/feeds/records?feed&limit&after — the
// committed-history read.
func (r *Riverbed) feedRecords(w http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	feed := r.feeds[query.Get("feed")]
	if feed == nil {
		refused(w, "feed %q not found", query.Get("feed"))
		return
	}
	start := 0
	if after := query.Get("after"); after != "" {
		idx, ok := feed.byCursor[after]
		if !ok {
			refused(w, "unknown cursor %q", after)
			return
		}
		start = idx + 1
	}
	limit, _ := strconv.Atoi(query.Get("limit"))
	if limit <= 0 {
		limit = 256
	}
	end := start + limit
	if end > len(feed.records) {
		end = len(feed.records)
	}
	records := make([]any, 0, end-start)
	for _, record := range feed.records[start:end] {
		records = append(records, record.wire())
	}
	writeJSON(w, http.StatusOK, records)
}

// ---------------------------------------------------------------------------
// Named Reader sessions
// ---------------------------------------------------------------------------

// readerOpen handles POST /v1/readers/open: a new epoch mints, the
// delivered frontier resets to durable acknowledged progress — an
// unacked page redelivers under the new session.
func (r *Riverbed) readerOpen(w http.ResponseWriter, body map[string]any) {
	reader := r.readers[strOf(body, "reader")]
	if reader == nil {
		refused(w, "reader %q not found", strOf(body, "reader"))
		return
	}
	r.epochs++
	reader.epoch = r.epochs
	reader.open = true
	reader.delivered = reader.acknowledged
	capacity := int64Of(body["capacity"])
	if capacity <= 0 {
		capacity = 128
	}
	reader.capacity = capacity
	writeJSON(w, http.StatusOK, r.sessionBody(reader))
}

// readerSession validates the (reader, session_epoch) fence shared by
// fetch/ack; a stale or closed session answers 409 stale_epoch.
func (r *Riverbed) readerSession(w http.ResponseWriter, body map[string]any) *fakeReader {
	reader := r.readers[strOf(body, "reader")]
	if reader == nil {
		refused(w, "reader %q not found", strOf(body, "reader"))
		return nil
	}
	if !reader.open || int64Of(body["session_epoch"]) != reader.epoch {
		staleEpoch(w, "reader %q session epoch was superseded", strOf(body, "reader"))
		return nil
	}
	return reader
}

// readerFetch handles POST /v1/readers/fetch: serve up to limit
// records after the delivered frontier and advance it.
func (r *Riverbed) readerFetch(w http.ResponseWriter, body map[string]any) {
	reader := r.readerSession(w, body)
	if reader == nil {
		return
	}
	feed := r.feeds[reader.feed]
	if feed == nil {
		refused(w, "feed %q not found", reader.feed)
		return
	}
	limit := int64Of(body["limit"])
	if limit <= 0 {
		limit = reader.capacity
	}
	if limit <= 0 {
		limit = 256
	}
	end := reader.delivered + int(limit)
	if end > len(feed.records) {
		end = len(feed.records)
	}
	page := feed.records[reader.delivered:end]
	reader.delivered = end
	records := make([]any, 0, len(page))
	for _, record := range page {
		records = append(records, record.wire())
	}
	// An empty page carries a null delivered cursor — a cursor the
	// client never received records for must not be ackable.
	var delivered any
	if len(page) > 0 {
		delivered = page[len(page)-1].cursor
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records":              records,
		"delivered_cursor":     delivered,
		"acknowledged_cursor":  r.cursorAt(feed, reader.acknowledged),
		"recommended_capacity": reader.capacity,
		"retry_after_ms":       0,
	})
}

// readerAck handles POST /v1/readers/ack: the page's delivered cursor
// commits as durable progress.
func (r *Riverbed) readerAck(w http.ResponseWriter, body map[string]any) {
	reader := r.readerSession(w, body)
	if reader == nil {
		return
	}
	feed := r.feeds[reader.feed]
	if feed == nil {
		refused(w, "feed %q not found", reader.feed)
		return
	}
	cursor := strOf(body, "cursor")
	idx, ok := feed.byCursor[cursor]
	if !ok {
		refused(w, "unknown cursor %q", cursor)
		return
	}
	if idx+1 > reader.delivered {
		refused(w, "ack cursor %q is beyond the delivered frontier", cursor)
		return
	}
	// The acknowledged frontier only advances; re-acking an older
	// cursor is idempotent, never a rewind.
	if idx+1 > reader.acknowledged {
		reader.acknowledged = idx + 1
	}
	writeJSON(w, http.StatusOK, r.sessionBody(reader))
}

// readerClose handles POST /v1/readers/close: the session releases and
// delivered work rewinds to acknowledged — unacked pages redeliver.
func (r *Riverbed) readerClose(w http.ResponseWriter, body map[string]any) {
	reader := r.readers[strOf(body, "reader")]
	if reader == nil {
		refused(w, "reader %q not found", strOf(body, "reader"))
		return
	}
	if int64Of(body["session_epoch"]) != reader.epoch {
		staleEpoch(w, "reader %q session epoch was superseded", strOf(body, "reader"))
		return
	}
	reader.open = false
	reader.delivered = reader.acknowledged
	writeJSON(w, http.StatusOK, r.sessionBody(reader))
}

// sessionBody renders the ReaderSession response document.
func (r *Riverbed) sessionBody(reader *fakeReader) map[string]any {
	feed := r.feeds[reader.feed]
	body := map[string]any{
		"reader_id":           reader.id,
		"session_epoch":       reader.epoch,
		"capacity":            reader.capacity,
		"delivered_cursor":    r.cursorAt(feed, reader.delivered),
		"acknowledged_cursor": r.cursorAt(feed, reader.acknowledged),
	}
	if feed != nil {
		body["feed_id"] = feed.id
	}
	return body
}

// cursorAt maps a record position back to its opaque cursor — nil at
// the feed's beginning.
func (r *Riverbed) cursorAt(feed *fakeFeed, position int) any {
	if feed == nil || position <= 0 || position > len(feed.records) {
		return nil
	}
	return feed.records[position-1].cursor
}

// ---------------------------------------------------------------------------
// Temporary fetches
// ---------------------------------------------------------------------------

// temporaryFetch handles POST /v1/readers/temporary/fetch: anonymous
// bounded read with after/new_only/event-time positioning — no durable
// progress is retained.
func (r *Riverbed) temporaryFetch(w http.ResponseWriter, body map[string]any) {
	feed := r.feeds[strOf(body, "feed")]
	if feed == nil {
		refused(w, "feed %q not found", strOf(body, "feed"))
		return
	}
	start := 0
	switch {
	case strOf(body, "after") != "":
		idx, ok := feed.byCursor[strOf(body, "after")]
		if !ok {
			refused(w, "unknown cursor %q", strOf(body, "after"))
			return
		}
		start = idx + 1
	case strOf(body, "after_event_time_ns") != "":
		start = firstAfterEventTime(feed, int64Of(body["after_event_time_ns"]))
	case boolOf(body, "new_only"):
		start = len(feed.records)
	}
	limit := int64Of(body["limit"])
	if limit <= 0 {
		limit = 256
	}
	end := start + int(limit)
	if end > len(feed.records) {
		end = len(feed.records)
	}
	page := feed.records[start:end]
	records := make([]any, 0, len(page))
	for _, record := range page {
		records = append(records, record.wire())
	}
	// The continuation frontier: the page's last record, or the
	// position the page started at when it came back empty.
	var next any
	if len(page) > 0 {
		next = page[len(page)-1].cursor
	} else {
		next = r.cursorAt(feed, start)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records":     records,
		"next_cursor": next,
	})
}
