// Package testkit provides an in-process Riverbed double for unit
// tests of code built on the whitewater client. It serves the real v1
// wire protocol on loopback HTTP — append/fetch/ack delivery,
// request_id dedup, session-epoch fencing, and StateStore semantics —
// so application tests exercise the same request/response shapes a
// live Riverbed returns without needing one:
//
//	fake := testkit.NewRiverbed(t)
//	defer fake.Close()
//	client := whitewater.NewClient("key", []string{fake.Endpoints()}, time.Second)
//
// The double is a single-Node semantic model: it stores committed
// records, fences stale writer/reader sessions, deduplicates
// idempotent writes by request identity, and redelivers unacknowledged
// pages. It is not a quorum, keeps nothing on disk, and ignores the
// bearer key. Paths outside the modeled surface answer the protocol's
// error envelope with status 404, which the client maps to Refused.
package testkit

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
)

// Riverbed is the fake Node: one httptest.Server plus the in-memory
// catalog, feed logs, and store rows behind it. Construct it with
// NewRiverbed; it is safe for concurrent client use.
type Riverbed struct {
	server *httptest.Server

	mu       sync.Mutex
	revision int64
	epochs   int64
	cursors  int64

	domains       map[string]bool
	feeds         map[string]*fakeFeed
	writers       map[string]*fakeWriter
	readers       map[string]*fakeReader
	subscriptions map[string]*fakeSubscription
	stores        map[string]*fakeStore
	tombstones    map[string]bool // dropped store names refuse everything
	pipes         map[string]*fakePipe
	roles         map[string]bool
}

// NewRiverbed starts a fake Riverbed on a loopback httptest.Server and
// returns it. The server closes on t.Cleanup; Close may also be
// deferred explicitly.
func NewRiverbed(t testing.TB) *Riverbed {
	t.Helper()
	r := &Riverbed{
		domains:       map[string]bool{},
		feeds:         map[string]*fakeFeed{},
		writers:       map[string]*fakeWriter{},
		readers:       map[string]*fakeReader{},
		subscriptions: map[string]*fakeSubscription{},
		stores:        map[string]*fakeStore{},
		tombstones:    map[string]bool{},
		pipes:         map[string]*fakePipe{},
		roles:         map[string]bool{},
	}
	r.server = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.Close)
	return r
}

// Endpoints returns the fake Node's base URL — feed it to
// NewClient's endpoints slice (one URL per fake when a test models
// failover).
func (r *Riverbed) Endpoints() string { return r.server.URL }

// Client returns a whitewater.Client bound to this fake — the quick
// path when a test does not care about the api key or timeout.
func (r *Riverbed) Client() *whitewater.Client {
	return whitewater.NewClient("testkit-key", []string{r.server.URL}, 30*time.Second)
}

// Close shuts the loopback server down. It is idempotent.
func (r *Riverbed) Close() {
	if r.server != nil {
		r.server.Close()
	}
}

// ---------------------------------------------------------------------------
// In-memory model
// ---------------------------------------------------------------------------

// fakeFeed is one committed record log plus its ad-hoc writer lanes.
type fakeFeed struct {
	id       string
	name     string
	records  []*fakeRecord
	byCursor map[string]int // cursor -> records index
	adHoc    map[string]*adHocWriter
}

// adHocWriter tracks one writer_session_id on /v1/feeds/append: its
// last committed epoch/sequence fence and request_id dedup table.
type adHocWriter struct {
	epoch   int64
	lastSeq int64
	dedup   map[string]*appendDedup
}

// fakeRecord is one committed record; binary fields stay base64 as on
// the wire.
type fakeRecord struct {
	cursor       string
	messageID    string
	producerID   string
	sequence     int64
	eventTimeNs  int64
	ingestTimeNs int64
	keyBase64    string
	hasKey       bool
	payloadB64   string
	metadataB64  map[string]string
}

// wire renders the Record document the SDK decodes; nanoseconds are
// decimal strings and a keyless record omits key_base64.
func (r *fakeRecord) wire() map[string]any {
	body := map[string]any{
		"cursor":          r.cursor,
		"message_id":      r.messageID,
		"producer_id":     r.producerID,
		"sequence":        r.sequence,
		"event_time_ns":   strconv.FormatInt(r.eventTimeNs, 10),
		"ingest_time_ns":  strconv.FormatInt(r.ingestTimeNs, 10),
		"payload_base64":  r.payloadB64,
		"metadata_base64": r.metadataB64,
	}
	if r.hasKey {
		body["key_base64"] = r.keyBase64
	}
	return body
}

// appendDedup remembers one committed request_id: the content
// fingerprint detects a conflicting reuse; messageID/cursor replay the
// original result on an identical retry.
type appendDedup struct {
	fingerprint string
	messageID   string
	cursor      string
}

// fakeWriter is a declared named Writer bound to a Feed. epoch is the
// current OPEN WRITER SESSION fencing token; dedup survives reopens.
type fakeWriter struct {
	id    string
	feed  string
	epoch int64
	seq   int64
	dedup map[string]*appendDedup
}

// fakeReader is a declared named Reader: durable acknowledged progress
// plus the session-scoped delivered frontier fenced by epoch.
type fakeReader struct {
	id           string
	feed         string
	epoch        int64
	capacity     int64
	open         bool
	delivered    int // records[0:delivered] handed to the live session
	acknowledged int // records[0:acknowledged] durably committed
}

// fakeSubscription is declaration-only bookkeeping: name -> source feed.
type fakeSubscription struct {
	id   string
	feed string
}

// fakeStore is one declared StateStore: primary rows plus the named
// key declarations get-by resolves against.
type fakeStore struct {
	id        string
	name      string
	manual    bool
	namedKeys map[string]bool // name -> unique
	rows      map[string]*fakeRow
	seq       int64
	mutations map[string]*mutationDedup
}

// fakeRow is one primary row: payload plus the named values its last
// put_row stamped; appliedSeq is the journal position it landed at.
type fakeRow struct {
	payloadB64 string
	named      map[string]any // named key -> raw wire values list
	appliedSeq int64
}

// mutationDedup replays a committed mutation_id and fences a reused id
// carrying different content.
type mutationDedup struct {
	fingerprint string
	sequence    int64
}

// fakePipe is one declared Pipe for describe/show/lifecycle.
type fakePipe struct {
	name         string
	subscription string
	operation    any
	outputFeed   string
	outputStore  string
	stage        string
	createdAtNs  int64
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

// serve routes the v1 surface; anything else gets the 404 envelope the
// SDK decodes as Refused.
func (r *Riverbed) serve(w http.ResponseWriter, req *http.Request) {
	body, err := requestBody(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "refused", "malformed JSON body: %v", err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case req.Method == http.MethodGet && req.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "ok",
			"version":  "testkit-1.0.0",
			"node_id":  "testkit-node-1",
			"members":  1,
			"protocol": map[string]any{"major": 1, "minor": 0},
			"limits": map[string]any{
				"max_frame_bytes":                     67108864,
				"max_committed_read_bytes":            67108864,
				"max_logical_read_frames":             1024,
				"max_logical_read_ranges":             64,
				"subscription_member_fetch_max_limit": 256,
				"subscription_member_max_lease_ticks": 600,
			},
		})
	case req.URL.Path == "/v1/admin/wcl":
		r.wcl(w, body)
	case req.URL.Path == "/v1/admin/commands":
		r.commands(w, body)
	case req.URL.Path == "/v1/writers/append":
		r.writerAppend(w, body)
	case req.URL.Path == "/v1/writers/append-batch":
		r.writerAppendBatch(w, body)
	case req.URL.Path == "/v1/feeds/append":
		r.feedAppend(w, body)
	case req.Method == http.MethodGet && req.URL.Path == "/v1/feeds/records":
		r.feedRecords(w, req)
	case req.URL.Path == "/v1/readers/open":
		r.readerOpen(w, body)
	case req.URL.Path == "/v1/readers/fetch":
		r.readerFetch(w, body)
	case req.URL.Path == "/v1/readers/ack":
		r.readerAck(w, body)
	case req.URL.Path == "/v1/readers/close":
		r.readerClose(w, body)
	case req.URL.Path == "/v1/readers/temporary/fetch":
		r.temporaryFetch(w, body)
	case req.URL.Path == "/v1/state-stores/mutate":
		r.storeMutate(w, body)
	case req.URL.Path == "/v1/state-stores/lookup":
		r.storeLookup(w, body)
	case req.URL.Path == "/v1/state-stores/get-by":
		r.storeGetBy(w, body)
	case req.URL.Path == "/v1/state-stores/iterate":
		r.storeIterate(w, body)
	case req.URL.Path == "/v1/state-stores/clear":
		r.storeClear(w, body)
	default:
		writeError(w, http.StatusNotFound, "",
			"testkit: no route for %s %s", req.Method, req.URL.Path)
	}
}

// requestBody decodes the JSON object every POST carries; GETs have no
// body and yield an empty map.
func requestBody(req *http.Request) (map[string]any, error) {
	out := map[string]any{}
	if req.Method == http.MethodGet || req.Body == nil {
		return out, nil
	}
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		raw = []byte(`{"error": "testkit: response encode failed"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

// writeError emits the protocol error envelope; code stays absent when
// the failure is not machine-actionable.
func writeError(w http.ResponseWriter, status int, code, format string, args ...any) {
	body := map[string]any{"error": fmt.Sprintf(format, args...)}
	if code != "" {
		body["code"] = code
	}
	writeJSON(w, status, body)
}

// refused is the 400 path: fix the request before retrying.
func refused(w http.ResponseWriter, format string, args ...any) {
	writeError(w, http.StatusBadRequest, "refused", format, args...)
}

// staleEpoch is the 409 fencing path the SDK maps to KindStaleSession.
func staleEpoch(w http.ResponseWriter, format string, args ...any) {
	writeError(w, http.StatusConflict, "stale_epoch", format, args...)
}

// conflict is the generic 409 fencing path the SDK maps to KindConflict.
func conflict(w http.ResponseWriter, code, format string, args ...any) {
	writeError(w, http.StatusConflict, code, format, args...)
}

// ---------------------------------------------------------------------------
// Scalar helpers
// ---------------------------------------------------------------------------

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

// mintCursor issues the next opaque cursor — hex-padded so byte order
// reads like a hash, never a parseable position.
func (r *Riverbed) mintCursor() string {
	r.cursors++
	return fmt.Sprintf("%016x", r.cursors)
}

// int64Of reads a JSON number or decimal-string field as int64.
func int64Of(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		return parsed
	case int64:
		return v
	}
	return 0
}

func strOf(body map[string]any, name string) string {
	value, _ := body[name].(string)
	return value
}

func boolOf(body map[string]any, name string) bool {
	value, _ := body[name].(bool)
	return value
}

// fingerprint canonicalizes the identity-bearing fields of a request:
// identical retries hash equal, tampered replays do not.
func fingerprint(parts ...any) string {
	sum := sha256.Sum256(canonical(parts))
	return hex.EncodeToString(sum[:])
}

// canonical renders a decoded JSON tree deterministically — encoding/
// json already sorts map keys, which is all the canonicalization the
// dedup fingerprint needs.
func canonical(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte(fmt.Sprintf("%v", value))
	}
	return raw
}

// recordFields extracts the content fields a dedup fingerprint covers
// for both append shapes (writer session and ad-hoc feed).
func recordFields(body map[string]any) []any {
	return []any{
		body["key_base64"],
		body["payload_base64"],
		body["event_time_ns"],
		body["metadata_base64"],
	}
}
