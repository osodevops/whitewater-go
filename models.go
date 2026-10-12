package whitewater

import "encoding/json"

// Cursor is an opaque stream position. It is never parsed, ordered, or
// fabricated — pass it back to the Riverbed verbatim.
type Cursor string

// Record is one immutable Feed record. EventTimeNs and IngestTimeNs are
// signed 64-bit nanoseconds; on the wire they travel as decimal strings.
type Record struct {
	Cursor       Cursor
	MessageID    string
	ProducerID   string
	Sequence     int64
	EventTimeNs  int64
	IngestTimeNs int64
	Key          []byte
	Payload      []byte
	Metadata     map[string][]byte
}

// ServerFeedback is backpressure guidance the Riverbed returns on writes.
type ServerFeedback struct {
	RecommendedBatchCount int
	RecommendedBatchBytes int
	Pressure              float64
	RetryAfterMs          int
	MaxFrameBytes         int64
	HotKey                bool
}

// AppendResult is the outcome of one idempotent append.
type AppendResult struct {
	MessageID    string
	Cursor       Cursor
	Deduplicated bool
	Feedback     ServerFeedback
}

// AppendBatchResult pairs per-record results with batch feedback.
type AppendBatchResult struct {
	Results  []AppendResult
	Feedback ServerFeedback
}

// ReaderSession is an opened Reader: sessionEpoch fences every call.
type ReaderSession struct {
	ReaderID           string
	FeedID             string
	SessionEpoch       int64
	Capacity           int
	DeliveredCursor    *Cursor
	AcknowledgedCursor *Cursor
}

// ReaderPage is one fetch: delivered records plus cursor positions.
type ReaderPage struct {
	Records             []Record
	DeliveredCursor     *Cursor
	AcknowledgedCursor  *Cursor
	RecommendedCapacity int
	RetryAfterMs        int
}

// StatementResult is one executed WCL statement. Data is the raw
// per-statement payload, interpreted by callers per statement kind.
type StatementResult struct {
	Statement string
	Message   string
	Data      json.RawMessage
}

// ControlExecution is the committed outcome of a WCL script.
type ControlExecution struct {
	RequestID string
	Revision  int64
	Authority string
	Warning   string
	Results   []StatementResult
}

// Joined is a Subscription membership with its fencing epoch.
type Joined struct {
	MemberID       string
	MemberEpoch    int64
	OwnershipEpoch int64
	Tick           int64
}

// WorkLease is a claimed work item under a bounded lease.
type WorkLease struct {
	WorkID        string
	MemberID      string
	MemberEpoch   int64
	LeaseEpoch    int64
	ExpiresAtTick int64
}

// MemberPage is one member fetch: records plus the frontier to ack.
type MemberPage struct {
	Records        []Record
	Cursor         *Cursor
	Positions      map[string]string
	OwnershipEpoch int64
}

// FeedAppend is one ad-hoc append for POST /v1/feeds/append: the caller
// supplies the writer identity directly instead of holding a named
// Writer session. Identical RequestID retries dedupe server-side.
type FeedAppend struct {
	RequestID       string
	WriterSessionID string
	WriterEpoch     int64
	Sequence        int64
	Key             []byte
	Payload         []byte
	EventTimeNs     *int64
	Metadata        map[string][]byte
}

// TemporaryFetchOptions selects an anonymous temporary Reader fetch.
// After continues after an opaque Cursor; AfterEventTimeNs seeks by
// record event time; NewOnly positions at the current end without
// returning history; Tail plus WaitMs long-polls for newly committed
// records.
type TemporaryFetchOptions struct {
	After            *Cursor
	Limit            int
	NewOnly          bool
	Tail             bool
	WaitMs           int
	AfterEventTimeNs *int64
}

// TemporaryFetchPage is one anonymous fetch: records plus the
// continuation frontier.
type TemporaryFetchPage struct {
	Records    []Record
	NextCursor *Cursor
}

// MaxNamedKeys bounds the named secondary keys one StateStore declares;
// it matches the server declaration budget.
const MaxNamedKeys = 10

// NamedKey is one declared secondary key on a StateStore. Non-unique
// keys map a value to every primary row carrying it (the group/list
// lookup); Unique enforces one row per value, arbitrated
// first-committed-wins by the store's journal order.
type NamedKey struct {
	Name   string
	Unique bool
}

// StoreFreshness is the evidence every StateStore read reports: the
// serving replica's applied and committed journal positions, whether it
// still lags, and which Node served the response.
type StoreFreshness struct {
	AppliedSeq   int64
	CommittedSeq int64
	Behind       bool
	ServedBy     string
}

// StoreRow is one StateStore row: its primary key and payload plus the
// journal cursor it last applied at. Freshness carries the serving
// replica's evidence when the row came from a read.
type StoreRow struct {
	Key           []byte
	Payload       []byte
	AppliedCursor *string
	Freshness     *StoreFreshness
}

// StoreRows is a named-key lookup result: every primary row the value
// points at plus the serving replica's freshness evidence.
type StoreRows struct {
	Rows      []StoreRow
	Freshness StoreFreshness
}

// StorePage is one bounded iteration page in primary-key order.
// NextKey continues the scan; PinnedSeq is the serving replica's
// applied journal position the page was pinned to.
type StorePage struct {
	Rows      []StoreRow
	NextKey   []byte
	PinnedSeq int64
	ServedBy  string
}

// MutationResult is a committed StateStore mutation: the journal
// sequence it landed at and whether MutationID deduplicated a retry.
type MutationResult struct {
	StoreID      string
	MutationID   string
	Sequence     int64
	Deduplicated bool
}
