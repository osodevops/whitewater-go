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
