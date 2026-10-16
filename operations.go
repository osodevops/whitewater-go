package whitewater

// Operational surfaces: Pipe lifecycle, replicated Index queries, and
// demand observability — the same topology-free model as Feeds (names,
// never partitions or Nodes).

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Pipes
// ---------------------------------------------------------------------------

// PipeInfo is the catalog row DESCRIBE PIPE / SHOW PIPES returns for a
// declared Pipe. Operation stays an opaque json.RawMessage — Forward,
// Window, and Enrich variants evolve server-side without an SDK bump.
type PipeInfo struct {
	Name           string          `json:"name"`
	Stage          string          `json:"stage"`
	SubscriptionID string          `json:"subscription_id"`
	Operation      json.RawMessage `json:"operation"`
	OutputFeedID   *string         `json:"output_feed_id"`
	OutputStoreID  *string         `json:"output_store_id"`
	Status         string          `json:"status"`
	CreatedAtNs    int64           `json:"created_at_ns"`
}

// Pipe binds a declared Pipe for lifecycle operations. Pausing is
// cooperative — an in-flight effect finishes and the frontier stays put
// so redelivery stays replay-safe — and resume restarts driving from
// the same durable frontier.
type Pipe struct {
	client *Client
	name   string
}

// Pipe binds a declared Pipe for lifecycle operations:
// Describe/Pause/Resume/Drop.
func (c *Client) Pipe(name string) *Pipe {
	return &Pipe{client: c, name: name}
}

// decodePipeRows accepts every shape the catalog uses: a bare list
// (SHOW PIPES data variants), a {"rows": [...]} envelope, or a single
// row object (DESCRIBE PIPE).
func decodePipeRows(data json.RawMessage) ([]PipeInfo, error) {
	var rows []json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		var wrapped struct {
			Rows []json.RawMessage `json:"rows"`
		}
		if wrapErr := json.Unmarshal(data, &wrapped); wrapErr == nil && wrapped.Rows != nil {
			rows = wrapped.Rows
		} else {
			rows = []json.RawMessage{data}
		}
	}
	infos := make([]PipeInfo, 0, len(rows))
	for _, raw := range rows {
		var info PipeInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, fmt.Errorf("pipe row undecodable: %w", err)
		}
		if info.Name == "" {
			continue
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Describe returns the live catalog row including stage and the opaque
// operation definition (DESCRIBE PIPE).
func (p *Pipe) Describe(ctx context.Context) (PipeInfo, error) {
	execution, err := p.client.Commands(ctx, []map[string]any{{
		"command": "describe", "kind": "pipe", "name": p.name,
	}})
	if err != nil {
		return PipeInfo{}, err
	}
	if len(execution.Results) == 0 {
		return PipeInfo{}, fmt.Errorf("describe pipe returned no result")
	}
	infos, err := decodePipeRows(execution.Results[0].Data)
	if err != nil {
		return PipeInfo{}, err
	}
	if len(infos) == 0 {
		return PipeInfo{}, fmt.Errorf("describe pipe returned no definition")
	}
	return infos[0], nil
}

// Pause halts every drive cooperatively; idempotent (PAUSE PIPE).
func (p *Pipe) Pause(ctx context.Context) error {
	return p.lifecycle(ctx, "pause_pipe")
}

// Resume restarts driving from the durable frontier (RESUME PIPE).
func (p *Pipe) Resume(ctx context.Context) error {
	return p.lifecycle(ctx, "resume_pipe")
}

// Drop retires the declaration and its compute lease (DROP PIPE).
func (p *Pipe) Drop(ctx context.Context) error {
	_, err := p.client.Commands(ctx, []map[string]any{{
		"command": "drop", "kind": "pipe", "name": p.name,
	}})
	return err
}

func (p *Pipe) lifecycle(ctx context.Context, command string) error {
	_, err := p.client.Commands(ctx, []map[string]any{{
		"command": command, "name": p.name,
	}})
	return err
}

// Pipes returns the declared Pipe catalog (SHOW PIPES).
func (c *Client) Pipes(ctx context.Context) ([]PipeInfo, error) {
	execution, err := c.Commands(ctx, []map[string]any{{
		"command": "show", "kind": "pipes",
	}})
	if err != nil {
		return nil, err
	}
	if len(execution.Results) == 0 {
		return nil, nil
	}
	return decodePipeRows(execution.Results[0].Data)
}

// ---------------------------------------------------------------------------
// Index queries
// ---------------------------------------------------------------------------

// IndexBound is an inclusive/exclusive bound for a range query.
type IndexBound struct {
	Value     any
	Inclusive bool
}

// InclusiveBound marks the bound as inclusive.
func InclusiveBound(value any) IndexBound {
	return IndexBound{Value: value, Inclusive: true}
}

// ExclusiveBound marks the bound as exclusive.
func ExclusiveBound(value any) IndexBound {
	return IndexBound{Value: value, Inclusive: false}
}

// IndexPredicate is the query shape QueryIndex lowers onto the wire:
// every declared field exact, a prefix over leading fields, or a range
// over the next field after exact leaders. Values accept the same typed
// scalars as StateStore named values (nil/bool/int*/uint*/string/
// []byte); floats are refused — the wire carries exact i64/text/bytes.
type IndexPredicate struct {
	mode        string
	leading     []any
	partialText string
	hasPartial  bool
	lower       IndexBound
	upper       IndexBound
}

// IndexExact matches every declared field exactly.
func IndexExact(values ...any) IndexPredicate {
	return IndexPredicate{mode: "exact", leading: values}
}

// IndexPrefix matches leading fields exactly plus an optional text
// prefix on the next field.
func IndexPrefix(leading []any, partialText string) IndexPredicate {
	return IndexPredicate{
		mode: "prefix", leading: leading,
		partialText: partialText, hasPartial: partialText != "",
	}
}

// IndexRange matches leading fields exactly and the next field between
// lower and upper.
func IndexRange(leading []any, lower, upper IndexBound) IndexPredicate {
	return IndexPredicate{mode: "range", leading: leading, lower: lower, upper: upper}
}

func (p IndexPredicate) wireFields() (map[string]any, error) {
	leading, err := encodeIndexValues(p.leading)
	if err != nil {
		return nil, err
	}
	switch p.mode {
	case "exact":
		return map[string]any{"mode": "exact", "values": leading}, nil
	case "prefix":
		body := map[string]any{"mode": "prefix", "leading": leading}
		if p.hasPartial {
			body["partial_text"] = p.partialText
		}
		return body, nil
	case "range":
		lower, err := encodeIndexValue(p.lower.Value)
		if err != nil {
			return nil, err
		}
		upper, err := encodeIndexValue(p.upper.Value)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"mode":    "range",
			"leading": leading,
			"lower":   map[string]any{"value": lower, "inclusive": p.lower.Inclusive},
			"upper":   map[string]any{"value": upper, "inclusive": p.upper.Inclusive},
		}, nil
	default:
		return nil, fmt.Errorf("unknown index predicate mode %q", p.mode)
	}
}

// IndexRow is one decoded index posting row.
type IndexRow struct {
	Key     []byte
	Payload []byte
}

// IndexPage is a page returned by QueryIndex.
type IndexPage struct {
	Rows          []IndexRow
	Generation    uint64
	AppliedCursor *string
	TargetCursor  *string
	// Behind reports the serving Node is still materializing the
	// activation frontier.
	Behind bool
	// Truncated reports the page hit a server bound; narrow the query
	// to continue.
	Truncated bool
	// ServedBy is the Node identity that served — diagnostics, never
	// routing input.
	ServedBy string
}

// QueryIndex queries a replicated Index by predicate — exact, prefix,
// or range over typed values. strict waits for the committed target
// rather than answering stale.
func (c *Client) QueryIndex(
	ctx context.Context,
	index string,
	predicate IndexPredicate,
	limit int,
	strict bool,
) (IndexPage, error) {
	body := map[string]any{
		"index":  index,
		"strict": strict,
	}
	fields, err := predicate.wireFields()
	if err != nil {
		return IndexPage{}, err
	}
	for key, value := range fields {
		body[key] = value
	}
	if limit > 0 {
		body["limit"] = limit
	}
	raw, err := c.transport.post(ctx, "/v1/indexes/query", body)
	if err != nil {
		return IndexPage{}, err
	}
	var reply struct {
		Rows []struct {
			KeyB64     string `json:"key_base64"`
			PayloadB64 string `json:"payload_base64"`
		} `json:"rows"`
		Generation    uint64  `json:"generation"`
		AppliedCursor *string `json:"applied_cursor"`
		TargetCursor  *string `json:"target_cursor"`
		Behind        bool    `json:"behind"`
		Truncated     bool    `json:"truncated"`
		ServedBy      string  `json:"served_by"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return IndexPage{}, fmt.Errorf("index page undecodable: %w", err)
	}
	page := IndexPage{
		Generation:    reply.Generation,
		AppliedCursor: reply.AppliedCursor,
		TargetCursor:  reply.TargetCursor,
		Behind:        reply.Behind,
		Truncated:     reply.Truncated,
		ServedBy:      reply.ServedBy,
	}
	for _, row := range reply.Rows {
		key, err := base64.StdEncoding.DecodeString(row.KeyB64)
		if err != nil {
			return IndexPage{}, fmt.Errorf("index row key undecodable: %w", err)
		}
		payload, err := base64.StdEncoding.DecodeString(row.PayloadB64)
		if err != nil {
			return IndexPage{}, fmt.Errorf("index row payload undecodable: %w", err)
		}
		page.Rows = append(page.Rows, IndexRow{Key: key, Payload: payload})
	}
	return page, nil
}

// ---------------------------------------------------------------------------
// Demand observability
// ---------------------------------------------------------------------------

// DemandRow is one demand row for a declared resource. Fields carries
// the server's counters verbatim — new counters arrive without an SDK
// update, the same contract as PipeInfo.Operation.
type DemandRow struct {
	Name   string
	Domain string
	Fields map[string]any
}

// Counter reads one counter as uint64 when present.
func (r DemandRow) Counter(name string) (uint64, bool) {
	switch value := r.Fields[name].(type) {
	case float64:
		return uint64(value), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return uint64(parsed), true
	default:
		return 0, false
	}
}

// DemandReport is a demand snapshot for one resource kind: per-resource
// rows plus the per-domain rollup map.
type DemandReport struct {
	Rows []DemandRow
	// Domains maps domain name to its aggregated counters — opaque
	// rollup rows keyed by Domain.
	Domains map[string]map[string]any
}

// Domain returns one domain's aggregated counters.
func (r DemandReport) Domain(name string) map[string]any {
	return r.Domains[name]
}

// HotKey is evidence that one Feed Key dominates its traffic window. A
// strictly ordered hot Key is never parallelized — the honest operator
// signal is to spread or shed load for the Key, not add capacity.
type HotKey struct {
	// Evidence is the evidence object verbatim — includes any
	// range-placement fields the server reports; those are diagnostics,
	// not a public placement contract.
	Evidence map[string]any
}

// Feed returns the Feed carrying the dominant Key, when reported.
func (h HotKey) Feed() string {
	feed, _ := h.Evidence["feed"].(string)
	return feed
}

// ShareBps returns the dominant-Key share of the window in basis points.
func (h HotKey) ShareBps() (uint64, bool) {
	return numericField(h.Evidence, "share_bps")
}

// WindowRecords returns records attributed to the Key in the window.
func (h HotKey) WindowRecords() (uint64, bool) {
	return numericField(h.Evidence, "window_records")
}

// WindowBytes returns bytes attributed to the Key in the window.
func (h HotKey) WindowBytes() (uint64, bool) {
	return numericField(h.Evidence, "window_bytes")
}

func numericField(evidence map[string]any, name string) (uint64, bool) {
	value, ok := evidence[name].(float64)
	if !ok {
		return 0, false
	}
	return uint64(value), true
}

func (c *Client) demand(
	ctx context.Context, surface, resourceKey, nameField string,
) (DemandReport, error) {
	raw, err := c.transport.get(ctx, "/v1/admin/demand/"+surface)
	if err != nil {
		return DemandReport{}, err
	}
	var body struct {
		Domains map[string]map[string]any `json:"domains"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return DemandReport{}, fmt.Errorf("demand report undecodable: %w", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return DemandReport{}, fmt.Errorf("demand report undecodable: %w", err)
	}
	var rows []map[string]any
	if raw := envelope[resourceKey]; raw != nil {
		if err := json.Unmarshal(raw, &rows); err != nil {
			return DemandReport{}, fmt.Errorf("demand rows undecodable: %w", err)
		}
	}
	report := DemandReport{Domains: body.Domains}
	if report.Domains == nil {
		report.Domains = map[string]map[string]any{}
	}
	for _, row := range rows {
		name, _ := row[nameField].(string)
		domain, _ := row["domain"].(string)
		report.Rows = append(report.Rows, DemandRow{
			Name: name, Domain: domain, Fields: row,
		})
	}
	return report, nil
}

// DemandFeeds reports per-Feed append/read/replication attribution
// plus per-domain rollups (GET /v1/admin/demand/feeds).
func (c *Client) DemandFeeds(ctx context.Context) (DemandReport, error) {
	return c.demand(ctx, "feeds", "feeds", "feed")
}

// DemandReaders reports per-Reader request/fetch/ack counters
// (GET /v1/admin/demand/readers).
func (c *Client) DemandReaders(ctx context.Context) (DemandReport, error) {
	return c.demand(ctx, "readers", "readers", "reader")
}

// DemandSubscriptions reports per-Subscription counters
// (GET /v1/admin/demand/subscriptions).
func (c *Client) DemandSubscriptions(ctx context.Context) (DemandReport, error) {
	return c.demand(ctx, "subscriptions", "subscriptions", "subscription")
}

// DemandIndexes reports per-Index query counters
// (GET /v1/admin/demand/indexes).
func (c *Client) DemandIndexes(ctx context.Context) (DemandReport, error) {
	return c.demand(ctx, "indexes", "indexes", "index")
}

// DemandPipes reports per-Pipe drive/apply/failure counters
// (GET /v1/admin/demand/pipes).
func (c *Client) DemandPipes(ctx context.Context) (DemandReport, error) {
	return c.demand(ctx, "pipes", "pipes", "pipe")
}

// HotKeys returns dominant-Key evidence across Feeds
// (GET /v1/admin/demand/hot-keys).
func (c *Client) HotKeys(ctx context.Context) ([]HotKey, error) {
	raw, err := c.transport.get(ctx, "/v1/admin/demand/hot-keys")
	if err != nil {
		return nil, err
	}
	var body struct {
		HotKeys []map[string]any `json:"hot_keys"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("hot-key evidence undecodable: %w", err)
	}
	keys := make([]HotKey, 0, len(body.HotKeys))
	for _, evidence := range body.HotKeys {
		keys = append(keys, HotKey{Evidence: evidence})
	}
	return keys, nil
}
