package whitewater

// Pipe declaration: the typed define surface over c.Commands. A Pipe
// consumes one Subscription through a declared operation — forward,
// enrich, or window — into a Feed or a managed StateStore view. The
// surface is names only; partitions never appear.

import (
	"context"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// Pipe operation vocabulary
// ---------------------------------------------------------------------------

// WindowTime selects which record clock a window Pipe buckets into
// panes.
type WindowTime string

const (
	// WindowEventTime buckets by event_time_ns — the application-stamped
	// occurrence clock (default).
	WindowEventTime WindowTime = "event"
	// WindowIngestTime buckets by ingest_time_ns — the durable-arrival
	// clock.
	WindowIngestTime WindowTime = "ingest"
)

// EnrichFreshness is how fresh an enrich Pipe's lookup row must be
// before it is journaled.
type EnrichFreshness string

const (
	// EnrichLatest serves the replica's applied state (default; the
	// resolution is journaled either way).
	EnrichLatest EnrichFreshness = "latest"
	// EnrichStrict requires applied to cover committed at read entry;
	// lag yields a retryable drive failure.
	EnrichStrict EnrichFreshness = "strict"
)

// EnrichMiss is what an enrich Pipe lookup miss does to the output
// record.
type EnrichMiss string

const (
	// EnrichMissForward forwards the record with enrich.matched = false
	// (default).
	EnrichMissForward EnrichMiss = "forward"
	// EnrichMissDrop consumes without emitting; the input frontier
	// still commits.
	EnrichMissDrop EnrichMiss = "drop"
	// EnrichMissQuarantine routes the miss to the declared quarantine
	// Feed.
	EnrichMissQuarantine EnrichMiss = "quarantine"
)

// WindowAggregation is what a window Pipe adds to each pane's
// accumulator: count records, sum an extracted integer, or track
// count+sum for average (readers divide). Build with CountAggregation,
// SumAggregation, or AverageAggregation.
type WindowAggregation struct {
	aggregation string
	value       *Extractor
}

// CountAggregation counts records per (group Key, pane).
func CountAggregation() WindowAggregation {
	return WindowAggregation{aggregation: "count"}
}

// SumAggregation sums one extracted integer value per pane.
func SumAggregation(value Extractor) WindowAggregation {
	return WindowAggregation{aggregation: "sum", value: &value}
}

// AverageAggregation tracks count and integer sum per pane; readers
// divide.
func AverageAggregation(value Extractor) WindowAggregation {
	return WindowAggregation{aggregation: "average", value: &value}
}

func (a WindowAggregation) wire() map[string]any {
	out := map[string]any{"aggregation": a.aggregation}
	if a.value != nil {
		out["value"] = a.value.wire()
	}
	return out
}

func (a WindowAggregation) validate() error {
	switch a.aggregation {
	case "count":
		return nil
	case "sum", "average":
		if a.value == nil {
			return fmt.Errorf("aggregation %q takes a value extractor", a.aggregation)
		}
		return a.value.validate()
	default:
		return fmt.Errorf(
			"window aggregation is undeclared — build with " +
				"CountAggregation/SumAggregation/AverageAggregation")
	}
}

// validate screens an Extractor before it reaches the wire — the
// constructors stay total (no error return), so a bad spec is caught
// here at define time, the same contract as QueryIndex predicates.
func (e Extractor) validate() error {
	switch e.kind {
	case "key":
		return nil
	case "metadata":
		if strings.TrimSpace(e.name) == "" {
			return fmt.Errorf("metadata extractor name is required")
		}
		return nil
	case "projection":
		if strings.TrimSpace(e.field) == "" {
			return fmt.Errorf("json extractor field is required")
		}
		return nil
	default:
		return fmt.Errorf(
			"extractor is undeclared — build with " +
				"KeyExtractor/MetadataExtractor/JsonExtractor")
	}
}

// ---------------------------------------------------------------------------
// PipeOperation
// ---------------------------------------------------------------------------

// PipeOperation is the processing step a Pipe applies to each delivered
// input record. Build with ForwardOperation, EnrichOperation, or
// WindowOperation; the wire form is internally tagged on "operation".
type PipeOperation struct {
	operation   string
	key         Extractor
	freshness   EnrichFreshness
	named       string
	onMiss      EnrichMiss
	windowNs    uint64
	aggregation WindowAggregation
	time        WindowTime
	maxLateNs   uint64
	watermarkNs uint64
	graceNs     uint64
	expireNs    uint64
	keyLimit    uint64
	byteLimit   uint64
}

// EnrichOptions tunes EnrichOperation; the zero value is the default —
// latest freshness, primary-key resolution, forward-on-miss.
type EnrichOptions struct {
	// Freshness bounds lookup-row staleness; empty means EnrichLatest.
	Freshness EnrichFreshness
	// Named resolves the lookup through a declared named secondary key
	// of the store — the same by-named contract GetBy uses — instead of
	// the primary key; empty resolves by primary key.
	Named string
	// OnMiss is the miss disposition; empty means EnrichMissForward.
	// EnrichMissQuarantine requires DefineEnrichPipe's quarantineFeed.
	OnMiss EnrichMiss
}

// WindowOptions tunes WindowOperation; every field is optional and a
// zero value is omitted from the wire — the server applies its own
// defaults and enforces the cross-field rules (lateness-model
// exclusivity, bound ordering) at define time.
type WindowOptions struct {
	// Time selects the pane clock; empty means WindowEventTime.
	Time WindowTime
	// MaxLateNs is the lateness bound under the event clock: a record
	// arriving more than this far behind its own event_time_ns
	// quarantines into a countable late row instead of a pane. Mutually
	// exclusive with WatermarkNs — one lateness model per Pipe — and
	// refused under WindowIngestTime.
	MaxLateNs uint64
	// WatermarkNs tracks a durable monotone horizon
	// max(event_time) − watermark_ns per Pipe and closes a pane once
	// the horizon passes its end — close decisions follow event-time
	// disorder rather than ingest-clock skew. Mutually exclusive with
	// MaxLateNs; refused under WindowIngestTime.
	WatermarkNs uint64
	// GraceNs is the revision bound; it requires WatermarkNs. An event
	// landing on an already-emitted pane while the horizon stays within
	// pane_end + grace_ns revises the pane and re-emits under a bumped
	// revision; past the grace bound the event counts into the late
	// row. Without GraceNs every emitted-pane event quarantines.
	GraceNs uint64
	// ExpireNs bounds pane-row retention: a row deletes once
	// pane_end + expire_ns is behind the close clock. Must exceed
	// MaxLateNs or GraceNs when declared with them — a pane must
	// provably close before it may expire.
	ExpireNs uint64
	// KeyLimit bounds distinct pane keys.
	KeyLimit uint64
	// ByteLimit bounds pane state bytes.
	ByteLimit uint64
}

// ForwardOperation copies each delivered record to the output Feed
// unchanged.
func ForwardOperation() PipeOperation {
	return PipeOperation{operation: "forward"}
}

// EnrichOperation point-lookup-enriches each delivered record against
// the Pipe's store: resolve one row by the projected key and copy its
// payload into output Metadata (enrich.payload, enrich.matched). The
// lookup resolution rides inside the effect journal, so replayed
// outputs are byte-identical. options is optional — the zero
// EnrichOptions gives latest freshness, forward-on-miss.
func EnrichOperation(key Extractor, options ...EnrichOptions) PipeOperation {
	if len(options) > 1 {
		panic("EnrichOperation takes at most one EnrichOptions")
	}
	op := PipeOperation{operation: "enrich", key: key}
	if len(options) == 1 {
		op.freshness = options[0].Freshness
		op.named = options[0].Named
		op.onMiss = options[0].OnMiss
	}
	return op
}

// WindowOperation accumulates each delivered record into fixed
// windowNs-wide panes of a managed StateStore view. Panes are immutable
// once written. windowNs must be positive and key/aggregation must be
// declared — all validated at define time, where the error is returned
// instead of a constructor panic.
func WindowOperation(
	windowNs uint64,
	key Extractor,
	aggregation WindowAggregation,
	options ...WindowOptions,
) PipeOperation {
	if len(options) > 1 {
		panic("WindowOperation takes at most one WindowOptions")
	}
	op := PipeOperation{
		operation:   "window",
		key:         key,
		windowNs:    windowNs,
		aggregation: aggregation,
	}
	if len(options) == 1 {
		op.time = options[0].Time
		op.maxLateNs = options[0].MaxLateNs
		op.watermarkNs = options[0].WatermarkNs
		op.graceNs = options[0].GraceNs
		op.expireNs = options[0].ExpireNs
		op.keyLimit = options[0].KeyLimit
		op.byteLimit = options[0].ByteLimit
	}
	return op
}

// wire is the {"operation": ...} command fragment — the tagged union
// the catalog parses. Defaults the Java surface spells are filled here:
// enrich always carries freshness/on_miss, window always carries time.
func (o PipeOperation) wire() map[string]any {
	out := map[string]any{"operation": o.operation}
	switch o.operation {
	case "enrich":
		out["key"] = o.key.wire()
		freshness := o.freshness
		if freshness == "" {
			freshness = EnrichLatest
		}
		out["freshness"] = string(freshness)
		if o.named != "" {
			out["named"] = o.named
		}
		miss := o.onMiss
		if miss == "" {
			miss = EnrichMissForward
		}
		out["on_miss"] = string(miss)
	case "window":
		out["key"] = o.key.wire()
		out["window_ns"] = o.windowNs
		out["aggregation"] = o.aggregation.wire()
		time := o.time
		if time == "" {
			time = WindowEventTime
		}
		out["time"] = string(time)
		if o.maxLateNs > 0 {
			out["max_late_ns"] = o.maxLateNs
		}
		if o.watermarkNs > 0 {
			out["watermark_ns"] = o.watermarkNs
		}
		if o.graceNs > 0 {
			out["grace_ns"] = o.graceNs
		}
		if o.expireNs > 0 {
			out["expire_ns"] = o.expireNs
		}
		if o.keyLimit > 0 {
			out["key_limit"] = o.keyLimit
		}
		if o.byteLimit > 0 {
			out["byte_limit"] = o.byteLimit
		}
	}
	return out
}

// validate screens the operation before define_pipe posts it — bad
// specs error locally, never reaching the Control Plane.
func (o PipeOperation) validate() error {
	switch o.operation {
	case "forward":
		return nil
	case "enrich":
		if err := o.key.validate(); err != nil {
			return fmt.Errorf("enrich key: %w", err)
		}
		switch o.freshness {
		case "", EnrichLatest, EnrichStrict:
		default:
			return fmt.Errorf("invalid enrich freshness %q", o.freshness)
		}
		switch o.onMiss {
		case "", EnrichMissForward, EnrichMissDrop, EnrichMissQuarantine:
		default:
			return fmt.Errorf("invalid enrich miss disposition %q", o.onMiss)
		}
		return nil
	case "window":
		if o.windowNs == 0 {
			return fmt.Errorf("windowNs must be positive")
		}
		if err := o.key.validate(); err != nil {
			return fmt.Errorf("window key: %w", err)
		}
		if err := o.aggregation.validate(); err != nil {
			return err
		}
		switch o.time {
		case "", WindowEventTime, WindowIngestTime:
		default:
			return fmt.Errorf("invalid window time %q", o.time)
		}
		return nil
	default:
		return fmt.Errorf(
			"pipe operation is undeclared — build with " +
				"ForwardOperation/EnrichOperation/WindowOperation")
	}
}

// ---------------------------------------------------------------------------
// Pipe definition
// ---------------------------------------------------------------------------

// definePipeCommand starts the shared {"command": "define_pipe", ...}
// document — Pipe names are dotted like Feeds and StateStores.
func definePipeCommand(name, subscription string) (map[string]any, error) {
	n, err := dottedName(name)
	if err != nil {
		return nil, err
	}
	sub, err := pipeSubscription(subscription)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"command":      "define_pipe",
		"name":         n,
		"subscription": sub,
	}, nil
}

// pipeSubscription accepts either name grammar: the typed builders mint
// resourceName subscriptions while WCL scripts and other SDKs may
// declare dotted ones.
func pipeSubscription(name string) (string, error) {
	if _, err := resourceName(name); err == nil {
		return name, nil
	}
	if _, err := dottedName(name); err != nil {
		return "", fmt.Errorf("invalid subscription name: %q", name)
	}
	return name, nil
}

// DefineForwardPipe declares the copy Pipe — each delivered record
// lands on the output Feed unchanged (DEFINE PIPE ... TO FEED).
func (c *Client) DefineForwardPipe(
	ctx context.Context,
	name, subscription, outputFeed string,
) (ControlExecution, error) {
	command, err := definePipeCommand(name, subscription)
	if err != nil {
		return ControlExecution{}, err
	}
	feed, err := dottedName(outputFeed)
	if err != nil {
		return ControlExecution{}, err
	}
	command["output_feed"] = feed
	command["operation"] = ForwardOperation().wire()
	return c.Commands(ctx, []map[string]any{command})
}

// DefineEnrichPipe declares point-lookup enrichment; op must be built
// by EnrichOperation. store names the lookup StateStore (dotted);
// quarantineFeed resolves the miss-quarantine target when the
// operation's miss disposition is EnrichMissQuarantine — empty omits
// it.
func (c *Client) DefineEnrichPipe(
	ctx context.Context,
	name, subscription, outputFeed, store string,
	op PipeOperation,
	quarantineFeed string,
) (ControlExecution, error) {
	if op.operation != "enrich" {
		return ControlExecution{},
			fmt.Errorf("enrich pipes take EnrichOperation(...)")
	}
	if err := op.validate(); err != nil {
		return ControlExecution{}, err
	}
	command, err := definePipeCommand(name, subscription)
	if err != nil {
		return ControlExecution{}, err
	}
	feed, err := dottedName(outputFeed)
	if err != nil {
		return ControlExecution{}, err
	}
	lookup, err := dottedName(store)
	if err != nil {
		return ControlExecution{}, err
	}
	command["output_feed"] = feed
	command["output_store"] = lookup
	if quarantineFeed != "" {
		quarantine, err := dottedName(quarantineFeed)
		if err != nil {
			return ControlExecution{}, err
		}
		command["quarantine_feed"] = quarantine
	}
	command["operation"] = op.wire()
	return c.Commands(ctx, []map[string]any{command})
}

// DefineWindowPipe declares rolling-window aggregation into a managed
// StateStore view; op must be built by WindowOperation. outputStore is
// the managed view name (dotted); emitFeed optionally emits closed
// panes and requires the operation declare MaxLateNs — empty omits it.
func (c *Client) DefineWindowPipe(
	ctx context.Context,
	name, subscription, outputStore string,
	op PipeOperation,
	emitFeed string,
) (ControlExecution, error) {
	if op.operation != "window" {
		return ControlExecution{},
			fmt.Errorf("window pipes take WindowOperation(...)")
	}
	if err := op.validate(); err != nil {
		return ControlExecution{}, err
	}
	command, err := definePipeCommand(name, subscription)
	if err != nil {
		return ControlExecution{}, err
	}
	view, err := dottedName(outputStore)
	if err != nil {
		return ControlExecution{}, err
	}
	command["output_store"] = view
	if emitFeed != "" {
		emit, err := dottedName(emitFeed)
		if err != nil {
			return ControlExecution{}, err
		}
		command["emit_feed"] = emit
	}
	command["operation"] = op.wire()
	return c.Commands(ctx, []map[string]any{command})
}
