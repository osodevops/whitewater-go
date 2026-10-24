package whitewater

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Pipe declaration: wire maps asserted verbatim — the tagged operation
// union and the define_pipe envelope both mirror the Java reference.

// wireJSON marshals got and deep-compares against the want literal;
// numbers normalize through decoding so integer shapes compare clean.
func wireJSON(t *testing.T, got map[string]any, want string) {
	t.Helper()
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var wantAny, gotAny any
	if err := json.Unmarshal([]byte(want), &wantAny); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &gotAny); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotAny, wantAny) {
		t.Fatalf("wire = %s, want %s", raw, want)
	}
}

// lastCommand decodes the first (only) command in the nth recorded
// commands body.
func lastCommand(t *testing.T, bodies *[]string, n int) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal([]byte((*bodies)[n]), &body); err != nil {
		t.Fatal(err)
	}
	commands, ok := body["commands"].([]any)
	if !ok || len(commands) != 1 {
		t.Fatalf("commands = %v", body["commands"])
	}
	return commands[0].(map[string]any)
}

// ---- operation wire shapes -------------------------------------------------

func TestForwardOperationWire(t *testing.T) {
	wireJSON(t, ForwardOperation().wire(), `{"operation": "forward"}`)
}

func TestEnrichOperationWireDefaults(t *testing.T) {
	wireJSON(t, EnrichOperation(KeyExtractor()).wire(), `{
		"operation": "enrich",
		"key": {"kind": "key"},
		"freshness": "latest",
		"on_miss": "forward"
	}`)
}

func TestEnrichOperationWireFullOptions(t *testing.T) {
	op := EnrichOperation(MetadataExtractor("user_id"), EnrichOptions{
		Freshness: EnrichStrict,
		Named:     "gold",
		OnMiss:    EnrichMissQuarantine,
	})
	wireJSON(t, op.wire(), `{
		"operation": "enrich",
		"key": {"kind": "metadata", "name": "user_id"},
		"freshness": "strict",
		"named": "gold",
		"on_miss": "quarantine"
	}`)
}

func TestWindowOperationWireDefaults(t *testing.T) {
	op := WindowOperation(60_000_000_000, KeyExtractor(), CountAggregation())
	wireJSON(t, op.wire(), `{
		"operation": "window",
		"key": {"kind": "key"},
		"window_ns": 60000000000,
		"aggregation": {"aggregation": "count"},
		"time": "event"
	}`)
}

func TestWindowOperationWireFullOptions(t *testing.T) {
	op := WindowOperation(
		60_000_000_000,
		MetadataExtractor("account"),
		SumAggregation(JsonExtractor("amount")),
		WindowOptions{
			Time:        WindowIngestTime,
			MaxLateNs:   5_000_000_000,
			WatermarkNs: 6_000_000_000,
			GraceNs:     7_000_000_000,
			ExpireNs:    8_000_000_000,
			KeyLimit:    900,
			ByteLimit:   4096,
		})
	wireJSON(t, op.wire(), `{
		"operation": "window",
		"key": {"kind": "metadata", "name": "account"},
		"window_ns": 60000000000,
		"aggregation": {
			"aggregation": "sum",
			"value": {"kind": "projection", "extractor": "json", "version": 1, "field": "amount"}
		},
		"time": "ingest",
		"max_late_ns": 5000000000,
		"watermark_ns": 6000000000,
		"grace_ns": 7000000000,
		"expire_ns": 8000000000,
		"key_limit": 900,
		"byte_limit": 4096
	}`)
}

func TestWindowAggregationWire(t *testing.T) {
	wireJSON(t, CountAggregation().wire(), `{"aggregation": "count"}`)
	wireJSON(t, SumAggregation(KeyExtractor()).wire(), `{
		"aggregation": "sum", "value": {"kind": "key"}
	}`)
	wireJSON(t, AverageAggregation(JsonExtractor("score")).wire(), `{
		"aggregation": "average",
		"value": {"kind": "projection", "extractor": "json", "version": 1, "field": "score"}
	}`)
}

// ---- define commands -------------------------------------------------------

func TestDefineForwardPipePostsWireShape(t *testing.T) {
	client, paths, bodies := operationsStub(t)
	if _, err := client.DefineForwardPipe(
		context.Background(), "orders.relay", "orders-sub", "orders.copy"); err != nil {
		t.Fatal(err)
	}
	command := lastCommand(t, bodies, 0)
	if command["command"] != "define_pipe" ||
		command["name"] != "orders.relay" ||
		command["subscription"] != "orders-sub" ||
		command["output_feed"] != "orders.copy" {
		t.Fatalf("command = %v", command)
	}
	operation, _ := command["operation"].(map[string]any)
	if len(operation) != 1 || operation["operation"] != "forward" {
		t.Fatalf("operation = %v", operation)
	}
	if _, present := command["output_store"]; present {
		t.Fatal("forward pipes carry no output_store")
	}
	if (*paths)[0] != "/v1/admin/commands" {
		t.Fatalf("path = %s", (*paths)[0])
	}
}

func TestDefineEnrichPipePostsWireShape(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	op := EnrichOperation(JsonExtractor("user_id"), EnrichOptions{
		OnMiss: EnrichMissQuarantine,
	})
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles", op, "orders.dlq"); err != nil {
		t.Fatal(err)
	}
	command := lastCommand(t, bodies, 0)
	if command["command"] != "define_pipe" ||
		command["output_feed"] != "orders.enriched" ||
		command["output_store"] != "orders.profiles" ||
		command["quarantine_feed"] != "orders.dlq" {
		t.Fatalf("command = %v", command)
	}
	operation, _ := command["operation"].(map[string]any)
	if operation["operation"] != "enrich" || operation["on_miss"] != "quarantine" {
		t.Fatalf("operation = %v", operation)
	}
	key, _ := operation["key"].(map[string]any)
	if key["kind"] != "projection" || key["extractor"] != "json" ||
		key["version"] != float64(1) || key["field"] != "user_id" {
		t.Fatalf("key = %v", key)
	}
	// Empty quarantine omits the field entirely.
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich2", "orders-sub", "orders.enriched",
		"orders.profiles", EnrichOperation(KeyExtractor()), ""); err != nil {
		t.Fatal(err)
	}
	second := lastCommand(t, bodies, 1)
	if _, present := second["quarantine_feed"]; present {
		t.Fatal("empty quarantineFeed must omit quarantine_feed")
	}
}

func TestDefineWindowPipePostsWireShape(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	op := WindowOperation(
		3_600_000_000_000,
		KeyExtractor(),
		AverageAggregation(MetadataExtractor("latency")),
		WindowOptions{MaxLateNs: 60_000_000_000})
	if _, err := client.DefineWindowPipe(
		ctx, "orders.hourly", "orders-sub", "orders.hourly.counts",
		op, "orders.panes"); err != nil {
		t.Fatal(err)
	}
	command := lastCommand(t, bodies, 0)
	if command["command"] != "define_pipe" ||
		command["output_store"] != "orders.hourly.counts" ||
		command["emit_feed"] != "orders.panes" {
		t.Fatalf("command = %v", command)
	}
	if _, present := command["output_feed"]; present {
		t.Fatal("window pipes carry no output_feed")
	}
	operation, _ := command["operation"].(map[string]any)
	if operation["operation"] != "window" ||
		operation["window_ns"] != float64(3_600_000_000_000) ||
		operation["time"] != "event" ||
		operation["max_late_ns"] != float64(60_000_000_000) {
		t.Fatalf("operation = %v", operation)
	}
	// Zero options leave every bound absent.
	if _, err := client.DefineWindowPipe(
		ctx, "orders.hourly2", "orders-sub", "orders.hourly.counts",
		WindowOperation(1_000_000_000, KeyExtractor(), CountAggregation()), ""); err != nil {
		t.Fatal(err)
	}
	second := lastCommand(t, bodies, 1)
	if _, present := second["emit_feed"]; present {
		t.Fatal("empty emitFeed must omit emit_feed")
	}
	bare, _ := second["operation"].(map[string]any)
	for _, field := range []string{
		"max_late_ns", "watermark_ns", "grace_ns", "expire_ns",
		"key_limit", "byte_limit",
	} {
		if _, present := bare[field]; present {
			t.Fatalf("%s must be omitted when unset: %v", field, bare)
		}
	}
}

// ---- failures --------------------------------------------------------------

func TestDefinePipeRejectsWrongOperationKind(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles", WindowOperation(1, KeyExtractor(), CountAggregation()),
		""); err == nil || !strings.Contains(err.Error(), "EnrichOperation") {
		t.Fatalf("expected enrich-kind refusal, got %v", err)
	}
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles", ForwardOperation(), ""); err == nil ||
		!strings.Contains(err.Error(), "EnrichOperation") {
		t.Fatalf("expected enrich-kind refusal, got %v", err)
	}
	if _, err := client.DefineWindowPipe(
		ctx, "orders.hourly", "orders-sub", "orders.counts",
		EnrichOperation(KeyExtractor()), ""); err == nil ||
		!strings.Contains(err.Error(), "WindowOperation") {
		t.Fatalf("expected window-kind refusal, got %v", err)
	}
	// Refusals are local — nothing was posted.
	if len(*bodies) != 0 {
		t.Fatalf("refused defines posted %d commands", len(*bodies))
	}
}

func TestDefineWindowRejectsZeroWindowNs(t *testing.T) {
	client, _, bodies := operationsStub(t)
	_, err := client.DefineWindowPipe(
		context.Background(), "orders.hourly", "orders-sub", "orders.counts",
		WindowOperation(0, KeyExtractor(), CountAggregation()), "")
	if err == nil || !strings.Contains(err.Error(), "windowNs") {
		t.Fatalf("expected windowNs refusal, got %v", err)
	}
	if len(*bodies) != 0 {
		t.Fatal("refused define must not reach the wire")
	}
}

func TestDefinePipeValidatesOperationsAndExtractors(t *testing.T) {
	client, _, _ := operationsStub(t)
	ctx := context.Background()
	// A zero-value PipeOperation declares nothing.
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles", PipeOperation{}, ""); err == nil {
		t.Fatal("expected a refusal for an undeclared operation")
	}
	// Extractors with missing required fields fail at define time.
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles", EnrichOperation(MetadataExtractor("  ")), ""); err == nil ||
		!strings.Contains(err.Error(), "metadata extractor") {
		t.Fatalf("expected extractor refusal, got %v", err)
	}
	if _, err := client.DefineWindowPipe(
		ctx, "orders.hourly", "orders-sub", "orders.counts",
		WindowOperation(1, JsonExtractor(""), CountAggregation()), ""); err == nil ||
		!strings.Contains(err.Error(), "json extractor") {
		t.Fatalf("expected extractor refusal, got %v", err)
	}
	// Bad enum values in options structs are refused.
	if _, err := client.DefineEnrichPipe(
		ctx, "orders.enrich", "orders-sub", "orders.enriched",
		"orders.profiles",
		EnrichOperation(KeyExtractor(), EnrichOptions{OnMiss: "explode"}),
		""); err == nil || !strings.Contains(err.Error(), "miss disposition") {
		t.Fatalf("expected miss-disposition refusal, got %v", err)
	}
	// A sum/average aggregation without a value extractor is refused.
	if _, err := client.DefineWindowPipe(
		ctx, "orders.hourly", "orders-sub", "orders.counts",
		WindowOperation(1, KeyExtractor(), WindowAggregation{}), ""); err == nil {
		t.Fatal("expected an undeclared-aggregation refusal")
	}
}

func TestDefinePipeValidatesNames(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	for _, call := range []func() error{
		func() error {
			_, err := client.DefineForwardPipe(ctx, "BAD NAME", "sub", "orders.copy")
			return err
		},
		func() error {
			_, err := client.DefineForwardPipe(ctx, "orders.relay", "sub", "BAD NAME")
			return err
		},
		func() error {
			_, err := client.DefineForwardPipe(ctx, "orders.relay", "bad sub", "orders.copy")
			return err
		},
		func() error {
			_, err := client.DefineEnrichPipe(
				ctx, "orders.enrich", "sub", "orders.enriched",
				"BAD NAME", EnrichOperation(KeyExtractor()), "")
			return err
		},
	} {
		if err := call(); err == nil {
			t.Fatal("expected a name validation error")
		}
	}
	if len(*bodies) != 0 {
		t.Fatal("invalid names must not reach the wire")
	}
}
