package whitewater

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Pipe lifecycle, index queries, and demand observability — the
// operational surfaces parity-tested against scripted wire shapes.

const pipeRow = `{
	"name": "orders.relay",
	"stage": "paused",
	"subscription_id": "019a0000-0000-7000-8000-000000000020",
	"operation": {"operation": "forward"},
	"output_feed_id": "019a0000-0000-7000-8000-000000000021",
	"output_store_id": null,
	"status": "active",
	"created_at_ns": 42
}`

func commandsWith(data string) string {
	return `{
		"request_id": "019a0000-0000-7000-8000-000000000006",
		"revision": 9,
		"authority": "control_plane",
		"warning": null,
		"results": [{"statement": "s", "message": "ok", "data": ` + data + `}]
	}`
}

func operationsStub(t *testing.T) (*Client, *[]string, *[]string) {
	t.Helper()
	var paths, bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, string(raw))
		w.Header().Set("content-type", "application/json")
		var reply string
		switch r.URL.Path {
		case "/v1/admin/commands":
			var body struct {
				Commands []map[string]any `json:"commands"`
			}
			_ = json.Unmarshal(raw, &body)
			switch body.Commands[0]["command"] {
			case "describe":
				reply = commandsWith(pipeRow)
			case "show":
				reply = commandsWith(`{"rows": [` + pipeRow + `]}`)
			default:
				reply = commandsWith(`{}`)
			}
		case "/v1/admin/demand/feeds":
			reply = `{
				"feeds": [{
					"feed": "orders.events", "domain": "orders",
					"appended_records": 1500, "appended_bytes": 2048000,
					"read_pages": 42
				}],
				"domains": {"orders": {"feeds": 1, "appended_records": 1500}}
			}`
		case "/v1/admin/demand/hot-keys":
			reply = `{
				"hot_keys": [{
					"feed": "orders.events", "share_bps": 9000,
					"window_records": 900, "window_bytes": 12000
				}]
			}`
		case "/v1/admin/demand/readers", "/v1/admin/demand/subscriptions",
			"/v1/admin/demand/indexes", "/v1/admin/demand/pipes":
			kind := r.URL.Path[len("/v1/admin/demand/"):]
			reply = `{"` + kind + `": [], "domains": {}}`
		case "/v1/indexes/query":
			reply = `{
				"index": "crm.byregion",
				"index_id": "019a0000-0000-7000-8000-000000000030",
				"generation": 2,
				"rows": [{
					"key_base64": "` + base64.StdEncoding.EncodeToString([]byte("acct-1")) + `",
					"payload_base64": "` + base64.StdEncoding.EncodeToString([]byte(`{"tier":"gold"}`)) + `"
				}],
				"applied_cursor": "idx-cursor",
				"target_cursor": "tip-cursor",
				"behind": false, "truncated": false,
				"served_by": "node-1"
			}`
		default:
			reply = `{}`
		}
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(server.Close)
	return NewClient("test-key", []string{server.URL}, 5*time.Second), &paths, &bodies
}

// ---- Pipe lifecycle --------------------------------------------------------

func TestPipeDescribeDecodesTheCatalogRow(t *testing.T) {
	client, _, bodies := operationsStub(t)
	info, err := client.Pipe("orders.relay").Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "orders.relay" || info.Stage != "paused" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if info.SubscriptionID != "019a0000-0000-7000-8000-000000000020" {
		t.Fatalf("subscription = %s", info.SubscriptionID)
	}
	var operation map[string]string
	if err := json.Unmarshal(info.Operation, &operation); err != nil {
		t.Fatal(err)
	}
	if operation["operation"] != "forward" {
		t.Fatalf("operation = %v", operation)
	}
	if info.OutputFeedID == nil || *info.OutputFeedID == "" {
		t.Fatal("output_feed_id should decode")
	}
	if info.OutputStoreID != nil {
		t.Fatal("output_store_id should be nil")
	}
	if info.CreatedAtNs != 42 {
		t.Fatalf("created_at_ns = %d", info.CreatedAtNs)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte((*bodies)[0]), &body)
	command := body["commands"].([]any)[0].(map[string]any)
	if command["command"] != "describe" || command["kind"] != "pipe" {
		t.Fatalf("command = %v", command)
	}
}

func TestPipeLifecyclePostsTheTypedVerbs(t *testing.T) {
	client, paths, bodies := operationsStub(t)
	pipe := client.Pipe("orders.relay")
	ctx := context.Background()
	if err := pipe.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pipe.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pipe.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"pause_pipe", "resume_pipe", "drop"}
	for i, raw := range *bodies {
		var body map[string]any
		_ = json.Unmarshal([]byte(raw), &body)
		command := body["commands"].([]any)[0].(map[string]any)
		if command["command"] != want[i] {
			t.Fatalf("verb %d = %v, want %s", i, command["command"], want[i])
		}
	}
	for _, path := range *paths {
		if path != "/v1/admin/commands" {
			t.Fatalf("path = %s", path)
		}
	}
}

func TestPipesListsTheCatalog(t *testing.T) {
	client, _, bodies := operationsStub(t)
	pipes, err := client.Pipes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pipes) != 1 || pipes[0].Name != "orders.relay" {
		t.Fatalf("pipes = %+v", pipes)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte((*bodies)[0]), &body)
	command := body["commands"].([]any)[0].(map[string]any)
	if command["command"] != "show" || command["kind"] != "pipes" {
		t.Fatalf("command = %v", command)
	}
}

// ---- Demand observability ---------------------------------------------------

func TestDemandFeedsDecodesRowsAndDomainRollups(t *testing.T) {
	client, paths, _ := operationsStub(t)
	report, err := client.DemandFeeds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("rows = %+v", report.Rows)
	}
	row := report.Rows[0]
	if row.Name != "orders.events" || row.Domain != "orders" {
		t.Fatalf("row = %+v", row)
	}
	if appended, ok := row.Counter("appended_records"); !ok || appended != 1500 {
		t.Fatalf("appended_records = %d,%v", appended, ok)
	}
	if _, ok := row.Counter("nonsense"); ok {
		t.Fatal("missing counter should report absent")
	}
	if report.Domain("orders")["feeds"].(float64) != 1 {
		t.Fatalf("domains = %+v", report.Domains)
	}
	if (*paths)[0] != "/v1/admin/demand/feeds" {
		t.Fatalf("path = %s", (*paths)[0])
	}
}

func TestDemandEndpointsCoverEveryResourceKind(t *testing.T) {
	client, paths, _ := operationsStub(t)
	ctx := context.Background()
	for _, call := range []func(context.Context) (DemandReport, error){
		client.DemandReaders,
		client.DemandSubscriptions,
		client.DemandIndexes,
		client.DemandPipes,
	} {
		report, err := call(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Rows) != 0 {
			t.Fatalf("rows = %+v", report.Rows)
		}
	}
	want := []string{
		"/v1/admin/demand/readers",
		"/v1/admin/demand/subscriptions",
		"/v1/admin/demand/indexes",
		"/v1/admin/demand/pipes",
	}
	for i, path := range *paths {
		if path != want[i] {
			t.Fatalf("path %d = %s, want %s", i, path, want[i])
		}
	}
}

func TestHotKeysDecodeDominantKeyEvidence(t *testing.T) {
	client, paths, _ := operationsStub(t)
	keys, err := client.HotKeys(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys = %+v", keys)
	}
	if keys[0].Feed() != "orders.events" {
		t.Fatalf("feed = %s", keys[0].Feed())
	}
	if share, ok := keys[0].ShareBps(); !ok || share != 9000 {
		t.Fatalf("share_bps = %d,%v", share, ok)
	}
	if records, ok := keys[0].WindowRecords(); !ok || records != 900 {
		t.Fatalf("window_records = %d,%v", records, ok)
	}
	if (*paths)[0] != "/v1/admin/demand/hot-keys" {
		t.Fatalf("path = %s", (*paths)[0])
	}
}

// ---- Index queries -----------------------------------------------------------

func TestQueryIndexRangeFlattensBoundsAndDecodesRows(t *testing.T) {
	client, paths, bodies := operationsStub(t)
	page, err := client.QueryIndex(
		context.Background(),
		"crm.byregion",
		IndexRange([]any{"emea"}, InclusiveBound(0), ExclusiveBound(99)),
		50,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if page.Behind || page.Generation != 2 || page.ServedBy != "node-1" {
		t.Fatalf("page = %+v", page)
	}
	if len(page.Rows) != 1 || string(page.Rows[0].Key) != "acct-1" {
		t.Fatalf("rows = %+v", page.Rows)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte((*bodies)[0]), &body)
	if body["mode"] != "range" || body["strict"] != true {
		t.Fatalf("body = %v", body)
	}
	upper := body["upper"].(map[string]any)
	if upper["inclusive"] != false {
		t.Fatalf("upper = %v", upper)
	}
	if (*paths)[0] != "/v1/indexes/query" {
		t.Fatalf("path = %s", (*paths)[0])
	}
}

func TestQueryIndexExactCarriesEveryTypedKind(t *testing.T) {
	client, _, bodies := operationsStub(t)
	_, err := client.QueryIndex(
		context.Background(),
		"crm.byregion",
		IndexExact("emea", 42, true, nil, []byte{1, 2}),
		0,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte((*bodies)[0]), &body)
	if body["mode"] != "exact" {
		t.Fatalf("mode = %v", body["mode"])
	}
	values := body["values"].([]any)
	if len(values) != 5 {
		t.Fatalf("values = %v", values)
	}
	if values[0].(map[string]any)["kind"] != "text" ||
		values[3].(map[string]any)["kind"] != "null" ||
		values[4].(map[string]any)["kind"] != "bytes" {
		t.Fatalf("values = %v", values)
	}
}

func TestQueryIndexPrefixOmitsPartialTextWhenAbsent(t *testing.T) {
	client, _, bodies := operationsStub(t)
	ctx := context.Background()
	if _, err := client.QueryIndex(
		ctx, "crm.byregion", IndexPrefix([]any{"emea"}, ""), 0, false); err != nil {
		t.Fatal(err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte((*bodies)[0]), &first)
	if first["mode"] != "prefix" {
		t.Fatalf("mode = %v", first["mode"])
	}
	if _, present := first["partial_text"]; present {
		t.Fatal("partial_text must be omitted when empty")
	}
	if _, err := client.QueryIndex(
		ctx, "crm.byregion", IndexPrefix([]any{"emea"}, "gold"), 0, false); err != nil {
		t.Fatal(err)
	}
	var second map[string]any
	_ = json.Unmarshal([]byte((*bodies)[1]), &second)
	if second["partial_text"] != "gold" {
		t.Fatalf("partial_text = %v", second["partial_text"])
	}
}

func TestQueryIndexRejectsUnsupportedValueTypes(t *testing.T) {
	client, _, _ := operationsStub(t)
	_, err := client.QueryIndex(
		context.Background(),
		"crm.byregion",
		IndexExact(3.14),
		0,
		false,
	)
	if err == nil {
		t.Fatal("float values must be refused — the wire carries exact types")
	}
}
