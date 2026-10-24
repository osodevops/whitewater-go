package whitewater

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Executes the shared fixture corpus under sdk/spec/conformance/ in
// whitewater against a live Riverbed, per conformance/README.md. Skipped
// unless endpoints and a fixture dir are configured: the shared names
// WHITEWATER_ENDPOINTS and WHITEWATER_SPEC_DIR win, with the legacy
// WHITEWATER_CONFORMANCE_URLS / WHITEWATER_FIXTURE_DIR as fallbacks.
// WHITEWATER_CHAOS_CMD names the driver fault hook without which chaos.*
// steps report unsupported.
//
//	go test -run Conformance -v .
func TestSharedConformanceCorpus(t *testing.T) {
	baseURLs := os.Getenv("WHITEWATER_ENDPOINTS")
	if baseURLs == "" {
		baseURLs = os.Getenv("WHITEWATER_CONFORMANCE_URLS")
	}
	fixtureDir := os.Getenv("WHITEWATER_SPEC_DIR")
	if fixtureDir == "" {
		fixtureDir = os.Getenv("WHITEWATER_FIXTURE_DIR")
	}
	if baseURLs == "" || fixtureDir == "" {
		t.Skip("set WHITEWATER_ENDPOINTS and WHITEWATER_SPEC_DIR")
	}
	apiKey := os.Getenv("WHITEWATER_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("FINNSTREAM_ADMIN_API_KEY")
	}
	if apiKey == "" {
		apiKey = "whitewater-local-development-admin-key"
	}
	chaosCommand := os.Getenv("WHITEWATER_CHAOS_CMD")
	files, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures under %s", fixtureDir)
	}
	sort.Strings(files)
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var scenario map[string]any
			if err := json.Unmarshal(raw, &scenario); err != nil {
				t.Fatalf("parse %s: %v", file, err)
			}
			scenarioKey, _ := scenario["api_key"].(string)
			if scenarioKey == "" {
				scenarioKey = apiKey
			}
			client := NewClient(scenarioKey, strings.Split(baseURLs, ","), 30*time.Second)
			runScenario(t, client, scenario, chaosCommand)
		})
	}
}

type scenarioContext struct {
	token    string
	namedIDs map[string]string
	bindings map[string]any
}

// runScenario drives one fixture: every step substitutes tokens, polls on
// the poll budget while `expect` keeps failing, rides infrastructure
// transients (transport errors and 5xx) on a separate budget without
// spending poll attempts, binds the result, then asserts expectations.
func runScenario(t *testing.T, client *Client, scenario map[string]any, chaosCommand string) {
	t.Helper()
	name, _ := scenario["scenario"].(string)
	ctx := &scenarioContext{
		token:    "t" + strings.ReplaceAll(newUUID(), "-", "")[:12],
		namedIDs: map[string]string{},
		bindings: map[string]any{},
	}
	steps, _ := scenario["steps"].([]any)
	for index, stepRaw := range steps {
		step, _ := stepRaw.(map[string]any)
		op, _ := step["op"].(string)
		expect, _ := step["expect"].(map[string]any)

		if strings.HasPrefix(op, "chaos.") {
			runChaos(t, name, index, op, step, ctx, chaosCommand)
			continue
		}
		if _, _, ok := endpointFor(op); !ok &&
			op != "feed.records" && op != "health" && op != "statestore.mutate" {
			t.Fatalf("%s: step %d (%s) unsupported op", name, index+1, op)
		}

		// Anonymous {{uuid}} occurrences bind once per step so transient
		// retries and poll re-dispatches reuse one request identity; named
		// {{uuid:name}} tokens memoize on the context either way.
		argsTemplate, _ := step["args"].(map[string]any)
		argsTemplate = bindAnonymousUUIDs(argsTemplate, index)

		poll, _ := step["poll"].(map[string]any)
		pollTimes := int(int64Value(poll["times"]))
		pollInterval := int64Value(poll["interval_ms"])
		if pollTimes <= 0 {
			pollTimes = 1
		}
		if pollTimes > 30 {
			pollTimes = 30
		}
		if pollInterval <= 0 {
			pollInterval = 500
		}
		if pollInterval > 10_000 {
			pollInterval = 10_000
		}

		attempt := 0
		transientAttempts := 0
		var status int
		var body any
		for {
			args := substitute(argsTemplate, ctx)
			var err error
			status, body, err = dispatch(context.Background(), client, op, args)
			if err != nil {
				if strings.HasPrefix(err.Error(), "transport:") && transientAttempts < 120 {
					transientAttempts++
					time.Sleep(500 * time.Millisecond)
					continue
				}
				attempt++
				if attempt < pollTimes {
					time.Sleep(time.Duration(pollInterval) * time.Millisecond)
					continue
				}
				t.Fatalf("%s: step %d (%s) failed: %v", name, index+1, op, err)
			}
			if status >= 500 && transientAttempts < 120 {
				transientAttempts++
				time.Sleep(500 * time.Millisecond)
				continue
			}
			attempt++
			if expect != nil {
				substituted := substitute(expect, ctx).(map[string]any)
				if message := checkExpect(status, body, substituted, ctx); message != "" {
					if attempt < pollTimes {
						time.Sleep(time.Duration(pollInterval) * time.Millisecond)
						continue
					}
					t.Fatalf("%s: step %d (%s) expectation failed (status %d): %s",
						name, index+1, op, status, message)
				}
			}
			break
		}
		if bind, ok := step["bind"].(string); ok {
			ctx.bindings[bind] = body
		}
	}
}

// runChaos hands the step to the environment's fault driver:
// `WHITEWATER_CHAOS_CMD <op> <rendered args...>`. No hook reports
// unsupported explicitly per the corpus contract.
func runChaos(t *testing.T, name string, index int, op string, step map[string]any, ctx *scenarioContext, command string) {
	t.Helper()
	args, _ := step["args"].(map[string]any)
	rendered := substitute(args, ctx).(map[string]any)
	action := op
	for _, key := range sortedKeys(rendered) {
		text := fmt.Sprint(rendered[key])
		action += " " + text
	}
	if command == "" {
		t.Fatalf("%s: step %d (%s) unsupported: no WHITEWATER_CHAOS_CMD hook installed",
			name, index+1, op)
	}
	shell, flag := "sh", "-c"
	if runtime.GOOS == "windows" {
		shell, flag = "cmd", "/C"
	}
	invocation := exec.Command(shell, flag, command+" "+action)
	output, err := invocation.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: step %d (%s) chaos hook failed: %v: %s",
			name, index+1, op, err, output)
	}
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// bindAnonymousUUIDs rewrites every anonymous {{uuid}} inside a step's
// args to a deterministic {{uuid:anon-<step>-<ordinal>}} so repeated
// substitutions mint one identity per occurrence.
func bindAnonymousUUIDs(args map[string]any, step int) map[string]any {
	ordinal := 0
	var walk func(node any) any
	walk = func(node any) any {
		switch typed := node.(type) {
		case map[string]any:
			out := map[string]any{}
			for key, value := range typed {
				out[key] = walk(value)
			}
			return out
		case []any:
			out := make([]any, 0, len(typed))
			for _, item := range typed {
				out = append(out, walk(item))
			}
			return out
		case string:
			for strings.Contains(typed, "{{uuid}}") {
				typed = strings.Replace(
					typed, "{{uuid}}",
					fmt.Sprintf("{{uuid:anon-%d-%d}}", step, ordinal), 1)
				ordinal++
			}
			return typed
		default:
			return node
		}
	}
	return walk(args).(map[string]any)
}

// -- dispatch -------------------------------------------------------------

// endpointFor maps the shared op vocabulary onto the wire surface; the
// fixtures assert wire names directly.
func endpointFor(op string) (string, string, bool) {
	paths := map[string][2]string{
		"wcl":                    {"POST", "/v1/admin/wcl"},
		"admin.wcl":              {"POST", "/v1/admin/wcl"},
		"admin.commands":         {"POST", "/v1/admin/commands"},
		"writer.append":          {"POST", "/v1/writers/append"},
		"writer.append_batch":    {"POST", "/v1/writers/append-batch"},
		"reader.open":            {"POST", "/v1/readers/open"},
		"reader.fetch":           {"POST", "/v1/readers/fetch"},
		"reader.ack":             {"POST", "/v1/readers/ack"},
		"reader.close":           {"POST", "/v1/readers/close"},
		"reader.temporary_fetch": {"POST", "/v1/readers/temporary/fetch"},
		"feed.append":            {"POST", "/v1/feeds/append"},
		"member.join":            {"POST", "/v1/subscriptions/members/join"},
		"member.claim":           {"POST", "/v1/subscriptions/members/claim"},
		"member.renew":           {"POST", "/v1/subscriptions/members/renew"},
		"member.release":         {"POST", "/v1/subscriptions/members/release"},
		"member.transfer":        {"POST", "/v1/subscriptions/members/transfer"},
		"member.fetch":           {"POST", "/v1/subscriptions/members/fetch"},
		"member.ack":             {"POST", "/v1/subscriptions/members/ack"},
		"subscription.seek":      {"POST", "/v1/subscriptions/seek"},
		"statestore.lookup":      {"POST", "/v1/state-stores/lookup"},
		"statestore.get_by":      {"POST", "/v1/state-stores/get-by"},
		"statestore.iterate":     {"POST", "/v1/state-stores/iterate"},
		"statestore.clear":       {"POST", "/v1/state-stores/clear"},
	}
	entry, ok := paths[op]
	if !ok {
		return "", "", false
	}
	return entry[0], entry[1], true
}

// dispatch executes one op over the wire and returns the HTTP status plus
// decoded body. Only transport-level failures (unreachable, encode) come
// back as `transport:` errors the transient budget covers.
func dispatch(ctx context.Context, client *Client, op string, args any) (int, any, error) {
	if op == "health" {
		return wireGet(ctx, client, "/health")
	}
	if op == "feed.records" {
		object, _ := args.(map[string]any)
		path := "/v1/feeds/records?feed=" + url.QueryEscape(str(object, "feed"))
		if limit := int64Arg(object, "limit"); limit > 0 {
			path += "&limit=" + strconv.FormatInt(limit, 10)
		}
		if after := str(object, "after"); after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		status, body, err := wireGet(ctx, client, path)
		if err != nil {
			return 0, nil, err
		}
		if records, ok := body.([]any); ok {
			return status, map[string]any{"records": records}, nil
		}
		return status, body, nil
	}
	if op == "statestore.mutate" {
		// Flat {store, mutation_id, kind, key, payload, named} lowers to
		// {store, mutation_id, op: {kind, key_base64?, payload_base64?, named?}}.
		body, ok := wireArgs(args).(map[string]any)
		if !ok {
			return 0, nil, fmt.Errorf("statestore.mutate args must be an object")
		}
		operation := map[string]any{}
		kind, _ := body["kind"].(string)
		if kind == "" {
			return 0, nil, fmt.Errorf("statestore.mutate needs a `kind`")
		}
		operation["kind"] = kind
		delete(body, "kind")
		for _, name := range []string{"key_base64", "payload_base64", "named"} {
			if value, present := body[name]; present {
				operation[name] = value
				delete(body, name)
			}
		}
		body["op"] = operation
		return wirePost(ctx, client, "/v1/state-stores/mutate", body)
	}
	method, path, ok := endpointFor(op)
	if !ok {
		return 0, nil, fmt.Errorf("unsupported op %v", op)
	}
	return wire(ctx, client, method, path, wireArgs(args))
}

// wireGet/wirePost issue a request and decode the reply body. Non-2xx
// statuses still return the body — `expect` asserts error_status and
// error_code against it.
func wireGet(ctx context.Context, client *Client, path string) (int, any, error) {
	return wire(ctx, client, "GET", path, nil)
}

func wirePost(ctx context.Context, client *Client, path string, body any) (int, any, error) {
	return wire(ctx, client, "POST", path, body)
}

func wire(ctx context.Context, client *Client, method, path string, body any) (int, any, error) {
	status, raw, err := client.transport.wireCall(ctx, method, path, body)
	if err != nil {
		return 0, nil, fmt.Errorf("transport: %w", err)
	}
	var decoded any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return status, nil, fmt.Errorf("decode %s %s body: %w", method, path, err)
		}
	}
	return status, decoded, nil
}

// wireCall is roundTrip with the status and raw body preserved on every
// reply — conformance asserts error_status/error_code against error bodies
// instead of mapping them onto typed errors.
func (t *transport) wireCall(
	ctx context.Context, method, path string, body any,
) (int, json.RawMessage, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(
		ctx, method, t.endpoint()+path, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := t.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, json.RawMessage(raw), nil
}

// -- arg lowering ---------------------------------------------------------

// wireArgs lowers the fixture vocabulary onto the wire: raw key/payload/
// after_key objects become *_base64, metadata becomes metadata_base64,
// nanosecond numbers become decimal strings, and records[] entries
// transform the same way.
func wireArgs(args any) any {
	object, _ := args.(map[string]any)
	if object == nil {
		return args
	}
	out := map[string]any{}
	for key, value := range object {
		out[key] = value
	}
	if records, ok := out["records"].([]any); ok {
		lowered := make([]any, 0, len(records))
		for _, record := range records {
			if row, ok := record.(map[string]any); ok {
				lowered = append(lowered, wireFields(row))
			} else {
				lowered = append(lowered, record)
			}
		}
		out["records"] = lowered
	}
	return wireFields(out)
}

func wireFields(object map[string]any) map[string]any {
	for _, pair := range [][2]string{
		{"key", "key_base64"},
		{"payload", "payload_base64"},
		{"after_key", "after_key_base64"},
	} {
		if value, present := object[pair[0]]; present {
			delete(object, pair[0])
			object[pair[1]] = base64.StdEncoding.EncodeToString(rawBytes(value))
		}
	}
	if metadata, present := object["metadata"]; present {
		delete(object, "metadata")
		encoded := map[string]any{}
		if entries, ok := metadata.(map[string]any); ok {
			for name, value := range entries {
				encoded[name] = base64.StdEncoding.EncodeToString(rawBytes(value))
			}
		}
		object["metadata_base64"] = encoded
	}
	for _, field := range []string{"event_time_ns", "after_event_time_ns", "timestamp_ns"} {
		if number, ok := object[field]; ok {
			switch typed := number.(type) {
			case float64:
				object[field] = strconv.FormatInt(int64(typed), 10)
			}
		}
	}
	return object
}

// rawBytes decodes {"utf8": ..} / {"hex": ..} / {"base64": ..} wrappers; a
// plain string is already utf8 bytes.
func rawBytes(value any) []byte {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return []byte(typed)
	case map[string]any:
		if utf8, ok := typed["utf8"].(string); ok {
			return []byte(utf8)
		}
		if hexText, ok := typed["hex"].(string); ok {
			decoded, _ := hexDecode(hexText)
			return decoded
		}
		if b64, ok := typed["base64"].(string); ok {
			decoded, _ := base64.StdEncoding.DecodeString(b64)
			return decoded
		}
	}
	return nil
}

func hexDecode(text string) ([]byte, error) {
	out := make([]byte, 0, len(text)/2)
	for index := 0; index+1 < len(text); index += 2 {
		value, err := strconv.ParseUint(text[index:index+2], 16, 8)
		if err != nil {
			return out, err
		}
		out = append(out, byte(value))
	}
	return out, nil
}

// -- expect ---------------------------------------------------------------

// checkExpect evaluates one substituted expect map against (status, body);
// empty string means satisfied, anything else is the failure message.
func checkExpect(status int, body any, expect map[string]any, ctx *scenarioContext) string {
	for path, expected := range expect {
		switch path {
		case "error_status":
			want := int(int64Value(expected))
			if status != want {
				return fmt.Sprintf("expected status %d, got %d: %v", want, status, body)
			}
		case "error_code":
			want, _ := expected.(string)
			object, _ := body.(map[string]any)
			got, _ := object["code"].(string)
			if got != want {
				return fmt.Sprintf("expected code %s, got body %v", want, body)
			}
		default:
			actual := digPath(body, path)
			if message := checkPath(path, expected, actual, body, ctx); message != "" {
				return message
			}
		}
	}
	return ""
}

func checkPath(path string, expected, actual, body any, ctx *scenarioContext) string {
	text, isText := expected.(string)
	switch {
	case isText && (text == "present" || text == "!=null"):
		if actual == nil {
			return fmt.Sprintf("%s absent in %v", path, body)
		}
	case isText && text == "absent":
		if actual != nil {
			return fmt.Sprintf("%s present: %v", path, actual)
		}
	case isText && strings.HasPrefix(text, "=="):
		want := digBound(ctx.bindings, text[2:])
		if !looseEquals(want, actual) {
			return fmt.Sprintf("%s = %v, expected %v (%s)", path, actual, want, text)
		}
	case isText && strings.HasPrefix(text, "len>="):
		want, _ := strconv.Atoi(text[5:])
		got := 0
		switch value := actual.(type) {
		case []any:
			got = len(value)
		case string:
			got = len(value)
		case map[string]any:
			got = len(value)
		}
		if got < want {
			return fmt.Sprintf("%s len %d < %d in %v", path, got, want, body)
		}
	default:
		if document, ok := expected.(map[string]any); ok {
			if nested, present := document["base64_json"].(map[string]any); present {
				// Decode the actual base64 string as JSON, then apply each
				// named expectation to the decoded document.
				encoded, _ := actual.(string)
				raw, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					return fmt.Sprintf("%s is not base64: %v", path, actual)
				}
				var decoded any
				if err := json.Unmarshal(raw, &decoded); err != nil {
					return fmt.Sprintf("%s is not base64 JSON: %v", path, actual)
				}
				if message := checkExpect(0, decoded, nested, ctx); message != "" {
					return fmt.Sprintf("%s decoded: %s", path, message)
				}
				return ""
			}
		}
		if !looseEquals(expected, actual) {
			return fmt.Sprintf("%s = %v, expected %v in %v", path, actual, expected, body)
		}
	}
	return ""
}

// -- arg helpers ----------------------------------------------------------

func str(object map[string]any, name string) string {
	value, _ := object[name].(string)
	return value
}

func int64Arg(object map[string]any, name string) int64 {
	return int64Value(object[name])
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	case int64:
		return typed
	case int:
		return int64(typed)
	}
	return 0
}

// -- substitution + comparison ---------------------------------------------

func substitute(node any, ctx *scenarioContext) any {
	switch typed := node.(type) {
	case map[string]any:
		copy := map[string]any{}
		for key, value := range typed {
			copy[key] = substitute(value, ctx)
		}
		return copy
	case []any:
		copy := make([]any, 0, len(typed))
		for _, item := range typed {
			copy = append(copy, substitute(item, ctx))
		}
		return copy
	case string:
		if strings.HasPrefix(typed, "{{") && strings.HasSuffix(typed, "}}") && !strings.Contains(typed[2:], "{{") {
			if value, ok := resolve(typed, ctx); ok {
				return value
			}
			return "null"
		}
		return replaceTokens(typed, ctx)
	default:
		return node
	}
}

// replaceTokens substitutes {{...}} occurrences inside a longer string,
// stringifying resolved values and emitting "null" for unresolvable ones.
func replaceTokens(text string, ctx *scenarioContext) string {
	var out strings.Builder
	rest := text
	for {
		start := strings.Index(rest, "{{")
		if start < 0 {
			break
		}
		out.WriteString(rest[:start])
		end := strings.Index(rest[start:], "}}")
		if end < 0 {
			out.WriteString(rest[start:])
			return out.String()
		}
		end += start
		token := rest[start : end+2]
		if value, ok := resolve(token, ctx); ok {
			out.WriteString(fmt.Sprint(value))
		} else {
			out.WriteString("null")
		}
		rest = rest[end+2:]
	}
	out.WriteString(rest)
	return out.String()
}

func resolve(template string, ctx *scenarioContext) (any, bool) {
	if !strings.HasPrefix(template, "{{") || !strings.HasSuffix(template, "}}") {
		return nil, false
	}
	inner := strings.TrimSpace(template[2 : len(template)-2])
	switch {
	case inner == "uniq":
		return ctx.token, true
	case inner == "uuid":
		return newUUID(), true
	case strings.HasPrefix(inner, "uuid:"):
		name := inner[5:]
		if _, ok := ctx.namedIDs[name]; !ok {
			ctx.namedIDs[name] = newUUID()
		}
		return ctx.namedIDs[name], true
	case inner == "now_ns" || strings.HasPrefix(inner, "now_ns:"):
		// Wall-clock epoch nanoseconds as a decimal string; an optional
		// signed offset shifts the instant (e.g. {{now_ns:-250000000}}).
		shift := int64(0)
		if suffix := strings.TrimPrefix(inner, "now_ns"); strings.HasPrefix(suffix, ":") {
			shift, _ = strconv.ParseInt(suffix[1:], 10, 64)
		}
		now := time.Now().UnixNano() + shift
		return strconv.FormatInt(now, 10), true
	default:
		bound := digBound(ctx.bindings, inner)
		if bound == nil {
			return nil, false
		}
		return bound, true
	}
}

// digBound resolves "bind.field.path" against the scenario's bindings:
// the first segment is the bound name, the rest walks the result.
func digBound(bindings map[string]any, path string) any {
	dot := strings.Index(path, ".")
	head := path
	if dot >= 0 {
		head = path[:dot]
	}
	current, present := bindings[head]
	if !present {
		return nil
	}
	if dot < 0 {
		return current
	}
	return digPath(current, path[dot+1:])
}

// digPath walks "a.b[0].c" — plus ["key"] quoted brackets for field names
// that themselves contain dots — against a decoded JSON value.
func digPath(root any, path string) any {
	for _, segment := range splitPath(path) {
		if root == nil {
			return nil
		}
		name := segment
		var indices []string
		if bracket := strings.Index(segment, "["); bracket >= 0 {
			name = segment[:bracket]
			rest := segment[bracket:]
			for len(rest) > 0 && strings.HasPrefix(rest, "[") {
				close := strings.Index(rest, "]")
				if close < 0 {
					return nil
				}
				indices = append(indices, rest[1:close])
				rest = rest[close+1:]
			}
		}
		if name != "" {
			object, ok := root.(map[string]any)
			if !ok {
				return nil
			}
			root = object[name]
		}
		for _, index := range indices {
			if quoted, ok := unquote(index); ok {
				object, isObject := root.(map[string]any)
				if !isObject {
					return nil
				}
				root = object[quoted]
				continue
			}
			position, err := strconv.Atoi(index)
			array, isArray := root.([]any)
			if err != nil || !isArray || position < 0 || position >= len(array) {
				return nil
			}
			root = array[position]
		}
	}
	return root
}

// splitPath splits on dots that are not inside ["..."] brackets.
func splitPath(path string) []string {
	var segments []string
	depth := 0
	start := 0
	for index := 0; index < len(path); index++ {
		switch path[index] {
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		case '.':
			if depth == 0 {
				segments = append(segments, path[start:index])
				start = index + 1
			}
		}
	}
	return append(segments, path[start:])
}

func unquote(token string) (string, bool) {
	if len(token) >= 2 && strings.HasPrefix(token, `"`) && strings.HasSuffix(token, `"`) {
		return token[1 : len(token)-1], true
	}
	return "", false
}

func looseEquals(expected, actual any) bool {
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}
	if number, ok := toInt(expected); ok {
		other, otherOK := toInt(actual)
		return otherOK && number == other
	}
	if reflect.TypeOf(expected) != reflect.TypeOf(actual) {
		return false
	}
	return reflect.DeepEqual(expected, actual)
}

func toInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	}
	return 0, false
}
