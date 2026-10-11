package whitewater

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Executes the shared fixture corpus under sdk/spec/conformance/ in
// whitewater against a live Riverbed, per conformance/README.md. Skipped
// unless WHITEWATER_CONFORMANCE_URLS and WHITEWATER_FIXTURE_DIR are set.
//
//	go test -run Conformance -v .
func TestSharedConformanceCorpus(t *testing.T) {
	baseURLs := os.Getenv("WHITEWATER_CONFORMANCE_URLS")
	fixtureDir := os.Getenv("WHITEWATER_FIXTURE_DIR")
	if baseURLs == "" || fixtureDir == "" {
		t.Skip("set WHITEWATER_CONFORMANCE_URLS and WHITEWATER_FIXTURE_DIR")
	}
	apiKey := os.Getenv("FINNSTREAM_ADMIN_API_KEY")
	if apiKey == "" {
		apiKey = "whitewater-local-development-admin-key"
	}
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
			runScenario(t, client, scenario)
		})
	}
}

type scenarioContext struct {
	token    string
	namedIDs map[string]string
	bindings map[string]any
}

func runScenario(t *testing.T, client *Client, scenario map[string]any) {
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
		errorCode, _ := expect["error_code"].(string)
		errorStatus, hasErrorStatus := expect["error_status"].(float64)
		expectsError := errorCode != "" || hasErrorStatus
		call := substitute(step, ctx).(map[string]any)

		result, err := callOp(context.Background(), client, call)
		if expectsError {
			if err == nil {
				t.Fatalf("%s: step %d (%s) succeeded, expected error %v",
					name, index+1, op, expect)
			}
			var wwErr *Error
			if !errors.As(err, &wwErr) {
				t.Fatalf("%s: step %d (%s) failed without a typed error: %v",
					name, index+1, op, err)
			}
			if errorCode != "" && wwErr.Code != errorCode {
				t.Fatalf("%s: step %d (%s) failed with %q, expected %q",
					name, index+1, op, wwErr.Code, errorCode)
			}
			if hasErrorStatus && wwErr.Status != int(errorStatus) {
				t.Fatalf("%s: step %d (%s) failed with HTTP %d, expected %d",
					name, index+1, op, wwErr.Status, int(errorStatus))
			}
			result = map[string]any{"error_code": wwErr.Code}
		} else if err != nil {
			t.Fatalf("%s: step %d (%s) failed: %v", name, index+1, op, err)
		}
		if bind, ok := step["bind"].(string); ok {
			ctx.bindings[bind] = result
		}
		for path, expectedRaw := range expect {
			if path == "error_code" || path == "error_status" {
				continue
			}
			expected := substitute(expectedRaw, ctx)
			checkExpectation(t, name, index+1, op, path, expected, result, ctx)
		}
	}
}

func checkExpectation(
	t *testing.T, name string, index int, op, path string,
	expected, result any, ctx *scenarioContext,
) {
	t.Helper()
	actual := digPath(result, path)
	fail := func(format string, args ...any) {
		t.Fatalf("%s: step %d (%s) expectation %q failed: %s",
			name, index, op, path, fmt.Sprintf(format, args...))
	}
	text, isText := expected.(string)
	switch {
	case isText && text == "!=null":
		if actual == nil {
			fail("expected non-null")
		}
	case isText && text == "present":
		if actual == nil {
			fail("expected present")
		}
	case isText && text == "absent":
		if actual != nil {
			fail("expected absent, got %v", actual)
		}
	case isText && strings.HasPrefix(text, "len>="):
		bound, _ := strconv.Atoi(text[5:])
		length := -1
		switch value := actual.(type) {
		case []any:
			length = len(value)
		case map[string]any:
			length = len(value)
		case string:
			length = len(value)
		}
		if length < bound {
			fail("expected len>=%d, got %d", bound, length)
		}
	case isText && strings.HasPrefix(text, "=="):
		bound := digBound(ctx.bindings, text[2:])
		if !looseEquals(bound, actual) {
			fail("expected %s=%v, got %v", text, bound, actual)
		}
	default:
		if !looseEquals(expected, actual) {
			fail("expected %v, got %v", expected, actual)
		}
	}
}

// -- operations ---------------------------------------------------------

func callOp(ctx context.Context, client *Client, step map[string]any) (any, error) {
	args, _ := step["args"].(map[string]any)
	switch step["op"] {
	case "wcl":
		execution, err := client.WCL(ctx, str(args, "script"))
		if err != nil {
			return nil, err
		}
		return bindExecution(execution), nil
	case "admin.commands":
		commands, _ := args["commands"].([]any)
		typed := make([]map[string]any, 0, len(commands))
		for _, command := range commands {
			if object, ok := command.(map[string]any); ok {
				typed = append(typed, object)
			}
		}
		execution, err := client.Commands(ctx, typed)
		if err != nil {
			return nil, err
		}
		return bindExecution(execution), nil
	case "feed.append":
		record := FeedAppend{
			RequestID:       str(args, "request_id"),
			WriterSessionID: str(args, "writer_session_id"),
			WriterEpoch:     int64Arg(args, "writer_epoch"),
			Sequence:        int64Arg(args, "sequence"),
			Key:             bytesArg(args["key"]),
			Payload:         bytesArg(args["payload"]),
			EventTimeNs:     int64PtrArg(args["event_time_ns"]),
			Metadata:        metadataArg(args["metadata"]),
		}
		result, err := client.Feeds.Append(ctx, str(args, "feed"), record)
		if err != nil {
			return nil, err
		}
		return bindAppendResult(result), nil
	case "reader.temporary_fetch":
		options := TemporaryFetchOptions{
			After:            cursorPtr(str(args, "after")),
			Limit:            int(int64Arg(args, "limit")),
			NewOnly:          args["new_only"] == true,
			Tail:             args["tail"] == true,
			WaitMs:           int(int64Arg(args, "wait_ms")),
			AfterEventTimeNs: int64PtrArg(args["after_event_time_ns"]),
		}
		page, err := client.Feeds.TemporaryFetch(ctx, str(args, "feed"), options)
		if err != nil {
			return nil, err
		}
		var records []any
		for _, record := range page.Records {
			records = append(records, bindRecord(record))
		}
		var nextCursor any
		if page.NextCursor != nil {
			nextCursor = string(*page.NextCursor)
		}
		return map[string]any{"records": records, "next_cursor": nextCursor}, nil
	case "writer.append":
		input := AppendInput{
			RequestID:   str(args, "request_id"),
			Key:         bytesArg(args["key"]),
			Payload:     bytesArg(args["payload"]),
			EventTimeNs: int64PtrArg(args["event_time_ns"]),
			Metadata:    metadataArg(args["metadata"]),
		}
		writer := client.Writer(str(args, "writer"), int64Arg(args, "session_epoch"))
		result, err := writer.Append(ctx, input)
		if err != nil {
			return nil, err
		}
		return bindAppendResult(result), nil
	case "writer.append_batch":
		var inputs []AppendInput
		var writer *WriterSessionClient
		for _, recordRaw := range slice(args["records"]) {
			record, _ := recordRaw.(map[string]any)
			if writer == nil {
				writer = client.Writer(
					str(record, "writer"), int64Arg(record, "session_epoch"))
			}
			inputs = append(inputs, AppendInput{
				RequestID:   str(record, "request_id"),
				Key:         bytesArg(record["key"]),
				Payload:     bytesArg(record["payload"]),
				EventTimeNs: int64PtrArg(record["event_time_ns"]),
				Metadata:    metadataArg(record["metadata"]),
			})
		}
		result, err := writer.AppendBatch(ctx, inputs)
		if err != nil {
			return nil, err
		}
		var results []any
		for _, item := range result.Results {
			results = append(results, bindAppendResult(item))
		}
		return map[string]any{
			"results":  results,
			"feedback": bindFeedback(result.Feedback),
		}, nil
	case "reader.open":
		reader := client.Reader(str(args, "reader"))
		capacity := 64
		if raw, ok := args["capacity"].(float64); ok {
			capacity = int(raw)
		}
		session, err := reader.Open(ctx, capacity)
		if err != nil {
			return nil, err
		}
		return bindSession(session), nil
	case "reader.fetch", "reader.ack", "reader.close":
		reader := client.Reader(str(args, "reader"))
		session := ReaderSession{SessionEpoch: int64Arg(args, "session_epoch")}
		switch step["op"] {
		case "reader.fetch":
			page, err := reader.Fetch(ctx, session, int(int64Arg(args, "limit")))
			if err != nil {
				return nil, err
			}
			return bindPage(page), nil
		case "reader.ack":
			acked, err := reader.Ack(ctx, session, Cursor(str(args, "cursor")))
			if err != nil {
				return nil, err
			}
			return bindSession(acked), nil
		default:
			closed, err := reader.Close(ctx, session)
			if err != nil {
				return nil, err
			}
			return bindSession(closed), nil
		}
	case "member.join":
		joined, err := client.SubscriptionMember(str(args, "subscription")).
			Join(ctx, str(args, "member_id"))
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"member_id":       joined.MemberID,
			"member_epoch":    joined.MemberEpoch,
			"ownership_epoch": joined.OwnershipEpoch,
			"tick":            joined.Tick,
		}, nil
	case "member.claim", "member.renew":
		member := client.SubscriptionMember(str(args, "subscription"))
		joined := joinedArgs(args)
		var lease WorkLease
		var err error
		if step["op"] == "member.claim" {
			lease, err = member.Claim(ctx, joined, str(args, "work_id"),
				int(int64Arg(args, "lease_ticks")))
		} else {
			lease, err = member.Renew(ctx, joined, leaseArgs(args),
				int(int64Arg(args, "lease_ticks")))
		}
		if err != nil {
			return nil, err
		}
		return map[string]any{"lease": bindLease(lease)}, nil
	case "member.fetch":
		member := client.SubscriptionMember(str(args, "subscription"))
		page, err := member.Fetch(ctx, joinedArgs(args), leaseArgs(args),
			int(int64Arg(args, "limit")))
		if err != nil {
			return nil, err
		}
		return bindMemberPage(page), nil
	case "member.ack":
		member := client.SubscriptionMember(str(args, "subscription"))
		page := MemberPage{
			Cursor:    cursorPtr(str(args, "cursor")),
			Positions: positionsArg(args["positions"]),
		}
		if err := member.Ack(ctx, joinedArgs(args), leaseArgs(args), page); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "member.release":
		member := client.SubscriptionMember(str(args, "subscription"))
		if err := member.Release(ctx, joinedArgs(args), leaseArgs(args)); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "feed.records":
		path := "/v1/feeds/records?feed=" + url.QueryEscape(str(args, "feed"))
		if limit := int64Arg(args, "limit"); limit > 0 {
			path += "&limit=" + strconv.FormatInt(limit, 10)
		}
		raw, err := client.transport.get(ctx, path)
		if err != nil {
			return nil, err
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		if object, ok := decoded.(map[string]any); ok {
			return object, nil
		}
		return map[string]any{"records": decoded}, nil
	default:
		return nil, fmt.Errorf("unsupported op %v", step["op"])
	}
}

// -- arg helpers --------------------------------------------------------

func str(object map[string]any, name string) string {
	value, _ := object[name].(string)
	return value
}

func slice(value any) []any {
	array, _ := value.([]any)
	return array
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
	}
	return 0
}

func int64PtrArg(value any) *int64 {
	if value == nil {
		return nil
	}
	parsed := int64Value(value)
	return &parsed
}

func bytesArg(value any) []byte {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		decoded, _ := base64.StdEncoding.DecodeString(typed)
		return decoded
	case map[string]any:
		if utf8, ok := typed["utf8"].(string); ok {
			return []byte(utf8)
		}
		if hexText, ok := typed["hex"].(string); ok {
			decoded, _ := hex.DecodeString(hexText)
			return decoded
		}
		if b64, ok := typed["base64"].(string); ok {
			decoded, _ := base64.StdEncoding.DecodeString(b64)
			return decoded
		}
	}
	return nil
}

func metadataArg(value any) map[string][]byte {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	metadata := map[string][]byte{}
	for name, raw := range object {
		metadata[name] = bytesArg(raw)
	}
	return metadata
}

func positionsArg(value any) map[string]string {
	object, ok := value.(map[string]any)
	if !ok {
		return map[string]string{}
	}
	positions := map[string]string{}
	for name, raw := range object {
		if cursor, ok := raw.(string); ok {
			positions[name] = cursor
		}
	}
	return positions
}

func cursorPtr(value string) *Cursor {
	if value == "" {
		return nil
	}
	cursor := Cursor(value)
	return &cursor
}

func joinedArgs(args map[string]any) Joined {
	return Joined{
		MemberID:    str(args, "member_id"),
		MemberEpoch: int64Arg(args, "member_epoch"),
	}
}

func leaseArgs(args map[string]any) WorkLease {
	return WorkLease{
		WorkID:      str(args, "work_id"),
		MemberID:    str(args, "member_id"),
		MemberEpoch: int64Arg(args, "member_epoch"),
		LeaseEpoch:  int64Arg(args, "lease_epoch"),
	}
}

// -- result -> wire-name bindings ---------------------------------------

func bindFeedback(feedback ServerFeedback) map[string]any {
	return map[string]any{
		"recommended_batch_count": feedback.RecommendedBatchCount,
		"recommended_batch_bytes": feedback.RecommendedBatchBytes,
		"pressure":                feedback.Pressure,
		"retry_after_ms":          feedback.RetryAfterMs,
		"max_frame_bytes":         feedback.MaxFrameBytes,
		"hot_key":                 feedback.HotKey,
	}
}

func bindAppendResult(result AppendResult) map[string]any {
	return map[string]any{
		"message_id":   result.MessageID,
		"cursor":       string(result.Cursor),
		"deduplicated": result.Deduplicated,
		"feedback":     bindFeedback(result.Feedback),
	}
}

func bindSession(session ReaderSession) map[string]any {
	var delivered, acknowledged any
	if session.DeliveredCursor != nil {
		delivered = string(*session.DeliveredCursor)
	}
	if session.AcknowledgedCursor != nil {
		acknowledged = string(*session.AcknowledgedCursor)
	}
	return map[string]any{
		"reader_id":           session.ReaderID,
		"feed_id":             session.FeedID,
		"session_epoch":       session.SessionEpoch,
		"capacity":            session.Capacity,
		"delivered_cursor":    delivered,
		"acknowledged_cursor": acknowledged,
	}
}

func bindRecord(record Record) map[string]any {
	metadata := map[string]any{}
	for name, value := range record.Metadata {
		metadata[name] = base64.StdEncoding.EncodeToString(value)
	}
	var key any
	if record.Key != nil {
		key = base64.StdEncoding.EncodeToString(record.Key)
	}
	return map[string]any{
		"cursor":          string(record.Cursor),
		"message_id":      record.MessageID,
		"producer_id":     record.ProducerID,
		"sequence":        record.Sequence,
		"event_time_ns":   strconv.FormatInt(record.EventTimeNs, 10),
		"ingest_time_ns":  strconv.FormatInt(record.IngestTimeNs, 10),
		"key_base64":      key,
		"payload_base64":  base64.StdEncoding.EncodeToString(record.Payload),
		"metadata_base64": metadata,
	}
}

func bindPage(page ReaderPage) map[string]any {
	var records []any
	for _, record := range page.Records {
		records = append(records, bindRecord(record))
	}
	var delivered, acknowledged any
	if page.DeliveredCursor != nil {
		delivered = string(*page.DeliveredCursor)
	}
	if page.AcknowledgedCursor != nil {
		acknowledged = string(*page.AcknowledgedCursor)
	}
	return map[string]any{
		"records":              records,
		"delivered_cursor":     delivered,
		"acknowledged_cursor":  acknowledged,
		"recommended_capacity": page.RecommendedCapacity,
		"retry_after_ms":       page.RetryAfterMs,
	}
}

func bindLease(lease WorkLease) map[string]any {
	return map[string]any{
		"work_id":         lease.WorkID,
		"member_id":       lease.MemberID,
		"member_epoch":    lease.MemberEpoch,
		"lease_epoch":     lease.LeaseEpoch,
		"expires_at_tick": lease.ExpiresAtTick,
	}
}

func bindMemberPage(page MemberPage) map[string]any {
	var records []any
	for _, record := range page.Records {
		records = append(records, bindRecord(record))
	}
	var cursor any
	if page.Cursor != nil {
		cursor = string(*page.Cursor)
	}
	positions := map[string]any{}
	for name, value := range page.Positions {
		positions[name] = value
	}
	return map[string]any{
		"records":         records,
		"cursor":          cursor,
		"positions":       positions,
		"ownership_epoch": page.OwnershipEpoch,
	}
}

func bindExecution(execution ControlExecution) map[string]any {
	var results []any
	for _, result := range execution.Results {
		var data any
		if len(result.Data) > 0 {
			_ = json.Unmarshal(result.Data, &data)
		}
		results = append(results, map[string]any{
			"statement": result.Statement,
			"message":   result.Message,
			"data":      data,
		})
	}
	return map[string]any{
		"request_id": execution.RequestID,
		"revision":   execution.Revision,
		"authority":  execution.Authority,
		"warning":    execution.Warning,
		"results":    results,
	}
}

// -- substitution + comparison ------------------------------------------

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
		if strings.HasPrefix(typed, "{{") && strings.HasSuffix(typed, "}}") {
			return resolve(typed, ctx)
		}
		return strings.ReplaceAll(typed, "{{uniq}}", ctx.token)
	default:
		return node
	}
}

func resolve(template string, ctx *scenarioContext) any {
	inner := template[2 : len(template)-2]
	switch {
	case inner == "uniq":
		return ctx.token
	case inner == "uuid":
		return newUUID()
	case strings.HasPrefix(inner, "uuid:"):
		name := inner[5:]
		if _, ok := ctx.namedIDs[name]; !ok {
			ctx.namedIDs[name] = newUUID()
		}
		return ctx.namedIDs[name]
	default:
		bound := digBound(ctx.bindings, inner)
		if bound == nil {
			panic(fmt.Sprintf("unresolved token %s", template))
		}
		return bound
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

// digPath walks "a.b[0].c" against a result value.
func digPath(root any, path string) any {
	current := root
	for _, segment := range strings.Split(path, ".") {
		if current == nil {
			return nil
		}
		name := segment
		index := -1
		if strings.HasSuffix(segment, "]") {
			if bracket := strings.Index(segment, "["); bracket >= 0 {
				name = segment[:bracket]
				index, _ = strconv.Atoi(segment[bracket+1 : len(segment)-1])
			}
		}
		if name != "" {
			object, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = object[name]
		}
		if index >= 0 {
			array, ok := current.([]any)
			if !ok || index >= len(array) {
				return nil
			}
			current = array[index]
		}
	}
	return current
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
