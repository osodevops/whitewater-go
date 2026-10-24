package testkit

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Control plane: POST /v1/admin/wcl executes a WCL script and
// POST /v1/admin/commands executes typed commands. Both reply in the
// ControlExecution envelope — request_id, revision, authority, and one
// StatementResult per entry — and track the declared resource names
// the data plane resolves against.

// stmtError is a refused statement/control command.
type stmtError struct{ msg string }

func (e *stmtError) Error() string { return e.msg }

func refusef(format string, args ...any) *stmtError {
	return &stmtError{msg: fmt.Sprintf(format, args...)}
}

// wcl handles POST /v1/admin/wcl: split the script on top-level
// semicolons, execute each statement, and stop at the first refusal
// (the whole request fails, like the Control Plane).
func (r *Riverbed) wcl(w http.ResponseWriter, body map[string]any) {
	requestID := strOf(body, "request_id")
	if requestID == "" {
		requestID = newUUID()
	}
	statements, err := splitStatements(strOf(body, "script"))
	if err != nil {
		refused(w, "%v", err)
		return
	}
	if len(statements) == 0 {
		refused(w, "empty WCL script")
		return
	}
	results := make([]any, 0, len(statements))
	for _, statement := range statements {
		result, serr := r.execStatement(statement)
		if serr != nil {
			refused(w, "%v", serr)
			return
		}
		results = append(results, result)
	}
	r.revision++
	writeJSON(w, http.StatusOK, map[string]any{
		"request_id": requestID,
		"revision":   r.revision,
		"authority":  "control_plane",
		"results":    results,
	})
}

// commands handles POST /v1/admin/commands: one typed command per
// result entry, first refusal fails the batch.
func (r *Riverbed) commands(w http.ResponseWriter, body map[string]any) {
	requestID := strOf(body, "request_id")
	if requestID == "" {
		requestID = newUUID()
	}
	raw, _ := body["commands"].([]any)
	if len(raw) == 0 {
		refused(w, "no commands supplied")
		return
	}
	results := make([]any, 0, len(raw))
	for _, item := range raw {
		command, ok := item.(map[string]any)
		if !ok {
			refused(w, "command entry is not an object")
			return
		}
		data, serr := r.execCommand(command)
		if serr != nil {
			refused(w, "%v", serr)
			return
		}
		results = append(results, map[string]any{
			"statement": strOf(command, "command"),
			"message":   "ok",
			"data":      data,
		})
	}
	r.revision++
	writeJSON(w, http.StatusOK, map[string]any{
		"request_id": requestID,
		"revision":   r.revision,
		"authority":  "control_plane",
		"results":    results,
	})
}

// ---------------------------------------------------------------------------
// WCL script parsing
// ---------------------------------------------------------------------------

// splitStatements cuts a WCL script on top-level semicolons, honoring
// single-quoted literals with ” escapes (START AT CURSOR 'x').
func splitStatements(script string) ([]string, error) {
	var statements []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(script); i++ {
		ch := script[i]
		switch {
		case ch == '\'':
			if inQuote && i+1 < len(script) && script[i+1] == '\'' {
				current.WriteByte(ch)
				i++
			} else {
				inQuote = !inQuote
			}
			current.WriteByte(ch)
		case ch == ';' && !inQuote:
			if s := strings.TrimSpace(current.String()); s != "" {
				statements = append(statements, s)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quote in WCL script")
	}
	if s := strings.TrimSpace(current.String()); s != "" {
		statements = append(statements, s)
	}
	return statements, nil
}

// execStatement parses one WCL statement. The leading verb becomes the
// StatementResult.statement the fixtures assert on.
func (r *Riverbed) execStatement(statement string) (map[string]any, error) {
	fields := strings.Fields(statement)
	if len(fields) == 0 {
		return nil, refusef("empty statement")
	}
	verb := strings.ToUpper(fields[0])
	result := func(data any) map[string]any {
		return map[string]any{"statement": verb, "message": "ok", "data": data}
	}

	switch verb {
	case "CREATE":
		return r.wclCreate(result, fields)
	case "OPEN":
		return r.wclOpen(result, fields)
	case "SEEK":
		return r.wclSeek(result, fields)
	case "DESCRIBE":
		if len(fields) != 3 {
			return nil, refusef("DESCRIBE takes <kind> <name>")
		}
		data, err := r.describe(strings.ToUpper(fields[1]), fields[2])
		if err != nil {
			return nil, err
		}
		return result(data), nil
	case "RENAME":
		if len(fields) != 5 || strings.ToUpper(fields[3]) != "TO" {
			return nil, refusef("RENAME takes <kind> <name> TO <renamed>")
		}
		if err := r.rename(strings.ToUpper(fields[1]), fields[2], fields[4]); err != nil {
			return nil, err
		}
		return result(map[string]any{"name": fields[4]}), nil
	case "DROP":
		if len(fields) != 3 {
			return nil, refusef("DROP takes <kind> <name>")
		}
		if err := r.drop(strings.ToUpper(fields[1]), fields[2]); err != nil {
			return nil, err
		}
		return result(map[string]any{}), nil
	case "SHOW":
		if len(fields) != 2 {
			return nil, refusef("SHOW takes a plural kind")
		}
		return result(r.show(strings.ToUpper(fields[1]))), nil
	case "DEFINE":
		return r.wclDefine(result, fields)
	default:
		return nil, refusef("testkit: unsupported WCL statement %q", statement)
	}
}

// wclCreate dispatches CREATE <kind> forms.
func (r *Riverbed) wclCreate(
	result func(any) map[string]any, fields []string,
) (map[string]any, error) {
	if len(fields) < 3 {
		return nil, refusef("CREATE takes a kind and a name")
	}
	kind := strings.ToUpper(fields[1])
	switch kind {
	case "DOMAIN":
		name := fields[2]
		if r.domains[name] {
			return nil, refusef("domain %q already exists", name)
		}
		r.domains[name] = true
		return result(map[string]any{"name": name}), nil
	case "FEED":
		feed, err := r.declareFeed(fields[2])
		if err != nil {
			return nil, err
		}
		return result(map[string]any{"name": feed.name, "feed_id": feed.id}), nil
	case "WRITER":
		// CREATE WRITER <name> TO <feed>
		if len(fields) != 5 || strings.ToUpper(fields[3]) != "TO" {
			return nil, refusef("CREATE WRITER takes <name> TO <feed>")
		}
		writer, err := r.declareWriter(fields[2], fields[4])
		if err != nil {
			return nil, err
		}
		return result(map[string]any{
			"name": fields[2], "writer_id": writer.id, "feed": writer.feed}), nil
	case "READER", "SUBSCRIPTION":
		// CREATE <kind> <name> FROM <feed> START AT <operand>
		if len(fields) < 8 || strings.ToUpper(fields[3]) != "FROM" ||
			strings.ToUpper(fields[5]) != "START" || strings.ToUpper(fields[6]) != "AT" {
			return nil, refusef(
				"CREATE %s takes <name> FROM <feed> START AT <start>", kind)
		}
		start, err := r.startPosition(fields[4], strings.Join(fields[7:], " "))
		if err != nil {
			return nil, err
		}
		if kind == "READER" {
			reader, err := r.declareReader(fields[2], fields[4], start)
			if err != nil {
				return nil, err
			}
			return result(map[string]any{
				"name": fields[2], "reader_id": reader.id, "feed": reader.feed}), nil
		}
		sub, err := r.declareSubscription(fields[2], fields[4], start)
		if err != nil {
			return nil, err
		}
		return result(map[string]any{
			"name": fields[2], "subscription_id": sub.id, "feed": sub.feed}), nil
	case "ROLE":
		name := fields[2]
		if r.roles[name] {
			return nil, refusef("role %q already exists", name)
		}
		r.roles[name] = true
		return result(map[string]any{"name": name}), nil
	default:
		return nil, refusef("testkit: unsupported CREATE kind %q", kind)
	}
}

// wclOpen dispatches OPEN WRITER SESSION <name>: the epoch mints here
// and fences every subsequent append from older sessions.
func (r *Riverbed) wclOpen(
	result func(any) map[string]any, fields []string,
) (map[string]any, error) {
	if len(fields) != 4 || strings.ToUpper(fields[1]) != "WRITER" ||
		strings.ToUpper(fields[2]) != "SESSION" {
		return nil, refusef("OPEN takes WRITER SESSION <name>")
	}
	writer := r.writers[fields[3]]
	if writer == nil {
		return nil, refusef("writer %q not found", fields[3])
	}
	r.epochs++
	writer.epoch = r.epochs
	return result(map[string]any{
		"session_epoch":  writer.epoch,
		"session_active": true,
	}), nil
}

// wclSeek dispatches SEEK READER <name> TO <start>: durable progress
// and any delivered frontier both move.
func (r *Riverbed) wclSeek(
	result func(any) map[string]any, fields []string,
) (map[string]any, error) {
	if len(fields) < 5 || strings.ToUpper(fields[1]) != "READER" ||
		strings.ToUpper(fields[3]) != "TO" {
		return nil, refusef("SEEK takes READER <name> TO <start>")
	}
	reader := r.readers[fields[2]]
	if reader == nil {
		return nil, refusef("reader %q not found", fields[2])
	}
	start, err := r.startPosition(reader.feed, strings.Join(fields[4:], " "))
	if err != nil {
		return nil, err
	}
	reader.acknowledged = start
	reader.delivered = start
	return result(map[string]any{"name": fields[2]}), nil
}

// wclDefine dispatches DEFINE STATESTORE <name> [KEYS k1, k2 UNIQUE].
func (r *Riverbed) wclDefine(
	result func(any) map[string]any, fields []string,
) (map[string]any, error) {
	if len(fields) < 3 || strings.ToUpper(fields[1]) != "STATESTORE" {
		return nil, refusef("DEFINE takes STATESTORE <name> [KEYS ...]")
	}
	named := map[string]bool{}
	if len(fields) > 3 {
		if len(fields) < 5 || strings.ToUpper(fields[3]) != "KEYS" {
			return nil, refusef("DEFINE STATESTORE takes [KEYS ...]")
		}
		for _, item := range strings.Split(strings.Join(fields[4:], " "), ",") {
			parts := strings.Fields(item)
			if len(parts) == 0 {
				continue
			}
			unique := len(parts) > 1 && strings.ToUpper(parts[1]) == "UNIQUE"
			named[parts[0]] = unique
		}
	}
	store, err := r.declareStore(fields[2], true, named)
	if err != nil {
		return nil, err
	}
	return result(map[string]any{
		"name": store.name, "store_id": store.id, "stage": "declared"}), nil
}

// startPosition resolves a START AT operand to a record position in
// the named feed: BEGINNING, NOW, CURSOR '<opaque>', or TIMESTAMP <ns>.
func (r *Riverbed) startPosition(feedName, operand string) (int, error) {
	feed := r.feeds[feedName]
	if feed == nil {
		return 0, refusef("feed %q not found", feedName)
	}
	text := strings.TrimSpace(operand)
	upper := strings.ToUpper(text)
	switch {
	case upper == "BEGINNING":
		return 0, nil
	case upper == "NOW":
		return len(feed.records), nil
	case strings.HasPrefix(upper, "CURSOR"):
		literal := strings.TrimSpace(text[len("CURSOR"):])
		if len(literal) >= 2 && literal[0] == '\'' && literal[len(literal)-1] == '\'' {
			literal = literal[1 : len(literal)-1]
		}
		literal = strings.ReplaceAll(literal, "''", "'")
		idx, ok := feed.byCursor[literal]
		if !ok {
			return 0, refusef("unknown cursor %q on feed %q", literal, feedName)
		}
		return idx + 1, nil
	case strings.HasPrefix(upper, "TIMESTAMP"):
		var nanos int64
		if _, err := fmt.Sscan(text[len("TIMESTAMP"):], &nanos); err != nil {
			return 0, refusef("bad TIMESTAMP operand %q", operand)
		}
		return firstAfterEventTime(feed, nanos), nil
	default:
		return 0, refusef("bad START AT operand %q", operand)
	}
}

// firstAfterEventTime returns the index of the first record whose
// event_time_ns is strictly after nanos.
func firstAfterEventTime(feed *fakeFeed, nanos int64) int {
	for i, record := range feed.records {
		if record.eventTimeNs > nanos {
			return i
		}
	}
	return len(feed.records)
}

// ---------------------------------------------------------------------------
// Catalog verbs shared by WCL and typed commands
// ---------------------------------------------------------------------------

func (r *Riverbed) declareFeed(name string) (*fakeFeed, error) {
	if feed := r.feeds[name]; feed != nil {
		return nil, refusef("feed %q already exists", name)
	}
	feed := &fakeFeed{
		id: newUUID(), name: name,
		byCursor: map[string]int{}, adHoc: map[string]*adHocWriter{},
	}
	r.feeds[name] = feed
	// A Feed's first dotted segment is its Domain; declare it so
	// SHOW/DESCRIBE stay coherent when callers skip CREATE DOMAIN.
	if dot := strings.IndexByte(name, '.'); dot > 0 {
		r.domains[name[:dot]] = true
	}
	return feed, nil
}

func (r *Riverbed) declareWriter(name, feed string) (*fakeWriter, error) {
	if r.writers[name] != nil {
		return nil, refusef("writer %q already exists", name)
	}
	if r.feeds[feed] == nil {
		return nil, refusef("feed %q not found", feed)
	}
	writer := &fakeWriter{id: newUUID(), feed: feed, dedup: map[string]*appendDedup{}}
	r.writers[name] = writer
	return writer, nil
}

func (r *Riverbed) declareReader(name, feed string, start int) (*fakeReader, error) {
	if r.readers[name] != nil {
		return nil, refusef("reader %q already exists", name)
	}
	if r.feeds[feed] == nil {
		return nil, refusef("feed %q not found", feed)
	}
	reader := &fakeReader{id: newUUID(), feed: feed, acknowledged: start, delivered: start}
	r.readers[name] = reader
	return reader, nil
}

func (r *Riverbed) declareSubscription(
	name, feed string, start int,
) (*fakeSubscription, error) {
	if r.subscriptions[name] != nil {
		return nil, refusef("subscription %q already exists", name)
	}
	if r.feeds[feed] == nil {
		return nil, refusef("feed %q not found", feed)
	}
	sub := &fakeSubscription{id: newUUID(), feed: feed}
	r.subscriptions[name] = sub
	return sub, nil
}

func (r *Riverbed) declareStore(
	name string, manual bool, named map[string]bool,
) (*fakeStore, error) {
	if r.tombstones[name] {
		return nil, refusef("state store %q was dropped", name)
	}
	if r.stores[name] != nil {
		return nil, refusef("state store %q already exists", name)
	}
	store := &fakeStore{
		id: newUUID(), name: name, manual: manual, namedKeys: named,
		rows: map[string]*fakeRow{}, mutations: map[string]*mutationDedup{},
	}
	r.stores[name] = store
	if dot := strings.IndexByte(name, '.'); dot > 0 {
		r.domains[name[:dot]] = true
	}
	return store, nil
}

// describe renders the catalog row for DESCRIBE <kind> and the typed
// describe command alike.
func (r *Riverbed) describe(kind, name string) (any, error) {
	switch kind {
	case "DOMAIN":
		if !r.domains[name] {
			return nil, refusef("domain %q not found", name)
		}
		return map[string]any{"name": name}, nil
	case "FEED":
		feed := r.feeds[name]
		if feed == nil {
			return nil, refusef("feed %q not found", name)
		}
		return map[string]any{"name": name, "feed_id": feed.id}, nil
	case "WRITER":
		writer := r.writers[name]
		if writer == nil {
			return nil, refusef("writer %q not found", name)
		}
		return map[string]any{
			"name": name, "writer_id": writer.id, "feed": writer.feed,
			"session_epoch": writer.epoch,
		}, nil
	case "READER":
		reader := r.readers[name]
		if reader == nil {
			return nil, refusef("reader %q not found", name)
		}
		return map[string]any{
			"name": name, "reader_id": reader.id, "feed": reader.feed,
		}, nil
	case "SUBSCRIPTION":
		sub := r.subscriptions[name]
		if sub == nil {
			return nil, refusef("subscription %q not found", name)
		}
		return map[string]any{
			"name": name, "subscription_id": sub.id, "feed": sub.feed,
		}, nil
	case "ROLE":
		if !r.roles[name] {
			return nil, refusef("role %q not found", name)
		}
		return map[string]any{"name": name}, nil
	case "STATESTORE", "STATE_STORE":
		store := r.stores[name]
		if store == nil || r.tombstones[name] {
			return nil, refusef("state store %q not found", name)
		}
		return map[string]any{
			"name": name, "store_id": store.id, "stage": "declared",
		}, nil
	case "PIPE":
		return r.describePipe(name)
	default:
		return nil, refusef("testkit: unsupported DESCRIBE kind %q", kind)
	}
}

func (r *Riverbed) describePipe(name string) (any, error) {
	pipe := r.pipes[name]
	if pipe == nil {
		return nil, refusef("pipe %q not found", name)
	}
	return pipe.row(), nil
}

// row renders the catalog document PipeInfo decodes.
func (p *fakePipe) row() map[string]any {
	row := map[string]any{
		"name": p.name, "stage": p.stage, "subscription_id": newUUID(),
		"operation": p.operation, "status": "active",
		"created_at_ns": p.createdAtNs,
	}
	if p.outputFeed != "" {
		row["output_feed_id"] = newUUID()
	}
	if p.outputStore != "" {
		row["output_store_id"] = newUUID()
	}
	return row
}

// rename moves a catalog entry to a new name (feeds carry identity, so
// reader/writer bindings survive).
func (r *Riverbed) rename(kind, current, renamed string) error {
	switch kind {
	case "DOMAIN":
		if !r.domains[current] {
			return refusef("domain %q not found", current)
		}
		delete(r.domains, current)
		r.domains[renamed] = true
	case "FEED":
		feed := r.feeds[current]
		if feed == nil {
			return refusef("feed %q not found", current)
		}
		delete(r.feeds, current)
		feed.name = renamed
		r.feeds[renamed] = feed
	case "WRITER":
		writer := r.writers[current]
		if writer == nil {
			return refusef("writer %q not found", current)
		}
		delete(r.writers, current)
		r.writers[renamed] = writer
	case "READER":
		reader := r.readers[current]
		if reader == nil {
			return refusef("reader %q not found", current)
		}
		delete(r.readers, current)
		r.readers[renamed] = reader
	case "SUBSCRIPTION":
		sub := r.subscriptions[current]
		if sub == nil {
			return refusef("subscription %q not found", current)
		}
		delete(r.subscriptions, current)
		r.subscriptions[renamed] = sub
	case "ROLE":
		if !r.roles[current] {
			return refusef("role %q not found", current)
		}
		delete(r.roles, current)
		r.roles[renamed] = true
	default:
		return refusef("testkit: unsupported RENAME kind %q", kind)
	}
	return nil
}

// drop retires a catalog entry; stores tombstone so the name cannot be
// recreated, matching the real lifecycle.
func (r *Riverbed) drop(kind, name string) error {
	switch kind {
	case "DOMAIN":
		if !r.domains[name] {
			return refusef("domain %q not found", name)
		}
		delete(r.domains, name)
	case "FEED":
		if r.feeds[name] == nil {
			return refusef("feed %q not found", name)
		}
		delete(r.feeds, name)
	case "WRITER":
		if r.writers[name] == nil {
			return refusef("writer %q not found", name)
		}
		delete(r.writers, name)
	case "READER":
		if r.readers[name] == nil {
			return refusef("reader %q not found", name)
		}
		delete(r.readers, name)
	case "SUBSCRIPTION":
		if r.subscriptions[name] == nil {
			return refusef("subscription %q not found", name)
		}
		delete(r.subscriptions, name)
	case "ROLE":
		if !r.roles[name] {
			return refusef("role %q not found", name)
		}
		delete(r.roles, name)
	case "STATESTORE", "STATE_STORE":
		if r.stores[name] == nil {
			return refusef("state store %q not found", name)
		}
		delete(r.stores, name)
		r.tombstones[name] = true
	case "PIPE":
		if r.pipes[name] == nil {
			return refusef("pipe %q not found", name)
		}
		delete(r.pipes, name)
	default:
		return refusef("testkit: unsupported DROP kind %q", kind)
	}
	return nil
}

// show renders SHOW <plural> as a bare row list.
func (r *Riverbed) show(plural string) []any {
	rows := []any{}
	switch plural {
	case "DOMAINS":
		for name := range r.domains {
			rows = append(rows, map[string]any{"name": name})
		}
	case "FEEDS":
		for _, feed := range r.feeds {
			rows = append(rows, map[string]any{"name": feed.name, "feed_id": feed.id})
		}
	case "WRITERS":
		for name, writer := range r.writers {
			rows = append(rows, map[string]any{
				"name": name, "writer_id": writer.id, "feed": writer.feed})
		}
	case "READERS":
		for name, reader := range r.readers {
			rows = append(rows, map[string]any{
				"name": name, "reader_id": reader.id, "feed": reader.feed})
		}
	case "SUBSCRIPTIONS":
		for name, sub := range r.subscriptions {
			rows = append(rows, map[string]any{
				"name": name, "subscription_id": sub.id, "feed": sub.feed})
		}
	case "ROLES":
		for name := range r.roles {
			rows = append(rows, map[string]any{"name": name})
		}
	case "STATESTORES", "STATE_STORES":
		for _, store := range r.stores {
			rows = append(rows, map[string]any{
				"name": store.name, "store_id": store.id, "stage": "declared"})
		}
	case "PIPES":
		for _, pipe := range r.pipes {
			rows = append(rows, pipe.row())
		}
	}
	return rows
}

// ---------------------------------------------------------------------------
// Typed commands
// ---------------------------------------------------------------------------

// execCommand runs one typed command document and returns its
// StatementResult data.
func (r *Riverbed) execCommand(command map[string]any) (any, error) {
	switch strOf(command, "command") {
	case "define_state_store":
		name := strOf(command, "name")
		manual := true
		if source, ok := command["source"].(map[string]any); ok {
			manual = strOf(source, "kind") == "manual"
		}
		named := map[string]bool{}
		if list, ok := command["named_keys"].([]any); ok {
			for _, item := range list {
				if entry, ok := item.(map[string]any); ok {
					named[strOf(entry, "name")] = boolOf(entry, "unique")
				}
			}
		}
		store, err := r.declareStore(name, manual, named)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"name": store.name, "store_id": store.id, "stage": "declared"}, nil
	case "define_pipe":
		name := strOf(command, "name")
		if r.pipes[name] != nil {
			return nil, refusef("pipe %q already exists", name)
		}
		r.pipes[name] = &fakePipe{
			name:         name,
			subscription: strOf(command, "subscription"),
			operation:    command["operation"],
			outputFeed:   strOf(command, "output_feed"),
			outputStore:  strOf(command, "output_store"),
			stage:        "declared",
			createdAtNs:  time.Now().UnixNano(),
		}
		return map[string]any{"name": name, "stage": "declared"}, nil
	case "describe":
		return r.describe(
			strings.ToUpper(strOf(command, "kind")), strOf(command, "name"))
	case "show":
		return r.show(strings.ToUpper(strOf(command, "kind"))), nil
	case "drop":
		if err := r.drop(
			strings.ToUpper(strOf(command, "kind")), strOf(command, "name")); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "pause_pipe", "resume_pipe":
		pipe := r.pipes[strOf(command, "name")]
		if pipe == nil {
			return nil, refusef("pipe %q not found", strOf(command, "name"))
		}
		if strOf(command, "command") == "pause_pipe" {
			pipe.stage = "paused"
		} else {
			pipe.stage = "active"
		}
		return map[string]any{}, nil
	default:
		// Lifecycle commands outside the modeled surface still commit —
		// the fake's job is to keep data-plane tests running.
		return map[string]any{}, nil
	}
}
