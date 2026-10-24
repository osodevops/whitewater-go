package testkit

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
)

// StateStore data plane: /v1/state-stores/mutate journals put_row /
// delete_row ops under mutation_id dedup; lookup/get-by/iterate serve
// the applied rows with freshness evidence; clear empties the store as
// one journaled mutation.

const storeServedBy = "testkit-node-1"

// storeFor resolves the named store for a data-plane op: manual,
// declared, and not tombstoned — anything else refuses.
func (r *Riverbed) storeFor(name string) (*fakeStore, *wireError) {
	store := r.stores[name]
	if store == nil || r.tombstones[name] {
		return nil, wireErr(http.StatusBadRequest, "refused",
			"state store %q not found", name)
	}
	return store, nil
}

// mutationSeq commits one mutation to the store journal under
// mutation_id dedup: identical retries replay the original sequence,
// conflicting reuse is a 409. apply runs only on a fresh commit.
func mutationSeq(
	store *fakeStore, mutationID string, body any, apply func(),
) (int64, bool, *wireError) {
	if mutationID == "" {
		return 0, false, wireErr(http.StatusBadRequest, "refused",
			"mutation_id is required")
	}
	fp := fingerprint(body)
	if seen := store.mutations[mutationID]; seen != nil {
		if seen.fingerprint == fp {
			return seen.sequence, true, nil
		}
		return 0, false, wireErr(http.StatusConflict, "conflict",
			"mutation_id %q reused with different content", mutationID)
	}
	store.seq++
	apply()
	store.mutations[mutationID] = &mutationDedup{fingerprint: fp, sequence: store.seq}
	return store.seq, false, nil
}

func mutationResultBody(store *fakeStore, mutationID string, sequence int64, deduplicated bool) map[string]any {
	return map[string]any{
		"store_id":     store.id,
		"mutation_id":  mutationID,
		"sequence":     sequence,
		"deduplicated": deduplicated,
	}
}

// storeMutate handles POST /v1/state-stores/mutate: put_row upserts a
// primary row plus its named values; delete_row removes it.
func (r *Riverbed) storeMutate(w http.ResponseWriter, body map[string]any) {
	store, serr := r.storeFor(strOf(body, "store"))
	if serr != nil {
		serr.write(w)
		return
	}
	if !store.manual {
		refused(w, "state store %q is feed-sourced; mutate applies to manual stores",
			store.name)
		return
	}
	op, _ := body["op"].(map[string]any)
	kind := strOf(op, "kind")
	if kind != "put_row" && kind != "delete_row" {
		refused(w, "unsupported mutation op kind %q", kind)
		return
	}
	if strOf(op, "key_base64") == "" {
		refused(w, "mutation op %s needs a non-empty key_base64", kind)
		return
	}
	mutationID := strOf(body, "mutation_id")
	sequence, deduplicated, merr := mutationSeq(
		store, mutationID, op, func() {
			switch kind {
			case "put_row":
				r.applyPut(store, op)
			case "delete_row":
				delete(store.rows, strOf(op, "key_base64"))
			}
		})
	if merr != nil {
		merr.write(w)
		return
	}
	writeJSON(w, http.StatusOK,
		mutationResultBody(store, mutationID, sequence, deduplicated))
}

// applyPut lands one put_row op: payload plus the declared named
// values the row carries.
func (r *Riverbed) applyPut(store *fakeStore, op map[string]any) {
	named := map[string]any{}
	if list, ok := op["named"].([]any); ok {
		for _, item := range list {
			entry, ok := item.(map[string]any)
			if !ok {
				continue
			}
			name := strOf(entry, "name")
			if _, declared := store.namedKeys[name]; !declared {
				continue // undeclared named keys are ignored on apply
			}
			named[name] = entry["values"]
		}
	}
	key := strOf(op, "key_base64")
	if key == "" {
		return
	}
	store.rows[key] = &fakeRow{
		payloadB64: strOf(op, "payload_base64"),
		named:      named,
		appliedSeq: store.seq,
	}
}

// freshnessBody stamps the replica evidence every store read reports;
// the fake is never behind.
func (r *Riverbed) freshnessBody(store *fakeStore) map[string]any {
	return map[string]any{
		"applied_seq":   store.seq,
		"committed_seq": store.seq,
		"behind":        false,
		"served_by":     storeServedBy,
	}
}

// appliedCursor renders a row's applied journal position the way the
// wire carries it — a zero-padded decimal cursor.
func appliedCursor(sequence int64) string {
	return fmt.Sprintf("%020d", sequence)
}

// storeLookup handles POST /v1/state-stores/lookup: the primary-key
// point read.
func (r *Riverbed) storeLookup(w http.ResponseWriter, body map[string]any) {
	store, serr := r.storeFor(strOf(body, "store"))
	if serr != nil {
		serr.write(w)
		return
	}
	reply := r.freshnessBody(store)
	reply["store_id"] = store.id
	row := store.rows[strOf(body, "key_base64")]
	if row == nil {
		reply["found"] = false
		reply["payload_base64"] = nil
		reply["applied_cursor"] = nil
	} else {
		reply["found"] = true
		reply["payload_base64"] = row.payloadB64
		reply["applied_cursor"] = appliedCursor(row.appliedSeq)
	}
	writeJSON(w, http.StatusOK, reply)
}

// storeGetBy handles POST /v1/state-stores/get-by: a declared named
// key's value resolves to every primary row pointing at it — the
// group/list lookup.
func (r *Riverbed) storeGetBy(w http.ResponseWriter, body map[string]any) {
	store, serr := r.storeFor(strOf(body, "store"))
	if serr != nil {
		serr.write(w)
		return
	}
	namedKey := strOf(body, "named_key")
	unique, declared := store.namedKeys[namedKey]
	if !declared {
		refused(w, "state store %q has no named key %q", store.name, namedKey)
		return
	}
	values, present := body["values"].([]any)
	if !present || len(values) == 0 {
		refused(w, "get-by needs at least one typed value")
		return
	}
	limit := int64Of(body["limit"])
	if limit <= 0 {
		limit = 64
	}
	var matches []string
	for _, key := range r.sortedKeys(store) {
		if namedValuesEqual(store.rows[key].named[namedKey], values) {
			matches = append(matches, key)
		}
	}
	if unique && len(matches) > 1 {
		// Unique keys arbitrate first-committed-wins by journal order:
		// the earliest applied row claims the posting.
		best := matches[0]
		for _, key := range matches[1:] {
			if store.rows[key].appliedSeq < store.rows[best].appliedSeq {
				best = key
			}
		}
		matches = []string{best}
	}
	if int64(len(matches)) > limit {
		matches = matches[:limit]
	}
	rows := []map[string]any{}
	for _, key := range matches {
		row := store.rows[key]
		rows = append(rows, map[string]any{
			"key_base64":     key,
			"payload_base64": row.payloadB64,
			"applied_cursor": appliedCursor(row.appliedSeq),
		})
	}
	reply := r.freshnessBody(store)
	reply["store_id"] = store.id
	reply["rows"] = rows
	writeJSON(w, http.StatusOK, reply)
}

// namedValuesEqual reports whether a stored named value list equals
// the request's typed values — canonical JSON keeps the comparison
// honest across i64/text/bytes/bool/null kinds.
func namedValuesEqual(stored, requested any) bool {
	if stored == nil {
		return false
	}
	return bytes.Equal(canonical(stored), canonical(requested))
}

// storeIterate handles POST /v1/state-stores/iterate: one bounded page
// in primary-key order, continuing after after_key_base64.
func (r *Riverbed) storeIterate(w http.ResponseWriter, body map[string]any) {
	store, serr := r.storeFor(strOf(body, "store"))
	if serr != nil {
		serr.write(w)
		return
	}
	var after []byte
	if encoded := strOf(body, "after_key_base64"); encoded != "" {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			refused(w, "bad continuation key: %v", err)
			return
		}
		after = decoded
	}
	limit := int64Of(body["limit"])
	if limit <= 0 {
		limit = 256
	}
	rows := []map[string]any{}
	var nextKey any
	for _, key := range r.sortedKeys(store) {
		if len(after) > 0 {
			decoded, _ := base64.StdEncoding.DecodeString(key)
			if bytes.Compare(decoded, after) <= 0 {
				continue
			}
		}
		if int64(len(rows)) >= limit {
			// More rows remain: the last emitted key continues the scan.
			nextKey = rows[len(rows)-1]["key_base64"]
			break
		}
		row := store.rows[key]
		rows = append(rows, map[string]any{
			"key_base64":     key,
			"payload_base64": row.payloadB64,
			"applied_seq":    row.appliedSeq,
			"applied_cursor": appliedCursor(row.appliedSeq),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"store_id":        store.id,
		"rows":            rows,
		"next_key_base64": nextKey,
		"pinned_seq":      store.seq,
		"served_by":       storeServedBy,
	})
}

// storeClear handles POST /v1/state-stores/clear: one journaled
// mutation that empties every row, idempotent under mutation_id.
func (r *Riverbed) storeClear(w http.ResponseWriter, body map[string]any) {
	store, serr := r.storeFor(strOf(body, "store"))
	if serr != nil {
		serr.write(w)
		return
	}
	if !store.manual {
		refused(w, "state store %q is feed-sourced; clear applies to manual stores",
			store.name)
		return
	}
	mutationID := strOf(body, "mutation_id")
	sequence, deduplicated, merr := mutationSeq(
		store, mutationID, map[string]any{"kind": "clear"}, func() {
			store.rows = map[string]*fakeRow{}
		})
	if merr != nil {
		merr.write(w)
		return
	}
	writeJSON(w, http.StatusOK,
		mutationResultBody(store, mutationID, sequence, deduplicated))
}

// sortedKeys returns the store's primary keys in byte order — the same
// order iterate pages and replica transfer serve.
func (r *Riverbed) sortedKeys(store *fakeStore) []string {
	keys := make([]string, 0, len(store.rows))
	for key := range store.rows {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, _ := base64.StdEncoding.DecodeString(keys[i])
		right, _ := base64.StdEncoding.DecodeString(keys[j])
		return bytes.Compare(left, right) < 0
	})
	return keys
}
