package whitewater

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
)

// StateStore is a thin facade over one node-side StateStore — the
// durable RF3 + Fjall store on the Nodes is the only store; this object
// holds no rows. Declare it with named keys, then Put/Get/Delete/
// Iterate:
//
//	spec, _ := whitewater.NewStateStore("accounts.employees")
//	spec.AddKey("passport", true).AddKey("department", false)
//	store, err := client.StateStores.Define(ctx, spec, "")
//
//	_, _ = store.Put(ctx, []byte("user-1"), []byte("profile-1"),
//	    map[string]any{"passport": "P111", "department": "eng"}, "")
//	row, _ := store.Get(ctx, []byte("user-1"), false, 0)
//	rows, _ := store.GetBy(ctx, "department", "eng", 0, false, 0)
//
// AddKey records declaration intent used by Define; it does not mutate
// an already-declared store (named keys are declared at creation).
// Named values are typed scalars — string, int/int64, bool, []byte, or
// nil; a []any value carries one typed value per posting component.
type StateStore struct {
	transport *transport
	name      string
	namedKeys []NamedKey
}

// NewStateStore returns an unbound declaration spec for
// StateStoresClient.Define.
func NewStateStore(name string) (*StateStore, error) {
	return newStateStore(nil, name)
}

// StateStore returns a thin facade over the node-side StateStore name —
// rows live on the Nodes; this client holds no state.
func (c *Client) StateStore(name string) (*StateStore, error) {
	return newStateStore(c.transport, name)
}

func newStateStore(t *transport, name string) (*StateStore, error) {
	name, err := dottedName(name)
	if err != nil {
		return nil, err
	}
	return &StateStore{transport: t, name: name}, nil
}

// AddKey declares a named secondary key for Define (at most
// MaxNamedKeys) and returns the spec for chaining. Non-unique keys map
// one value to every matching primary row; unique enforces one row per
// value.
// AddKey declares a named secondary key for Define (at most
// MaxNamedKeys). Non-unique keys map one value to every primary row
// carrying it; unique enforces one row per value. An optional Extractor
// declares how replay projects each source-Feed record to this key's
// values — required on feed-sourced stores, refused on manual ones.
func (s *StateStore) AddKey(namedKey string, unique bool, extractors ...Extractor) *StateStore {
	if strings.TrimSpace(namedKey) == "" {
		panic("named key is required")
	}
	if len(extractors) > 1 {
		panic("a named key declares at most one extractor")
	}
	for _, key := range s.namedKeys {
		if key.Name == namedKey {
			panic("duplicate named key: " + namedKey)
		}
	}
	if len(s.namedKeys) >= MaxNamedKeys {
		panic(fmt.Sprintf("a StateStore declares at most %d named keys", MaxNamedKeys))
	}
	key := NamedKey{Name: namedKey, Unique: unique}
	if len(extractors) == 1 {
		extractor := extractors[0]
		key.Extractor = &extractor
	}
	s.namedKeys = append(s.namedKeys, key)
	return s
}

// NamedKeys returns the declared named keys in declaration order.
func (s *StateStore) NamedKeys() []NamedKey {
	return append([]NamedKey(nil), s.namedKeys...)
}

// Name returns the store's name.
func (s *StateStore) Name() string { return s.name }

func (s *StateStore) bound() (*transport, error) {
	if s.transport == nil {
		return nil, fmt.Errorf(
			"unbound StateStore spec — pass it to StateStores.Define " +
				"or get a bound facade via client.StateStore(name)")
	}
	return s.transport, nil
}

// Put inserts or replaces one row: named maps a declared named key to a
// typed value (or []any for multi-component postings). An empty
// mutationID generates one; supply a stable ID for idempotent retries.
func (s *StateStore) Put(
	ctx context.Context,
	key, payload []byte,
	named map[string]any,
	mutationID string,
) (MutationResult, error) {
	t, err := s.bound()
	if err != nil {
		return MutationResult{}, err
	}
	op := map[string]any{
		"kind":           "put_row",
		"key_base64":     base64.StdEncoding.EncodeToString(key),
		"payload_base64": base64.StdEncoding.EncodeToString(payload),
	}
	if len(named) > 0 {
		values, err := encodeNamedValues(named)
		if err != nil {
			return MutationResult{}, err
		}
		op["named"] = values
	}
	raw, err := t.post(ctx, "/v1/state-stores/mutate", map[string]any{
		"store":       s.name,
		"mutation_id": mutationOrNew(mutationID),
		"op":          op,
	})
	if err != nil {
		return MutationResult{}, err
	}
	return decodeMutationResult(raw)
}

// Get reads one row by primary key with freshness evidence on the
// returned row; nil when absent. strict bounded-waits for the serving
// replica to apply its committed frontier rather than answering stale.
func (s *StateStore) Get(
	ctx context.Context,
	key []byte,
	strict bool,
	waitMs int,
) (*StoreRow, error) {
	t, err := s.bound()
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"store":      s.name,
		"key_base64": base64.StdEncoding.EncodeToString(key),
		"strict":     strict,
	}
	if waitMs > 0 {
		body["wait_ms"] = waitMs
	}
	raw, err := t.post(ctx, "/v1/state-stores/lookup", body)
	if err != nil {
		return nil, err
	}
	object, err := decodeBody(raw)
	if err != nil {
		return nil, err
	}
	found, _ := object["found"].(bool)
	if !found {
		return nil, nil
	}
	payload, err := base64.StdEncoding.DecodeString(stringField(object, "payload_base64"))
	if err != nil {
		return nil, fmt.Errorf("decode payload_base64: %w", err)
	}
	freshness := decodeStoreFreshness(object)
	row := &StoreRow{
		Key:       append([]byte(nil), key...),
		Payload:   payload,
		Freshness: &freshness,
	}
	if cursor := stringField(object, "applied_cursor"); cursor != "" {
		row.AppliedCursor = &cursor
	}
	return row, nil
}

// GetBy resolves a declared named-key value to every primary row it
// points at — the group/list lookup — plus the serving replica's
// freshness evidence.
func (s *StateStore) GetBy(
	ctx context.Context,
	namedKey string,
	value any,
	limit int,
	strict bool,
	waitMs int,
) (StoreRows, error) {
	t, err := s.bound()
	if err != nil {
		return StoreRows{}, err
	}
	values, err := encodeIndexValues(value)
	if err != nil {
		return StoreRows{}, err
	}
	body := map[string]any{
		"store":     s.name,
		"named_key": namedKey,
		"values":    values,
		"strict":    strict,
	}
	if limit > 0 {
		body["limit"] = limit
	}
	if waitMs > 0 {
		body["wait_ms"] = waitMs
	}
	raw, err := t.post(ctx, "/v1/state-stores/get-by", body)
	if err != nil {
		return StoreRows{}, err
	}
	object, err := decodeBody(raw)
	if err != nil {
		return StoreRows{}, err
	}
	freshness := decodeStoreFreshness(object)
	fresh := freshness
	return StoreRows{
		Rows:      decodeStoreRows(object, &fresh),
		Freshness: freshness,
	}, nil
}

// Delete removes the row and its named postings.
func (s *StateStore) Delete(
	ctx context.Context,
	key []byte,
	mutationID string,
) (MutationResult, error) {
	t, err := s.bound()
	if err != nil {
		return MutationResult{}, err
	}
	raw, err := t.post(ctx, "/v1/state-stores/mutate", map[string]any{
		"store":       s.name,
		"mutation_id": mutationOrNew(mutationID),
		"op": map[string]any{
			"kind":       "delete_row",
			"key_base64": base64.StdEncoding.EncodeToString(key),
		},
	})
	if err != nil {
		return MutationResult{}, err
	}
	return decodeMutationResult(raw)
}

// Page returns one bounded iteration page in primary-key order — the
// same row set replica transfer serves, pinned to PinnedSeq.
func (s *StateStore) Page(
	ctx context.Context,
	afterKey []byte,
	limit int,
) (StorePage, error) {
	t, err := s.bound()
	if err != nil {
		return StorePage{}, err
	}
	body := map[string]any{"store": s.name}
	if len(afterKey) > 0 {
		body["after_key_base64"] = base64.StdEncoding.EncodeToString(afterKey)
	}
	if limit > 0 {
		body["limit"] = limit
	}
	raw, err := t.post(ctx, "/v1/state-stores/iterate", body)
	if err != nil {
		return StorePage{}, err
	}
	object, err := decodeBody(raw)
	if err != nil {
		return StorePage{}, err
	}
	page := StorePage{
		Rows:      decodeStoreRows(object, nil),
		PinnedSeq: intField(object, "pinned_seq"),
		ServedBy:  stringField(object, "served_by"),
	}
	if next := stringField(object, "next_key_base64"); next != "" {
		if decoded, err := base64.StdEncoding.DecodeString(next); err == nil {
			page.NextKey = decoded
		}
	}
	return page, nil
}

// Iterate visits every row in primary-key order across bounded pages.
func (s *StateStore) Iterate(
	ctx context.Context,
	limit int,
	fn func(StoreRow) error,
) error {
	var after []byte
	for {
		page, err := s.Page(ctx, after, limit)
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			if err := fn(row); err != nil {
				return err
			}
		}
		if page.NextKey == nil {
			return nil
		}
		after = page.NextKey
	}
}

// Clear empties the store as one journaled mutation — durable and
// idempotent under mutationID retries.
func (s *StateStore) Clear(
	ctx context.Context,
	mutationID string,
) (MutationResult, error) {
	t, err := s.bound()
	if err != nil {
		return MutationResult{}, err
	}
	raw, err := t.post(ctx, "/v1/state-stores/clear", map[string]any{
		"store":       s.name,
		"mutation_id": mutationOrNew(mutationID),
	})
	if err != nil {
		return MutationResult{}, err
	}
	return decodeMutationResult(raw)
}

// Drop tombstones the store: the name cannot be recreated and every
// data path refuses. Applied rows remain on replicas until reclaim.
func (s *StateStore) Drop(ctx context.Context) (ControlExecution, error) {
	t, err := s.bound()
	if err != nil {
		return ControlExecution{}, err
	}
	raw, err := t.post(ctx, "/v1/admin/commands", map[string]any{
		"commands": []map[string]any{{
			"command": "drop",
			"kind":    "state_store",
			"name":    s.name,
		}},
	})
	if err != nil {
		return ControlExecution{}, err
	}
	return decodeControlExecution(raw)
}

// StateStoresClient is the collection-level facade: declaration and
// drop. Row operations live on the bound StateStore returned by
// client.StateStore(name).
type StateStoresClient struct {
	transport *transport
}

// Define declares spec on the Control Plane and returns it bound to
// this client. source names a same-Domain Feed the store materializes
// from; empty means a manual store (required when the spec declares
// named keys).
func (c *StateStoresClient) Define(
	ctx context.Context,
	spec *StateStore,
	source string,
) (*StateStore, error) {
	if spec == nil {
		return nil, fmt.Errorf("define takes a StateStore spec — use NewStateStore(name)")
	}
	for _, key := range spec.namedKeys {
		if source == "" && key.Extractor != nil {
			return nil, fmt.Errorf(
				"named-key extractors apply to feed-sourced StateStores; " +
					"manual stores take named values on each Put")
		}
		if source != "" && key.Extractor == nil {
			return nil, fmt.Errorf(
				"feed-sourced named keys require an extractor " +
					"(KeyExtractor/MetadataExtractor/JsonExtractor)")
		}
	}
	var sourceBody map[string]any
	if source != "" {
		feed, err := dottedName(source)
		if err != nil {
			return nil, err
		}
		sourceBody = map[string]any{"kind": "feed", "feed": feed}
	} else {
		sourceBody = map[string]any{"kind": "manual"}
	}
	named := make([]map[string]any, 0, len(spec.namedKeys))
	for _, key := range spec.namedKeys {
		entry := map[string]any{"name": key.Name, "unique": key.Unique}
		if key.Extractor != nil {
			entry["extractor"] = key.Extractor.wire()
		}
		named = append(named, entry)
	}
	_, err := c.transport.post(ctx, "/v1/admin/commands", map[string]any{
		"commands": []map[string]any{{
			"command":    "define_state_store",
			"name":       spec.name,
			"source":     sourceBody,
			"named_keys": named,
		}},
	})
	if err != nil {
		return nil, err
	}
	spec.transport = c.transport
	return spec, nil
}

// Drop tombstones the named store through the facade's drop path.
func (c *StateStoresClient) Drop(
	ctx context.Context,
	name string,
) (ControlExecution, error) {
	store, err := newStateStore(c.transport, name)
	if err != nil {
		return ControlExecution{}, err
	}
	return store.Drop(ctx)
}

func mutationOrNew(mutationID string) string {
	if mutationID == "" {
		return newUUID()
	}
	return mutationID
}
