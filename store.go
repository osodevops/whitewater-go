package whitewater

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ReadOption tunes a store read, listing, or page — Strict bounds the
// freshness wait, Limit caps returned rows, and After continues a Page
// or Iterate past a key.
type ReadOption func(*readConfig)

type readConfig struct {
	strict bool
	waitMs int
	limit  int
	after  any
}

// Strict makes a read bounded-wait up to wait for the serving replica
// to apply its committed frontier rather than answering stale.
func Strict(wait time.Duration) ReadOption {
	return func(c *readConfig) {
		c.strict = true
		c.waitMs = int(wait.Milliseconds())
	}
}

// Limit caps the rows a GetBy or Page returns, and sets the page size
// Iterate steps by.
func Limit(n int) ReadOption {
	return func(c *readConfig) { c.limit = n }
}

// After continues a Page or starts an Iterate after the given key —
// pass a previous page's NextKey ([]byte on StateStore, K on
// Store[K, V]) to continue.
func After(key any) ReadOption {
	return func(c *readConfig) { c.after = key }
}

func resolveReads(opts []ReadOption) readConfig {
	var cfg readConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func afterBytes(after any) ([]byte, error) {
	switch v := after.(type) {
	case nil:
		return nil, nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("After key must be []byte, got %T", after)
	}
}

// StateStore is a thin facade over one node-side StateStore — the
// durable RF3 + Fjall store on the Nodes is the only store; this object
// holds no rows. Declare it with named keys, then Put/Get/Delete/
// Iterate:
//
//	spec, _ := whitewater.NewStateStore("employees")
//	spec.AddUniqueKey("passport").AddNonUniqueKey("department")
//	store, err := domain.DefineStateStore(ctx, spec)
//
//	_, _ = store.Put(ctx, []byte("user-1"), []byte("profile-1"),
//	    map[string]any{"passport": "P111", "department": "eng"})
//	row, _ := store.Get(ctx, []byte("user-1"))
//	rows, _ := store.GetBy(ctx, "department", "eng")
//
// AddUniqueKey/AddNonUniqueKey record declaration intent used by
// Define; they do not mutate an already-declared store (named keys are
// declared at creation).
// Named values are typed scalars — string, int/int64, bool, []byte, or
// nil; a []any value carries one typed value per posting component.
// StoreKeySet represents strongly-typed or inline secondary-key postings
// for a StateStore or Store PutKeySet call. Embed BaseStoreKeySet in a
// struct to support fluent value chaining:
//
//	type PassportKeySet struct{ whitewater.BaseStoreKeySet }
//	func (k PassportKeySet) WithPassport(v string) PassportKeySet   { k.Put("passport", v); return k }
//	func (k PassportKeySet) WithDepartment(v string) PassportKeySet { k.Put("department", v); return k }
type StoreKeySet interface {
	ToMap() map[string]any
}

// BaseStoreKeySet is the embeddable base for typed StoreKeySet structs.
type BaseStoreKeySet struct {
	keys map[string]any
}

// Put records a secondary-key value and returns the receiver for chaining.
func (b *BaseStoreKeySet) Put(name string, value any) *BaseStoreKeySet {
	if strings.TrimSpace(name) == "" {
		panic("secondary key name must not be blank")
	}
	if b.keys == nil {
		b.keys = make(map[string]any)
	}
	b.keys[name] = value
	return b
}

// Get reads back a previously set secondary-key value.
func (b BaseStoreKeySet) Get(name string) any {
	if b.keys == nil {
		return nil
	}
	return b.keys[name]
}

// IsEmpty reports whether no secondary keys have been populated.
func (b BaseStoreKeySet) IsEmpty() bool {
	return len(b.keys) == 0
}

// ToMap returns a shallow copy of the secondary-key postings.
func (b BaseStoreKeySet) ToMap() map[string]any {
	if len(b.keys) == 0 {
		return nil
	}
	out := make(map[string]any, len(b.keys))
	for k, v := range b.keys {
		out[k] = v
	}
	return out
}

// NewStoreKeySet builds an ad-hoc StoreKeySet from alternating key/value pairs.
func NewStoreKeySet(pairs ...any) *BaseStoreKeySet {
	var ks BaseStoreKeySet
	for i := 0; i+1 < len(pairs); i += 2 {
		name, ok := pairs[i].(string)
		if !ok {
			panic("NewStoreKeySet expects string key names")
		}
		ks.Put(name, pairs[i+1])
	}
	return &ks
}

// BuildStoreKeySet builds an ad-hoc StoreKeySet via a callback.
func BuildStoreKeySet(fn func(*BaseStoreKeySet)) *BaseStoreKeySet {
	var ks BaseStoreKeySet
	if fn != nil {
		fn(&ks)
	}
	return &ks
}

var declaredDefaultKeys sync.Map

func registerDefaultKey(storeName, defaultKey string) {
	if storeName != "" && strings.TrimSpace(defaultKey) != "" {
		if norm, err := dottedName(storeName); err == nil {
			declaredDefaultKeys.Store(norm, defaultKey)
		}
	}
}

type StateStore struct {
	transport  *transport
	name       string
	namedKeys  []NamedKey
	defaultKey string
}

// NewStateStore returns an unbound declaration spec for
// StateStoresClient.Define.
func NewStateStore(name string) (*StateStore, error) {
	return newStateStore(nil, name)
}

// StateStore returns a thin facade over the node-side StateStore name —
// rows live on the Nodes; this client holds no state.
// stateStore binds a StateStore facade — internal; the public route is
// Domain.StateStore / Domain.DefineStateStore.
func (c *Client) stateStore(name string) (*StateStore, error) {
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
// one value to every primary row carrying it; unique enforces one row
// per value. An optional Extractor declares how replay projects each
// source-Feed record to this key's values — required on feed-sourced
// stores, refused on manual ones. Prefer AddUniqueKey/AddNonUniqueKey —
// they name the flag at the call site.
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

// AddUniqueKey declares a unique named key — one value resolves to at
// most one primary row (AddKey(namedKey, true, extractors...)).
func (s *StateStore) AddUniqueKey(namedKey string, extractors ...Extractor) *StateStore {
	return s.AddKey(namedKey, true, extractors...)
}

// AddNonUniqueKey declares a non-unique named key — one value lists
// every matching primary row (AddKey(namedKey, false, extractors...)).
func (s *StateStore) AddNonUniqueKey(namedKey string, extractors ...Extractor) *StateStore {
	return s.AddKey(namedKey, false, extractors...)
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
			"unbound StateStore spec — pass it to Domain.DefineStateStore " +
				"or get a bound facade via Domain.StateStore(name)")
	}
	return s.transport, nil
}

// Put inserts or replaces one row: named maps a declared named key to a
// typed value (or []any for multi-component postings). An empty
// mutationID generates one; supply a stable ID for idempotent retries.
// WithDefaultKey binds the secondary-key name used by PutWithKey.
func (s *StateStore) WithDefaultKey(defaultKeyName string) *StateStore {
	if strings.TrimSpace(defaultKeyName) == "" {
		panic("defaultKeyName must not be blank")
	}
	s.defaultKey = defaultKeyName
	return s
}

func (s *StateStore) resolveDefaultKeyName() string {
	if s.defaultKey != "" {
		return s.defaultKey
	}
	if len(s.namedKeys) > 0 {
		return s.namedKeys[0].Name
	}
	if v, ok := declaredDefaultKeys.Load(s.name); ok {
		if str, ok := v.(string); ok && str != "" {
			return str
		}
	}
	return "default"
}

// PutKeySet inserts or replaces one row using a typed StoreKeySet.
// Omit mutationID to generate one; supply a stable ID for idempotent retries.
func (s *StateStore) PutKeySet(
	ctx context.Context,
	key, payload []byte,
	keys StoreKeySet,
	mutationID ...string,
) (MutationResult, error) {
	var named map[string]any
	if keys != nil {
		named = keys.ToMap()
	}
	return s.Put(ctx, key, payload, named, mutationID...)
}

// PutWithKey inserts or replaces one row with a single default secondary-key string.
func (s *StateStore) PutWithKey(
	ctx context.Context,
	key, payload []byte,
	singleSecondaryKey string,
	mutationID ...string,
) (MutationResult, error) {
	var named map[string]any
	if singleSecondaryKey != "" {
		named = map[string]any{s.resolveDefaultKeyName(): singleSecondaryKey}
	}
	return s.Put(ctx, key, payload, named, mutationID...)
}

func (s *StateStore) Put(
	ctx context.Context,
	key, payload []byte,
	named map[string]any,
	mutationID ...string,
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
// returned row; nil when absent. Pass Strict(d) to bounded-wait for the
// serving replica to apply its committed frontier rather than answering stale.
func (s *StateStore) Get(
	ctx context.Context,
	key []byte,
	opts ...ReadOption,
) (*StoreRow, error) {
	t, err := s.bound()
	if err != nil {
		return nil, err
	}
	cfg := resolveReads(opts)
	body := map[string]any{
		"store":      s.name,
		"key_base64": base64.StdEncoding.EncodeToString(key),
		"strict":     cfg.strict,
	}
	if cfg.waitMs > 0 {
		body["wait_ms"] = cfg.waitMs
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
// freshness evidence. Limit(n) caps the page; Strict(d) bounded-waits
// for freshness.
func (s *StateStore) GetBy(
	ctx context.Context,
	namedKey string,
	value any,
	opts ...ReadOption,
) (StoreRows, error) {
	t, err := s.bound()
	if err != nil {
		return StoreRows{}, err
	}
	cfg := resolveReads(opts)
	values, err := encodeIndexValues(value)
	if err != nil {
		return StoreRows{}, err
	}
	body := map[string]any{
		"store":     s.name,
		"named_key": namedKey,
		"values":    values,
		"strict":    cfg.strict,
	}
	if cfg.limit > 0 {
		body["limit"] = cfg.limit
	}
	if cfg.waitMs > 0 {
		body["wait_ms"] = cfg.waitMs
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
	mutationID ...string,
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
// same row set replica transfer serves, pinned to PinnedSeq. Pass
// After(page.NextKey) to continue and Limit(n) to bound the page.
func (s *StateStore) Page(
	ctx context.Context,
	opts ...ReadOption,
) (StorePage, error) {
	t, err := s.bound()
	if err != nil {
		return StorePage{}, err
	}
	cfg := resolveReads(opts)
	afterKey, err := afterBytes(cfg.after)
	if err != nil {
		return StorePage{}, err
	}
	body := map[string]any{"store": s.name}
	if len(afterKey) > 0 {
		body["after_key_base64"] = base64.StdEncoding.EncodeToString(afterKey)
	}
	if cfg.limit > 0 {
		body["limit"] = cfg.limit
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

// Iterate visits every row in primary-key order across bounded pages —
// Limit(n) sets the page size and After(key) a start position.
func (s *StateStore) Iterate(
	ctx context.Context,
	fn func(StoreRow) error,
	opts ...ReadOption,
) error {
	cfg := resolveReads(opts)
	after, err := afterBytes(cfg.after)
	if err != nil {
		return err
	}
	for {
		page, err := s.Page(ctx, After(after), Limit(cfg.limit))
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
	mutationID ...string,
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
// Domain.StateStore(name).
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
	if len(spec.namedKeys) > 0 {
		registerDefaultKey(spec.name, spec.namedKeys[0].Name)
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

func mutationOrNew(mutationIDs []string) string {
	if len(mutationIDs) > 0 && mutationIDs[0] != "" {
		return mutationIDs[0]
	}
	return newUUID()
}
