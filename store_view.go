package whitewater

import (
	"context"
	"fmt"
)

// Row is one StateStore row with key and payload decoded through the
// store's bound codecs — the StoreRow fields carry through; only the
// values are typed.
type Row[K comparable, V any] struct {
	Key           K
	Value         V
	AppliedCursor *string
	Freshness     *StoreFreshness
}

// RowPage is one bounded typed iteration page in primary-key order.
// NextKey continues the scan; PinnedSeq is the replica's applied
// journal position.
type RowPage[K comparable, V any] struct {
	Rows      []Row[K, V]
	NextKey   *K
	PinnedSeq int64
	ServedBy  string
}

// RowsResult is a named-key lookup result: every primary row the value
// points at plus the serving replica's freshness evidence.
type RowsResult[K comparable, V any] struct {
	Rows      []Row[K, V]
	Freshness StoreFreshness
}

// Store is a codec-bound facade over one node-side StateStore — the
// same shape Java calls StateStore<K,V>. Every Get/Put/Iterate
// serializes through the bound codecs, so application code stays in
// its own types:
//
//	store, _ := whitewater.StoreOf(client, "accounts.employees",
//	    whitewater.StringCodec(), whitewater.JSONCodec[Profile]())
//	store.Put(ctx, "user-1", profile, map[string]any{"passport": "P111"}, "")
//	row, _ := store.Get(ctx, "user-1", false, 0)   // row.Value is Profile
//
// Declare named keys on the raw spec first — Store binds codecs for
// data operations, not declaration:
//
//	spec, _ := whitewater.NewStateStore("accounts.employees")
//	spec.AddKey("passport", true)
//	raw, _ := client.StateStores.Define(ctx, spec, "")
//	store := whitewater.StoreOfBound[string, Profile](raw, kc, vc)
type Store[K comparable, V any] struct {
	store      *StateStore
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// StoreOf binds a declared StateStore under explicit codecs; nil codec
// arguments default to CodecFor (JSON for object types).
func StoreOf[K comparable, V any](
	client *Client,
	name string,
	keyCodec Codec[K],
	valueCodec Codec[V],
) (*Store[K, V], error) {
	bound, err := client.StateStore(name)
	if err != nil {
		return nil, err
	}
	return StoreOfBound[K, V](bound, keyCodec, valueCodec), nil
}

// StoreFor binds a declared StateStore with string keys and
// CodecFor[V] values — the common case: StoreFor[Profile](client, "s").
func StoreFor[V any](client *Client, name string) (*Store[string, V], error) {
	return StoreOf(client, name, StringCodec(), CodecFor[V]())
}

// Store binds the default facade: string keys + raw byte payloads.
func (c *Client) Store(name string) (*Store[string, []byte], error) {
	return StoreOf(c, name, StringCodec(), BytesCodec())
}

// StoreOfBound rebinds an already-bound StateStore under typed codecs
// — no wire call; nil arguments default to CodecFor.
func StoreOfBound[K comparable, V any](
	store *StateStore, keyCodec Codec[K], valueCodec Codec[V],
) *Store[K, V] {
	if keyCodec == nil {
		keyCodec = CodecFor[K]()
	}
	if valueCodec == nil {
		valueCodec = CodecFor[V]()
	}
	return &Store[K, V]{store: store, keyCodec: keyCodec, valueCodec: valueCodec}
}

// RebindStore rebinds the same store under different codecs — no wire
// call: RebindStore(profiles, StringCodec(), BytesCodec()) reads raw.
func RebindStore[K comparable, V any, K2 comparable, V2 any](
	s *Store[K, V], keyCodec Codec[K2], valueCodec Codec[V2],
) *Store[K2, V2] {
	return StoreOfBound[K2, V2](s.store, keyCodec, valueCodec)
}

// Name returns the store's name.
func (s *Store[K, V]) Name() string { return s.store.Name() }

// Raw exposes the underlying byte-level StateStore.
func (s *Store[K, V]) Raw() *StateStore { return s.store }

// Put inserts or replaces one row, encoding key and payload through
// the bound codecs. named maps declared named keys to typed scalar
// values; a stable mutationID makes retries idempotent.
func (s *Store[K, V]) Put(
	ctx context.Context,
	key K,
	value V,
	named map[string]any,
	mutationID string,
) (MutationResult, error) {
	keyBytes, err := s.keyCodec.Encode(key)
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode key: %w", err)
	}
	payload, err := s.valueCodec.Encode(value)
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode payload: %w", err)
	}
	return s.store.Put(ctx, keyBytes, payload, named, mutationID)
}

// Get reads one row by primary key with freshness evidence; nil when
// absent. strict bounded-waits for the replica's committed frontier.
func (s *Store[K, V]) Get(
	ctx context.Context,
	key K,
	strict bool,
	waitMs int,
) (*Row[K, V], error) {
	keyBytes, err := s.keyCodec.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}
	row, err := s.store.Get(ctx, keyBytes, strict, waitMs)
	if err != nil || row == nil {
		return nil, err
	}
	decoded, err := s.decodeRow(*row)
	if err != nil {
		return nil, err
	}
	return &decoded, nil
}

// GetBy resolves a declared named-key value to every typed primary row
// it points at plus freshness evidence.
func (s *Store[K, V]) GetBy(
	ctx context.Context,
	namedKey string,
	value any,
	limit int,
	strict bool,
	waitMs int,
) (RowsResult[K, V], error) {
	result := RowsResult[K, V]{}
	rows, err := s.store.GetBy(ctx, namedKey, value, limit, strict, waitMs)
	if err != nil {
		return result, err
	}
	decoded, err := s.decodeRows(rows.Rows)
	if err != nil {
		return result, err
	}
	result.Rows = decoded
	result.Freshness = rows.Freshness
	return result, nil
}

// Delete removes the row and its named postings.
func (s *Store[K, V]) Delete(
	ctx context.Context,
	key K,
	mutationID string,
) (MutationResult, error) {
	keyBytes, err := s.keyCodec.Encode(key)
	if err != nil {
		return MutationResult{}, fmt.Errorf("encode key: %w", err)
	}
	return s.store.Delete(ctx, keyBytes, mutationID)
}

// Page returns one bounded typed iteration page in primary-key order;
// pass the previous page's NextKey (or nil to start) to continue.
func (s *Store[K, V]) Page(
	ctx context.Context,
	afterKey *K,
	limit int,
) (RowPage[K, V], error) {
	var after []byte
	if afterKey != nil {
		encoded, err := s.keyCodec.Encode(*afterKey)
		if err != nil {
			return RowPage[K, V]{}, fmt.Errorf("encode key: %w", err)
		}
		after = encoded
	}
	page, err := s.store.Page(ctx, after, limit)
	if err != nil {
		return RowPage[K, V]{}, err
	}
	rows, err := s.decodeRows(page.Rows)
	if err != nil {
		return RowPage[K, V]{}, err
	}
	result := RowPage[K, V]{Rows: rows, PinnedSeq: page.PinnedSeq, ServedBy: page.ServedBy}
	if page.NextKey != nil {
		next, err := s.keyCodec.Decode(page.NextKey)
		if err != nil {
			return RowPage[K, V]{}, fmt.Errorf("decode next_key: %w", err)
		}
		result.NextKey = &next
	}
	return result, nil
}

// Iterate visits every row in primary-key order across bounded pages.
func (s *Store[K, V]) Iterate(
	ctx context.Context,
	limit int,
	fn func(Row[K, V]) error,
) error {
	var after *K
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

// Clear empties the store as one journaled mutation.
func (s *Store[K, V]) Clear(ctx context.Context, mutationID string) (MutationResult, error) {
	return s.store.Clear(ctx, mutationID)
}

// Drop tombstones the store.
func (s *Store[K, V]) Drop(ctx context.Context) (ControlExecution, error) {
	return s.store.Drop(ctx)
}

func (s *Store[K, V]) decodeRow(raw StoreRow) (Row[K, V], error) {
	key, err := s.keyCodec.Decode(raw.Key)
	if err != nil {
		return Row[K, V]{}, fmt.Errorf("decode key: %w", err)
	}
	value, err := s.valueCodec.Decode(raw.Payload)
	if err != nil {
		return Row[K, V]{}, fmt.Errorf("decode payload: %w", err)
	}
	return Row[K, V]{
		Key: key, Value: value,
		AppliedCursor: raw.AppliedCursor, Freshness: raw.Freshness,
	}, nil
}

func (s *Store[K, V]) decodeRows(raw []StoreRow) ([]Row[K, V], error) {
	rows := make([]Row[K, V], 0, len(raw))
	for _, r := range raw {
		decoded, err := s.decodeRow(r)
		if err != nil {
			return nil, err
		}
		rows = append(rows, decoded)
	}
	return rows, nil
}
