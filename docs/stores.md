# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```go
spec, _ := whitewater.NewStateStore("accounts.employees")
spec.AddKey("passport", true).            // unique: one row per value
      AddKey("department", false)         // non-unique: value -> many rows

store, err := client.StateStores.Define(ctx, spec, "")
//                                       source "" = manual store
```

`AddKey` records declaration intent used by `Define` — named keys are
declared at creation; at most `whitewater.MaxNamedKeys` (10). Adding a
key to an existing store has no effect server-side.

## Typed stores — codecs do the serialization

The raw facade carries `[]byte` keys/payloads; `Store[K, V]` binds a
key codec and a payload codec once so every `Put`/`Get`/`Iterate`
serializes through them — matching the Java SDK's `StateStore<K,V>`:

```go
type Profile struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

// String keys + JSON payloads — codecs inferred from the types.
profiles, _ := whitewater.StoreFor[Profile](client, "accounts.employees")
// …or explicit codecs — protobuf/msgpack/custom via FuncCodec:
//   StoreOf(client, "accounts.employees", StringCodec(), proto)

_, err = profiles.Put(ctx, "user-1", profile,
    map[string]any{"passport": "P111"}, "")
row, _ := profiles.Get(ctx, "user-1", true, 500)   // row.Value is Profile
_ = profiles.Iterate(ctx, 256, func(r whitewater.Row[string, Profile]) error {
    return export(r)
})

// Rebind under different codecs — no wire call:
raw := whitewater.RebindStore(profiles, whitewater.StringCodec(),
    whitewater.BytesCodec())
```

`client.Store(name)` binds the default `Store[string, []byte]`; the
byte-level `client.StateStore(name)` facade below remains for raw
access and declaration.

## Writing

```go
_, err = store.Put(ctx, []byte("user-1"), []byte("profile-1"),
    map[string]any{"passport": "P111", "department": "eng", "score": 42},
    "")                                    // "" auto-generates mutation_id
_, err = store.Delete(ctx, []byte("user-1"), "stable-mutation-uuid")
```

- `Put` commits one journaled `put_row` through the store's RF3 quorum.
- Named values are **typed**: `string` -> text, signed/unsigned ints ->
  i64, `bool` -> bool, `[]byte` -> bytes, `nil` -> null. A `[]any`
  value carries one typed value per posting component. Floats are
  refused — the wire carries exact i64/text/bytes.
- Named values must match the store's declared keys — the server
  refuses undeclared names. `unique` claims arbitrate
  first-committed-wins by journal order.
- `mutationID` is the idempotency identity: retry a timed-out Put with
  the same ID and it dedupes; reuse it with different contents and it
  conflicts.

## Reading

```go
row, err := store.Get(ctx, []byte("user-1"), false, 0)   // *StoreRow | nil
row.Payload                        // []byte
row.Freshness                      // AppliedSeq / CommittedSeq / Behind / ServedBy

rows, err := store.GetBy(ctx, "department", "eng", 0, false, 0)
rows.Rows                          // []StoreRow — every match
rows.Freshness.ServedBy            // which Node answered
```

`GetBy` is the group/list lookup: one non-unique named value resolves
to **every** primary row carrying it. `strict` + `waitMs` bounded-waits
for the serving replica to apply its committed frontier rather than
answering stale; non-strict reads still report `Freshness.Behind`
honestly.

## Iterating

```go
err = store.Iterate(ctx, 256, func(row StoreRow) error {
    export(row)
    return nil
})
page, err := store.Page(ctx, afterKey, 256)   // one bounded page
page.NextKey                                   // continuation
page.PinnedSeq                                 // journal position pinned to
```

`Iterate` pages `/v1/state-stores/iterate` — the same bounded, ordered
row set replica catch-up serves — so `PinnedSeq` is the iteration's
freshness evidence.

## Lifecycle

```go
_, err = store.Clear(ctx, "")                 // one journaled ClearAll
_, err = client.StateStores.Drop(ctx, name)   // or store.Drop(ctx)
```

- `Clear` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `Drop` tombstones the definition — the name cannot be recreated and
  every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.StateStores.Define(ctx, spec,
  "accounts.events")` materializes committed Feed history.

### Feed-sourced named keys

Named keys on a feed-sourced store declare a projection — how each
committed source record maps to typed lookup values. Every key carries
an `Extractor`:

```go
spec, _ := whitewater.NewStateStore("accounts.tagged")
spec.AddKey("passport", true, whitewater.MetadataExtractor("pp"))
spec.AddKey("department", false, whitewater.JsonExtractor("dept"))
store, err := client.StateStores.Define(ctx, spec, "accounts.events")
```

`whitewater.KeyExtractor()` reads the record's key,
`MetadataExtractor("pp")` a Metadata entry, `JsonExtractor("dept")` a
dotted JSON-path over the payload. Missing fields and unparseable
payloads project a `Null` posting rather than stalling replay. Manual
stores omit the extractor — `Put` carries the values per row.

### WCL and typed commands

Everything above is a convenience over the Control Plane's two doors.
WCL spells `DEFINE`, `DROP`, `DESCRIBE`, and `SHOW` for stores (data ops
stay typed):

```go
c.WCL(ctx, "DEFINE STATESTORE accounts.employees KEYS department, passport UNIQUE")
c.WCL(ctx, "DEFINE STATESTORE accounts.tagged FROM FEED accounts.events KEYS passport = METADATA 'pp' UNIQUE, department = JSON 'dept'")
c.WCL(ctx, "SHOW STATESTORES")
c.WCL(ctx, "DESCRIBE STATESTORE accounts.employees") // stage incl. "dropped"
c.WCL(ctx, "DROP STATESTORE accounts.employees")
```

`Commands` posts the typed command the facade builds for you:

```go
c.Commands(ctx, []map[string]any{{
    "command": "define_state_store", "name": "accounts.employees",
    "source": map[string]any{"kind": "manual"},
    "named_keys": []map[string]any{{"name": "passport", "unique": true}},
}})
```

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `GetBy` always returns a list.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `AddKey` #11 / duplicate / blank | `panic` | Declaration contract (10-key bound, distinct names). |
| `Define` spec + `source` + bare named keys | `error` | Feed-sourced keys need an `Extractor` — checked before the call. |
| `Define` manual spec + `Extractor` | `error` | Extractors apply to feed-sourced stores only. |
| Data op on unbound spec | `error` | `NewStateStore` builds a spec; bind via `Define` or `client.StateStore(name)`. |
| Undeclared named key / missing store | `*Error` 400 | Server-side contract — fix the declaration. |
| `mutationID` reused differently | `*Error` 409 | Retry with the original contents or a fresh ID. |
| No quorum / replica down | `*Error` 503 | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `*Error` 409 `index_behind` | Bounded wait expired; retry or drop `strict`. |
