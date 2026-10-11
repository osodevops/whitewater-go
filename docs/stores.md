# RecordStore and LoadStore — coding guide

A `RecordStore` is a named, thread-safe, in-memory projection of Feed
records. Fill it once with `Client.LoadStore(store).FromFeed` (bulk
history) and keep it warm with `Client.Subscribe` (live tail) — the
Whitewater equivalent of a local materialized view.

## Store anatomy

Two structures per store:

- **Primary map** — `primary key -> Record`. One row per key; a new row
  for an existing key replaces it.
- **Named-key posting maps** — one per declared named key, conceptually
  `employee-store.passport`: `named value -> set of primary keys`.
  Named keys are **not unique** — many rows may share one named value,
  so lookups return a slice.

```go
store := whitewater.NewRecordStore("employee-store").
    AddKey("passport").        // declare before use
    AddKey("department")       // at most whitewater.MaxNamedKeys (10)
```

`AddKey` returns the store for chaining. An eleventh key **panics**
("a RecordStore supports at most 10 named keys") — treat the budget as a
construction-time contract; an empty name also panics.

## Loading history: `client.LoadStore(store).FromFeed(...)`

```go
loaded, err := client.LoadStore(store).FromFeed(ctx,
    "hr.employees",
    whitewater.StartBeginning(),       // or StartNow(), StartCursor(c), StartTimestampNs(ns)
    func(s *whitewater.RecordStore, r whitewater.Record) {
        s.Upsert(r).
            WithKey(r.Key).            // primary key
            WithNamedKey("passport", r.Metadata["pp"]).
            WithNamedKey("department", []byte("eng"))
    })
```

Semantics, in order:

1. Pages `TemporaryFetch` from `start` until the frontier is drained —
   a **one-shot bounded read**, not a tail. `ctx` cancels mid-read.
2. For every record the projection runs `store.Upsert(r)`.
3. `WithKey([]byte)` chooses the primary key. Defaults to `r.Key`;
   pass anything — a composite `"dept:emp-1"` — and the row re-keys
   (old postings are stripped first).
4. `WithNamedKey(name, []byte)` attaches one named-key value; call it
   once per declared key you want indexed. An **undeclared** named key
   **panics** (`"undeclared named key: ..."`) — a projection bug fails
   the load immediately, never silently writes an unindexed row.
5. Returns `(count, error)` — count of records read, not rows written;
   re-keys replace, they don't duplicate.

Skip `WithNamedKey` and the row lands in the primary map only — fully
readable, just not indexable by that named key.

`Feeds.Load(ctx, feed, start, sink)` / `Feeds.LoadInto(ctx, feed,
start, store)` remain the lighter "dump history" paths — `sink` is a
`func([]Record) error`, and `store.PutAll` fits it directly.

## Keeping the store warm: `Subscribe`

```go
sub := client.Subscribe(ctx, "hr-view", "hr.employees",
    whitewater.StartNow(),                          // only new records
    func(records []whitewater.Record) error {       // per fetched page
        for _, r := range records {
            s := store.Upsert(r).WithKey(r.Key)
            s.WithNamedKey("department", r.Metadata["department"])
        }
        return nil                                  // error -> no ack, page replays
    },
    func(err error) { log.Println("subscription ended:", err) })
defer sub.Close()                                   // stops the loop + releases the session
// sub.Err() reports the terminal error after Done() closes.
```

Delivery is **at-least-once**: records land in `onRecords`, then the
loop acks `deliveredCursor`. Return a non-nil error from `onRecords` and
the page is **not** acked — it replays on the next fetch. A crash
between callback and ack also replays — make the projection idempotent
(upsert semantics already are).

## Reading the store

```go
rec, ok := store.Get([]byte("emp-1"))         // (Record, bool) by primary key
rec, ok  = store.GetString("emp-1")           // UTF-8 convenience
rows := store.GetBy("department", b)          // []Record — every match, non-unique
rows  = store.GetByString("department", "eng")
store.GetBy("undeclared", v)                  // nil — GetBy is a query, not a write;
                                              // use NamedKeys() to guard projections
store.Len()                                   // row count
store.NamedKeys()                             // declared keys, in order
store.Name()                                  // "employee-store"
store.Entries()                               // []StoreEntry — {PrimaryKey, Record}
store.ForEach(func(e whitewater.StoreEntry) { export(e.Record) })
```

## Mutating outside a load

```go
store.Put(record)                    // primary-key upsert, no named keys
store.PutAll(records)                // subscribe-ready sink shape (error for fit)
store.Upsert(record).WithKey(k)      // same builder as projections
store.Delete([]byte("emp-1"))        // removes row + all its postings
store.Clear()                        // empties everything
```

`Delete` and `Clear` strip named-key postings too — a deleted row never
lingers in a `GetBy` result.

## Copying and parallel work

```go
replica := whitewater.NewRecordStore("employee-backup")
store.Copy(replica)                  // rows + named postings; declares
                                     // missing keys on the target
store.ForEachParallel(3, func(e whitewater.StoreEntry) {
    rebuild(e)                       // runs on one of 3 goroutines
})
```

`ForEachParallel` is **real parallelism** — a `jobs` channel feeds
`workers` goroutines (capped at the row count), joined before return.
`workers < 1` panics. The callback runs concurrently on multiple
goroutines: keep it free of un-synchronized shared state (the store
itself is already locked internally).

## Error handling reference

| Where | Shape | Meaning / remedy |
|---|---|---|
| `AddKey` #11 / empty name | panic | Construction-time contract — declare ≤10 keys at build. |
| `WithNamedKey` undeclared | panic | Programmer error — declare the key before projecting it. |
| `GetBy` undeclared key | `nil` | Check `NamedKeys()`; declare keys up front. |
| `FromFeed` mid-read | `error` (usually `*Error` kind `Unavailable`) | Transport refused a page; retry the whole call — re-upserts are idempotent. |
| `Subscribe` transient | retried internally | `KindUnavailable`/`KindAmbiguous` backoff inside the loop. |
| `Subscribe` terminal | `onError` + `sub.Err()` | `IsStaleSession(err)` (another client opened your Reader — pick a unique name or take over deliberately), `IsRefused`, `IsConflict`. The loop is dead; inspect, fix, re-subscribe. |

Classify errors with `whitewater.IsStaleSession(err)`,
`IsUnavailable(err)`, `IsAmbiguous(err)`, `IsLeaseConflict(err)`, and
`err.(*whitewater.Error)` for `Status`/`Code`.

## What a RecordStore is **not**

- **Not durable** — it dies with the process. Rebuild is cheap:
  `FromFeed` from `StartBeginning()` (or a snapshot Cursor).
- **Not a server StateStore** — the server-side replicated store model
  is a separate, later seam. This is the client-side projection.
- **Not unique on named keys** — `GetBy` always returns a slice, even
  when it happens to hold one row.
