# whitewater-go

Go SDK for [Whitewater](https://github.com/osodevops/whitewater) — the
developer-friendly, platform-scalable streaming system written in Rust.

Zero dependencies: `net/http` transport, `context.Context` cancellation,
`[]byte` keys/payloads/metadata, `int64` nanoseconds, opaque `Cursor`
strings.

## Concepts

Whitewater exposes `domain -> feed -> key -> cursor -> subscription`.
Partitions, ranges, replicas, and physical positions are internal
implementation details — cursors are opaque tokens, never parsed.

- **Feeds** hold an immutable history of records keyed by `key`.
- **Writers** append through epoch-fenced sessions; the same
  `RequestID` deduplicates retries.
- **Readers** own independent durable cursors (at-least-once delivery;
  ack to commit).
- **Subscriptions** share durable acknowledged progress across
  cooperating members with lease fencing.

## Usage

```go
package main

import (
    "context"
    "time"

    whitewater "github.com/osodevops/whitewater-go"
)

func main() {
    ctx := context.Background()
    client := whitewater.NewClient(
        "api-key",
        []string{"https://node1:7070", "https://node2:7070"},
        30*time.Second,
    )

    // Provision a Domain, Feed, Writer, and session — either typed
    // (each method builds its WCL statement for you):
    _, err := client.CreateDomain(ctx, "shop")
    _, err = client.CreateFeed(ctx, "shop.orders")
    _, err = client.CreateWriter(ctx, "orders-writer", "shop.orders")
    setup, err := client.OpenWriterSession(ctx, "orders-writer")
    // setup.Results[0].Data holds {"session_epoch": N}
    //
    // …or raw WCL when a script is clearer:
    //   client.WCL(ctx, "CREATE DOMAIN shop; CREATE FEED shop.orders; ...")

    // Idempotent append — retrying with the same RequestID is safe.
    writer := client.Writer("orders-writer", sessionEpoch)
    eventTime := time.Now().UnixNano()
    appended, err := writer.Append(ctx, whitewater.AppendInput{
        RequestID:   "a-stable-request-uuid",
        Key:         []byte("order-42"),
        Payload:     []byte("created"),
        EventTimeNs: &eventTime,
        Metadata:    map[string][]byte{"source": []byte("api")},
    })

    // Independent durable reader.
    reader := client.Reader("orders-reader")
    session, err := reader.Open(ctx, 64)
    page, err := reader.Fetch(ctx, session, 128)
    for _, record := range page.Records {
        process(record)
    }
    if page.DeliveredCursor != nil {
        _, err = reader.Ack(ctx, session, *page.DeliveredCursor)
    }
    _ = appended
}
```

### Managed consume: `load` + `Subscribe`

```go
store := whitewater.NewRecordStore("employee-store").
    AddKey("passport").AddKey("department")            // up to 10 named keys

// One-shot bulk read of committed history, projected per row.
_, err = client.LoadStore(store).FromFeed(ctx, "hr.employees",
    whitewater.StartBeginning(),
    func(s *whitewater.RecordStore, r whitewater.Record) {
        s.Upsert(r).WithKey(r.Key).WithNamedKey("passport", r.Metadata["pp"])
    })

rec, _ := store.GetString("emp-1")                      // by primary key
rows := store.GetByString("passport", "P123")           // by named key — non-unique
store.ForEachParallel(3, func(e whitewater.StoreEntry) { export(e.Record) })

// ...then keep it warm: provisions the Reader, runs
// Open -> [Fetch -> onRecords -> Ack] -> Close on a background goroutine.
sub := client.Subscribe(ctx, "hr-view", "hr.employees", whitewater.StartNow(),
    store.PutAll, func(err error) { log.Println("subscription ended:", err) })
defer sub.Close()
```

`client.Feeds.LoadInto`/`Load` remain the lighter one-shot reads when
you don't need named-key projections.

### Error handling

`call` failures return `*whitewater.Error`; class helpers select policy:

```go
switch {
case whitewater.IsStaleSession(err):   // re-open the session
case whitewater.IsAmbiguous(err):      // may have committed — dedup retry
case whitewater.IsUnavailable(err):    // safe retry after backoff
case whitewater.IsLeaseConflict(err):  // lease lost or contended
case whitewater.IsConflict(err):       // fencing; inspect err.Code
default:                               // refused; fix the request
}
```

## Contract parity

This SDK implements the versioned wire contract in
[`whitewater/sdk/spec`](https://github.com/osodevops/whitewater/tree/main/sdk/spec):
decimal-string signed 64-bit nanoseconds, base64 binary fields, opaque
cursors, UUID request ids, and the shared structured error codes.

## Conformance

The shared fixture corpus runs against a live three-Node Riverbed:

```sh
export WHITEWATER_CONFORMANCE_URLS="http://localhost:7371,http://localhost:7372,http://localhost:7373"
export WHITEWATER_FIXTURE_DIR=/path/to/whitewater/sdk/spec/conformance
go test -run Conformance -v .
```

## Development

```sh
go build ./...
go test ./...   # unit tests; conformance skips without env vars
```
