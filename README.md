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

### Typed feeds — the idiomatic path

`Feed[K, V]` binds a feed to a key codec and a payload codec once;
every write encodes and every read decodes through them — application
code never touches bytes:

```go
type Order struct {
    ID    string `json:"id"`
    Total int    `json:"total"`
}

client := whitewater.NewClient("api-key", endpoints, 30*time.Second)

// String keys + JSON payloads — FeedFor picks both codecs from the
// types (FeedOf takes explicit codecs for protobuf/custom formats).
orders, _ := whitewater.FeedFor[Order](client, "shop.orders")

// Writer auto-declares the named Writer and opens its session —
// Append(key, value) encodes for you.
writer, _ := orders.Writer(ctx, "ingest")
appended, _ := writer.Append(ctx, "order-42", Order{ID: "order-42", Total: 99})

// Idempotent events: pin RequestID for caller-driven retries.
_, _ = writer.AppendEvent(ctx, whitewater.FeedEvent[string, Order]{
    RequestID:   "a-stable-request-uuid",
    Key:         whitewater.Ptr("order-42"),
    Value:       Order{ID: "order-42", Total: 99},
    Metadata:    whitewater.TextMetadata(map[string]string{"source": "api"}),
})

// Named Reader — Fetch returns decoded messages; Ack commits progress.
reader, _ := orders.Reader(ctx, "audit", 64)
defer reader.Close(ctx)
page, _ := reader.Fetch(ctx, 0)
for _, m := range page.Records {
    fmt.Println(*m.Key, m.Value.Total) // Order, not []byte
}
reader.Ack(ctx)

// History reads decode the same way.
messages, _ := orders.Records(ctx, 100, nil)
_ = orders.Load(ctx, whitewater.StartBeginning(),
    func(batch []whitewater.FeedMessage[string, Order]) error {
        return replay(batch)
    })
```

### Codecs

```go
whitewater.StringCodec()          // UTF-8 text
whitewater.BytesCodec()           // raw []byte identity
whitewater.JSONCodec[T]()         // encoding/json for any mapped type
whitewater.CodecFor[T]()          // infers: string→UTF-8, []byte→raw, else JSON
whitewater.FuncCodec(enc, dec)    // protobuf, msgpack, application framing
```

```go
// A protobuf payload, for instance, is one FuncCodec away:
proto := whitewater.FuncCodec(
    func(u *User) ([]byte, error) { return proto.Marshal(u) },
    func(b []byte) (*User, error) { var u User; return &u, proto.Unmarshal(b, &u) })
users, _ := whitewater.FeedOf(client, "app.users", whitewater.StringCodec(), proto)

// Rebind any bound facade under different codecs — no wire call:
raw := whitewater.RebindFeed(users, whitewater.StringCodec(), whitewater.BytesCodec())
```

`client.Feed(name)` returns the default `Feed[string, []byte]` — string
keys and untouched payload bytes, the escape hatch to the wire.

### Wire-level API

The raw `[]byte` API stays available for full control:

```go
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
```

### Node-side StateStore

Declare named keys on the spec, then bind typed codecs once — every
Put/Get/Iterate serializes through them:

```go
spec, _ := whitewater.NewStateStore("accounts.employees")
spec.AddKey("passport", true).AddKey("department", false)  // up to 10 named keys
_, err := client.StateStores.Define(ctx, spec, "")

profiles, _ := whitewater.StoreFor[Profile](client, "accounts.employees")
_, err = profiles.Put(ctx, "user-1", profile,
    map[string]any{"passport": "P111", "department": "eng"}, "")

row, _ := profiles.Get(ctx, "user-1", true, 500)        // row.Value is Profile
rows, _ := profiles.GetBy(ctx, "department", "eng", 0, false, 0)
_ = profiles.Iterate(ctx, 256,
    func(r whitewater.Row[string, Profile]) error { export(r); return nil })

// Rebind under different codecs without a wire call:
raw := whitewater.RebindStore(profiles, whitewater.StringCodec(), whitewater.BytesCodec())
```

`client.Store(name)` binds the default `Store[string, []byte]`; the
byte-level `client.StateStore(name)` facade remains for raw access.
The facade is thin — rows live on the Nodes (RF3 + Fjall) and reads
carry freshness evidence. See [docs/stores.md](docs/stores.md) for the
full guide. `client.Feeds.Load` remains the lighter one-shot history
read into any `func([]Record) error` sink.

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

### Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model
— names, never partitions or Nodes:

```go
// Pipe lifecycle: describe, pause, resume, drop.
pipe := client.Pipe("orders.relay")
info, _ := pipe.Describe(ctx)   // stage, operation, outputs, status
_ = pipe.Pause(ctx); _ = pipe.Resume(ctx); _ = pipe.Drop(ctx)
pipes, _ := client.Pipes(ctx)   // the declared catalog

// Index queries: exact, prefix, or range predicates over typed values.
page, _ := client.QueryIndex(ctx, "crm.byregion",
    whitewater.IndexRange([]any{"emea"},
        whitewater.InclusiveBound(0), whitewater.ExclusiveBound(99)),
    50,    // limit
    true)  // strict: wait for the committed target rather than stale
for _, row := range page.Rows { /* row.Key / row.Payload bytes */ }

// Demand observability: typed rows + per-domain rollups.
report, _ := client.DemandFeeds(ctx)
appended, _ := report.Rows[0].Counter("appended_records")
client.DemandReaders(ctx); client.DemandSubscriptions(ctx)
client.DemandIndexes(ctx); client.DemandPipes(ctx)
hot, _ := client.HotKeys(ctx) // dominant-key evidence per feed
```

Pipe operation payloads (`PipeInfo.Operation`) and demand counter
fields (`DemandRow.Fields`, `HotKey.Evidence`) stay opaque maps so
server-side variants evolve without an SDK bump. `QueryIndex` values
accept the same typed scalars as StateStore named values — floats are
refused because the wire carries exact `i64`/`text`/`bytes`.

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
