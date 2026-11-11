# whitewater-go

Go SDK for [Whitewater](https://github.com/osodevops/whitewater) — the
developer-friendly, platform-scalable streaming system written in Rust.

Zero dependencies: `net/http` transport, `context.Context` cancellation,
`[]byte` keys/payloads/metadata, `int64` nanoseconds, opaque `Cursor`
strings.

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

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

## Clients

```go
client := whitewater.NewClient("api-key",
    []string{"https://node1:7070", "https://node2:7070", "https://node3:7070"},
    30*time.Second)
```

Everything a service needs hangs off the client — `CreateDomain`,
`Domain`/`GetDomain` handles, WCL + typed commands, health and demand
reports — and every Feed, Writer, Reader, Subscription, Member, Pipe,
StateStore, and Index is reached **only** through a `Domain`. The typed
path below binds codecs once; the wire-level escape hatches keep raw
`[]byte` control.

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
shop, _ := client.Domain("shop")

// String keys + JSON payloads — FeedForDomain picks both codecs from the
// types (FeedOfDomain takes explicit codecs for protobuf/custom formats).
orders, _ := whitewater.FeedForDomain[Order](shop, "orders")

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
app, _ := client.Domain("app")
users, _ := whitewater.FeedOfDomain(app, "users", whitewater.StringCodec(), proto)

// Rebind any bound facade under different codecs — no wire call:
raw := whitewater.RebindFeed(users, whitewater.StringCodec(), whitewater.BytesCodec())
```

`domain.Feed(relative)` returns the default `Feed[string, string]` — UTF-8
strings both ways, so un-schema-backed JSON flows with zero binding.
`RebindFeed`/`FeedOfDomain` with `BytesCodec()` is the escape hatch to
raw bytes.

### Node-side StateStore

Declare named keys on the spec, then bind typed codecs once — every
Put/Get/Iterate serializes through them:

```go
accounts, _ := client.CreateDomain(ctx, "accounts")
spec, _ := whitewater.NewStateStore("employees")           // resolves to accounts.employees
spec.AddUniqueKey("passport").AddNonUniqueKey("department")  // up to 10 named keys
_, err := accounts.DefineStateStore(ctx, spec, nil)        // nil source = manual store

profiles, _ := whitewater.StoreForDomain[Profile](accounts, "employees")
// named-key postings: a fluent StoreKeySet struct, or a raw map —
_, err = profiles.PutKeySet(ctx, "user-1", profile,
    PassportKeySet{}.WithPassport("P111").WithDepartment("eng"))
_, err = profiles.Put(ctx, "user-1", profile,
    map[string]any{"passport": "P111", "department": "eng"})

row, _ := profiles.Get(ctx, "user-1", whitewater.Strict(500*time.Millisecond))        // row.Value is Profile
rows, _ := profiles.GetBy(ctx, "department", "eng")
_ = profiles.Iterate(ctx, func(r whitewater.Row[string, Profile]) error { export(r); return nil })

// Rebind under different codecs without a wire call:
raw := whitewater.RebindStore(profiles, whitewater.StringCodec(), whitewater.BytesCodec())
```

```go
user, found, _ := profiles.Find(ctx, email)
user, found, _ = profiles.FindBy(ctx, "user_id", userID)
count, _ := counters.GetOrDefault(ctx, bucket, 0)
```

`domain.Store(relative)` binds the default `Store[string, []byte]`; the
byte-level `domain.StateStore(relative)` facade remains for raw access.
The facade is thin — rows live on the Nodes (RF3 + Fjall) and reads
carry freshness evidence. See [docs/stores.md](docs/stores.md) for the
full guide and [docs/pipes.md](docs/pipes.md) for managed stream jobs
(forward/enrich/window) built on subscriptions and stores. When you only need the value, `Find` (primary) and `FindBy`
(named key, first match) return `(V, bool, error)`; `GetOrDefault`
collapses miss handling to one line: `feed.Load` remains the lighter one-shot history
read into any `func([]Record) error` sink.

### Wire-level API

The raw `[]byte` API stays available for full control:

```go
// Provision a Domain, Feed, Writer, and session — every resource
// hangs off the Domain; each method builds its WCL statement for you:
shop, err := client.CreateDomain(ctx, "shop")
_, err = shop.CreateFeed(ctx, "orders")
_, err = shop.CreateWriterFeed(ctx, "orders-writer", whitewater.Rel("orders"))
setup, err := shop.OpenWriterSession(ctx, "orders-writer")
// setup.Results[0].Data holds {"session_epoch": N}
//
// …or raw WCL when a script is clearer:
//   client.WCL(ctx, "CREATE DOMAIN shop; CREATE FEED shop.orders; ...")

// Idempotent append — retrying with the same RequestID is safe.
writer, _ := shop.Writer("orders-writer", sessionEpoch)
eventTime := time.Now().UnixNano()
appended, err := writer.Append(ctx, whitewater.AppendInput{
    RequestID:   "a-stable-request-uuid",
    Key:         []byte("order-42"),
    Payload:     []byte("created"),
    EventTimeNs: &eventTime,
    Metadata:    map[string][]byte{"source": []byte("api")},
})

// Independent durable reader — declared inside the Domain too:
_, _ = shop.CreateReaderFeed(ctx, "orders-reader",
    whitewater.Rel("orders"), whitewater.StartBeginning())
reader, _ := shop.Reader("orders-reader")
session, err := reader.Open(ctx, 64)
page, err := reader.Fetch(ctx, session, 128)
for _, record := range page.Records {
    process(record)
}
if page.DeliveredCursor != nil {
    _, err = reader.Ack(ctx, session, *page.DeliveredCursor)
}
```

## Domains — the strongly-typed front door

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so the
friendliest surface navigates domain → feed → reader/writer:

```go
shop, _ := client.Domain("shop")            // lazy handle — no I/O yet
shop, _ := client.GetDomain(ctx, "shop")    // verifies + materializes now
shop.CreateFeed(ctx, "orders")          // CREATE FEED shop.orders;

shop.Feeds(ctx)                             // [shop.orders shop.returns]
shop.FeedsMatching(ctx, "order")            // prefix-filtered names
shop.Readers(ctx); shop.Writers(ctx)
shop.Subscriptions(ctx); shop.Pipes(ctx)
shop.Indexes(ctx);    shop.StateStores(ctx)
shop.Refresh(ctx)                           // re-pull the snapshot

orders, _ := shop.Feed("orders")            // Feed[string, string] — UTF-8 by default
typed,  _ := whitewater.FeedForDomain[Order](shop, "orders")

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
builder, _ := shop.CreateReader("orders")
reader, _ := builder.ReaderID("fulfillment").  // durable identity — Kafka groupId
    StartPoint(whitewater.StartBeginning()).   // first-time start — auto.offset.reset
    MaxPageSize(256).
    Open(ctx)                                // *FeedReader[string, string]

wbuilder, _ := shop.CreateWriter("orders")
writer, _ := wbuilder.WriterID("ingest").Open(ctx)   // declares shop.ingest

// reader.ForEach pumps records one at a time — deliver, then ack each
// page; return false or an error to stop with the page unacked:
reader.ForEach(ctx, func(m whitewater.FeedMessage[string, string]) (bool, error) {
    return handle(m) == nil, nil
})
// Typed feeds: NewReaderBuilder(client, shop, orders) takes *Feed[K,V].
```

The first catalog accessor issues **one batched control read**
(`describe domain` + every `show` listing) joined by `space_id` /
`feed_id` and cached. `ReaderID` is the progress identity — reopening
the same name resumes the acknowledged frontier, and `StartPoint` only
applies at first declaration (the `auto.offset.reset` mapping). A feed
outside the Domain's prefix fails at `Open`/`Declare`.

### Record schemas

Records stay schema-neutral bytes on the wire; a Feed can still declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```go
client.SetDomainSchema(ctx, "shop", &whitewater.FeedSchema{
    Value: &whitewater.SchemaDecl{Format: "json", Subject: "order-v1"},
})
shop.SetFeedSchema(ctx, "orders", &whitewater.FeedSchema{
    Key: &whitewater.SchemaDecl{Format: "text"},
})

schema, _   := shop.Schema(ctx)                  // the Domain default
effective,_ := shop.FeedSchema(ctx, "orders")    // key=text, value=json/order-v1
declared, _ := shop.DeclaredFeedSchema(ctx, "orders")
shop.ClearFeedSchema(ctx, "orders")              // reverts to inheritance
```

Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, or `custom.<name>`;
compatibility enforcement is a no-op until the registry contract lands.

## Permanent consumers — the Kafka loop without the plumbing

`feed.Consumer(name)` binds the durable named Reader (the consumer-group
equivalent) and owns the fetch → deliver → ack cycle: transient failures
retry with backoff, a fenced session re-opens, progress resumes from the
server-side cursor. Your code ranges a channel of decoded records:

```go
consumer := in.Consumer("enricher")
for msg := range consumer.Messages(ctx) {   // msg.Value is UpdateRequest
    process(msg)
}
if err := consumer.Err(); err != nil { ... } // startup refusal only

// or the handler form, blocking until ctx cancel/Close:
err := in.Consumer("enricher").Run(ctx, func(m FeedMessage[string, UpdateRequest]) error {
    return process(m)
})
```

- The page acks after every record's delivery succeeds — a handler
  error (or an abandoned channel) leaves it unacked, so it redelivers
  (at-least-once: dedupe effects via `RequestID`, route poison pills to
  a DLQ yourself).
- `Run`/`Err` return only for startup refusals and local errors — bad
  names, denied auth — everything after the first open is supervised
  forever. `Close()` signals stop from anywhere (SIGTERM handler or
  inside the handler itself); `Wait()`/`Done()` await full stop.
- `.StartAt`, `.MaxPageSize(n)`, `.Backoff(min, max)`, `.OnError(fn)`,
  `.LastError()` tune and observe it; `feed.Subscribe` remains for
  page-oriented control.

A complete enrichment service — durable consumer, named-key fallback,
DLQ, idempotent outputs, counters — is
[examples/enrichusers](examples/enrichusers/main.go).

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

## Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model
— names, never partitions or Nodes:

```go
// Pipe declaration: the client names the business key, source and
// outputs — the server owns panes, leases and the effect journal. Every
// endpoint is a ScopedRef: Rel resolves in-domain; a *Feed/*Store
// handle or Abs(...) crosses Domains — the Pipe's own name and its
// input Subscription always stay in the declaring Domain.
orders, _ := client.Domain("orders")
_, _ = orders.DefineForwardPipe(ctx, "relay",
    whitewater.Rel("orders-sub"), whitewater.Rel("relayed"))
_, _ = orders.DefineEnrichPipe(ctx, "enrich",
    whitewater.Rel("orders-sub"), whitewater.Rel("enriched"),
    whitewater.Abs("crm.customers"),   // the lookup store lives in crm
    whitewater.EnrichOperation(whitewater.KeyExtractor(),
        whitewater.EnrichOptions{Freshness: whitewater.EnrichLatest,
            OnMiss: whitewater.EnrichMissQuarantine}),
    whitewater.Rel("enrich-dlq"))
_, _ = orders.DefineWindowPipe(ctx, "hourly",
    whitewater.Rel("orders-sub"), whitewater.Abs("stats.orders-hourly"),
    whitewater.WindowOperation(time.Hour, // 1h panes
        whitewater.KeyExtractor(), whitewater.CountAggregation(),
        whitewater.WindowOptions{Time: whitewater.WindowEventTime,
            Watermark: 5 * time.Minute, // 5m horizon
            Grace:     30 * time.Second, // 30s late revisions
            Expire:    2 * time.Hour}),
    whitewater.Rel("hourly-emitted"))

// Pipe lifecycle: describe, pause, resume, drop.
pipe, _ := orders.Pipe("relay")
info, _ := pipe.Describe(ctx)   // stage, operation, outputs, status
_ = pipe.Pause(ctx); _ = pipe.Resume(ctx); _ = pipe.Drop(ctx)
pipes, _ := orders.PipeInfos(ctx)   // the declared catalog, in-domain

// Index queries: exact, prefix, or range predicates over typed values —
// indexes stay same-Domain with their Feed.
crm, _ := client.Domain("crm")
page, _ := crm.QueryIndex(ctx, "byregion",
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

## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`Commands`, `WCL`, and the `*SessionClient` types are the explicit
escape hatches for the same wire contract.

| Area | API |
|---|---|
| Client & transport | `NewClient(apiKey, endpoints, timeout)` · `Healthy(ctx)` · `UseCompression` on feeds/writers |
| Typed Domains | `CreateDomain` / `Domain(name)` lazy / `GetDomain(ctx, name)` eager · `Name` `ID` `Revision` `Describe` `Refresh` · `Feeds` `FeedsMatching` `Readers` `Writers` `Subscriptions` `Pipes` `Indexes` `StateStores` · `Feed(relative)` `FeedForDomain[V]` `FeedOfDomain[K,V]` · `CreateFeed` `CreateReaderFeed` `CreateWriterFeed` `CreateSubscription` `CreateReader`/`CreateWriter` builders (relative) |
| Schemas | `SetDomainSchema` `ClearDomainSchema` on the client · `Domain.SetFeedSchema` `ClearFeedSchema` `Schema` `FeedSchema` `DeclaredFeedSchema` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `Domain.Feed(relative)` `FeedOfDomain[K,V]` `FeedForDomain[V]` `RebindFeed` → `Feed[K,V]` · `UseCompression` · `Records` `Fetch` `Load` · `Writer` `Append` `Reader` `Subscribe` `Consumer` |
| Writing | `FeedWriter.Append(k, v)` `AppendEvent` `AppendAll` · `UseCompression` `SessionEpoch` · idempotent `request_id` per call · Metadata + event time via `FeedEvent` |
| Reading | `FeedReader.Fetch(limit)` `Ack` `AckCursor` `Close` `SessionEpoch` · durable `ReaderID` · `ReaderStart` — `StartBeginning()` `StartNow()` `StartCursor(c)` `StartTimestampNs(ns)` |
| Temporary fetches | `Feed.Fetch(opts)` / `Domain.FeedOps().TemporaryFetch` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `Feed.Load(start, sink)` · `Feed.Records` |
| Consumers | `feed.Consumer(reader)` → `FeedConsumer`: `StartAt` `MaxPageSize` `Backoff` `OnError` `LastError` `Err` · `Messages` channel / `Run` / `Close` `Wait` `Done` |
| Subscription members | `Domain.MemberClient(sub)` / `Domain.SubscriptionRef(name).MemberClient()`: `Join` `Claim` `Renew` `Release` `Fetch` `Ack` — shared progress + RF3 work leases · `Feed.Subscribe` `Domain.Subscribe` managed loops |
| StateStores | `Domain.StateStore` `Domain.Store` / `StoreForDomain[V]` `StoreOfDomain[K,V]` `NewStateStore(relative)`: `AddUniqueKey` `AddNonUniqueKey` `AddKey` `NamedKeys` `Put` `Get` `GetBy` `Find` `FindBy` `GetOrDefault` `Delete` `Page` `Iterate` `Clear` `Drop` · `Strict(d)`/`Limit(n)`/`After(k)` read options · `Domain.DefineStateStore` `DropStateStore` — source Feeds may cross Domains |
| Indexes | `Domain.QueryIndex(relative, IndexPredicate)` → `IndexPage` · `IndexExact` `IndexPrefix` `IndexRange` + `InclusiveBound` `ExclusiveBound` |
| Pipes | `Domain.DefineForwardPipe` `DefineEnrichPipe` `DefineWindowPipe` — every endpoint a `ScopedRef` (`Rel`/`Abs`/`*Feed`/`*Store`/`SubscriptionRef`); Pipe + input Subscription stay in-domain · `Domain.Pipe(relative)` → `Describe` `Pause` `Resume` `Drop` · `PipeInfos` · `ForwardOperation` `EnrichOperation` `WindowOperation` + `EnrichOptions` `WindowOptions` · `CountAggregation` `SumAggregation` `AverageAggregation` · durations are `time.Duration` |
| Catalog lifecycle | `Create*` `Describe*` `Rename*` `Drop*` `List*` on `Domain` for Feed, Writer, Reader, Subscription · client-level `CreateDomain` `DescribeDomain` `RenameDomain` `DropDomain` `ListDomains` `CreateRole` `DescribeRole` `RenameRole` `DropRole` `ListRoles` `ListGrants` · `Domain.OpenWriterSession` `SeekReader` |
| Observability | `DemandFeeds` `DemandReaders` `DemandSubscriptions` `DemandIndexes` `DemandPipes` → `DemandReport`/`DemandRow.Counter` · `HotKeys` → `HotKey.Feed` `ShareBps` `WindowRecords` `WindowBytes` |
| Escape hatches | `WCL(ctx, script)` `WCLWithID` · `Commands(ctx, []map)` raw command batch · `Domain.Writer`/`Reader`/`MemberClient` session clients · `Domain.FeedOps` |
| Errors | `*Error` carries `Kind` + wire `code` · `IsRefused` `IsStaleSession` `IsAmbiguous` `IsConflict` `IsUnavailable` `IsLeaseConflict` |
| Testing | HTTP-transport seam — stub `RoundTripper`/loopback `httptest` servers; see the repo's `*_test.go` fixtures |

## Contract parity

This SDK implements the versioned wire contract in
[`whitewater/sdk/spec`](https://github.com/osodevops/whitewater/tree/main/sdk/spec):
decimal-string signed 64-bit nanoseconds, base64 binary fields, opaque
cursors, UUID request ids, and the shared structured error codes.

## Development

```sh
go build ./...
go test ./...   # unit tests; conformance skips without env vars
```

## Unit-test doubles

`testkit.NewRiverbed(t)` serves the wire contract on a loopback
`httptest.Server` — request-id dedupe, ack-gated redelivery, epoch
fencing, modeled StateStores — so application tests exercise the real
client.

```go
fake := testkit.NewRiverbed(t)
defer fake.Close()
client := fake.Client()
```

See `testkit/testkit_test.go` and the
[testing guide](../whitewater/docs/guides/testing-your-app.md).

## Conformance

The shared fixture corpus runs against a live three-Node Riverbed:

```sh
export WHITEWATER_CONFORMANCE_URLS="http://localhost:7371,http://localhost:7372,http://localhost:7373"
export WHITEWATER_FIXTURE_DIR=/path/to/whitewater/sdk/spec/conformance
go test -run Conformance -v .
```

