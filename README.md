# whitewater-go

Go SDK for Whitewater. Implements the versioned `whitewater-protocol`
wire contract defined in the main repository under `sdk/spec/`
(`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Zero dependencies: `net/http` transport, `context.Context` cancellation,
`[]byte` keys/payloads/metadata, `int64` nanoseconds, opaque `Cursor`
strings.

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

```go
client := whitewater.NewClient("api-key",
    []string{"https://node1:7070", "https://node2:7070", "https://node3:7070"},
    30*time.Second)                          // per-request timeout
```

`Healthy(ctx)` checks liveness. The client also carries the flat
administration surface — `WCL`/`WCLWithID` scripts, raw `Commands`
batches, cluster health, and demand reports — while every Feed, Writer,
Reader, Subscription, Member, Pipe, StateStore, and Index is reached
**only** through a `Domain`.

Whitewater's public model is `domain -> feed -> key -> cursor ->
subscription`: partitions, ranges, replicas, and physical positions are
internal implementation details — cursors are opaque tokens, never
parsed.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Writers, Readers, Subscriptions,
StateStores, Indexes, and Pipes resolve their relative names inside the
owning Domain, so a `"fulfillment"` Reader declared on `shop` is
`shop.fulfillment` everywhere it matters: progress, leases, WCL, demand
reports, and access control all scope to it. Cross-Domain references are
possible but always explicit (`whitewater.Abs(...)`), which makes
accidental cross-wiring of two tenants' resources a deliberate act
rather than a silent mistake.

```go
shop, _ := client.CreateDomain(ctx, "shop")   // CREATE DOMAIN shop;
shop, _ = client.GetDomain(ctx, "shop")       // verifies + materializes now
shop.CreateFeed(ctx, "orders")                // CREATE FEED shop.orders;

shop.Feeds(ctx)                               // [shop.orders shop.returns]
shop.FeedsMatching(ctx, "order")              // prefix-filtered names
shop.Readers(ctx); shop.Writers(ctx)
shop.Subscriptions(ctx); shop.Pipes(ctx)
shop.Indexes(ctx);    shop.StateStores(ctx)
shop.Refresh(ctx)                             // re-pull the cached snapshot

// Everything a service needs hangs off the Domain — relative names
// resolve inside it:
builder, _ := shop.CreateReader("orders")
reader, _ := builder.
    ReaderID("fulfillment").                        // durable identity — Kafka groupId
    StartPoint(whitewater.StartBeginning()).        // first-time start — auto.offset.reset
    MaxPageSize(256).                               // records per server fetch
    Open(ctx)                                       // *FeedReader[string, string]

wbuilder, _ := shop.CreateWriter("orders")
writer, _ := wbuilder.
    WriterID("ingest").                             // durable producer identity — required
    Open(ctx)                                       // declares shop.ingest
```

`client.Domain(name)` is the lazy variant for code paths that only
compose names; the first catalog accessor issues **one batched control
read** (`describe domain` + every `show` listing) joined by `space_id` /
`feed_id` and cached. `ReaderID` is the progress identity — reopening
the same name resumes the acknowledged frontier, and `StartPoint` only
applies at first declaration (the `auto.offset.reset` mapping).
`WriterID` is required because it's the Writer's transactional identity:
the fencing epoch binds to it and the retry-dedup ledger lives under it.

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
effective,_ := shop.FeedSchema(ctx, "orders")    // effective: key=text, value=json/order-v1
declared, _ := shop.DeclaredFeedSchema(ctx, "orders")  // the Feed's own sides only
shop.ClearFeedSchema(ctx, "orders")              // reverts to inheritance
```

Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, or `custom.<name>`;
compatibility enforcement is a no-op until the registry contract lands.

## Working with Feeds

`Feed[K, V]` binds a feed to a key codec and a payload codec once;
every write encodes and every read decodes through them — application
code never touches bytes. `domain.Feed(relative)` returns the default
`Feed[string, string]` — UTF-8 both ways; `FeedForDomain[V]` infers
codecs from types; `FeedOfDomain` takes explicit codecs; `RebindFeed`
rebinds under different codecs with no wire call:

```go
type Order struct {
    ID    string `json:"id"`
    Total int    `json:"total"`
}

orders, _ := whitewater.FeedForDomain[Order](shop, "orders")  // JSON payloads

// A protobuf payload is one FuncCodec away:
proto := whitewater.FuncCodec(
    func(u *User) ([]byte, error) { return proto.Marshal(u) },
    func(b []byte) (*User, error) { var u User; return &u, proto.Unmarshal(b, &u) })
users, _ := whitewater.FeedOfDomain(app, "users", whitewater.StringCodec(), proto)
```

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side Cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```go
builder, _ := shop.CreateReader("orders")
reader, _ := builder.
    ReaderID("fulfillment").                    // durable consume identity
    StartPoint(whitewater.StartBeginning()).    // first-time start position
    MaxPageSize(256).                           // max records per fetch
    Open(ctx)
defer reader.Close(ctx)                         // releases the session fence

page, _ := reader.Fetch(ctx, 0)                 // up to MaxPageSize records
for _, m := range page.Records {
    fmt.Println(*m.Key, m.Value.Total)          // Order, not []byte
}
reader.Ack(ctx)                                 // commits the delivered frontier
```

`reader.ForEach` runs the same fetch→ack cycle for you — deliver, then
ack each page; return false or an error to stop with the page unacked:

```go
reader.ForEach(ctx, func(m whitewater.FeedMessage[string, string]) (bool, error) {
    return handle(m) == nil, nil
})
```

For ad-hoc reads without a durable identity, `Feed.Records`,
`Feed.Fetch` (`tail`/`wait_ms` long-poll, `after_event_time_ns` seeks),
and `Feed.Load(start, sink)` one-shot replays cover committed history:

```go
messages, _ := orders.Records(ctx, 100, nil)    // first 100 committed records
_ = orders.Load(ctx, whitewater.StartBeginning(),
    func(batch []whitewater.FeedMessage[string, Order]) error {
        return replay(batch)
    })
```

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `Domain.MemberClient(sub)` (or
`Domain.SubscriptionRef(name).MemberClient()`) joins a member and runs
the work loop:

```go
member, _ := shop.MemberClient("orders-sub")
_ = member.Join(ctx, "member-1")                // register this member
work, _ := member.Claim(ctx, "")                // RF3-fenced work lease
page, _ := member.Fetch(ctx, work, 256)         // records for this member
member.Ack(ctx, work, page)                     // commits shared progress
member.Release(ctx, work)                       // or Transfer to another member
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member.

### The read loop — consumers

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

### Writing

```go
// Writer auto-declares the named Writer — Append encodes for you and
// blocks until the record is majority-committed (at least 2 of 3 Nodes):
writer, _ := orders.Writer(ctx, "ingest")
appended, _ := writer.Append(ctx, "order-42", Order{ID: "order-42", Total: 99})
// appended: MessageID, Cursor, Deduplicated, Durability — the ack payload

// Idempotent events: pin RequestID for caller-driven retries —
// the same identity replays its committed result instead of writing twice.
_, _ = writer.AppendEvent(ctx, whitewater.FeedEvent[string, Order]{
    RequestID:   "a-stable-request-uuid",        // retry-safe identity
    Key:         whitewater.Ptr("order-42"),
    Value:       Order{ID: "order-42", Total: 99},
    Metadata:    whitewater.TextMetadata(map[string]string{"source": "api"}),
})

// One commit for a group of records:
_, _ = writer.AppendAll(ctx, []whitewater.FeedEvent[string, Order]{e1, e2})
```

Every append carries an idempotent `request_id` — retrying a timed-out
append with the same identity deduplicates; reusing it with different
contents is refused. Writers are epoch-fenced sessions: a second writer
claiming `ingest` fences the first, so an ambiguous timeout can never
double-commit under retry. `UseCompression` opts into LZ4; record
`event_time_ns`/`ingest_time_ns` are signed 64-bit `int64` nanoseconds
(decimal strings on the wire).

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```go
accounts, _ := client.CreateDomain(ctx, "accounts")
spec, _ := whitewater.NewStateStore("employees")   // resolves to accounts.employees
spec.AddUniqueKey("passport").                     // one row per value
    AddNonUniqueKey("department")                  // value → many rows; up to 10 keys
_, err := accounts.DefineStateStore(ctx, spec, nil) // nil source = manual store
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `GetBy` returns the single match and a second claim on the
same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`GetBy` returns every match as a bounded page.

Two store kinds: **manual** (your app puts rows — nil source) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `whitewater.KeyExtractor()` / `whitewater.MetadataExtractor` /
`whitewater.JSONExtractor` — required on feed-sourced keys, refused on
manual ones).

### Putting and deleting

Named-key postings take a fluent `StoreKeySet` struct or a raw map:

```go
profiles, _ := whitewater.StoreForDomain[Profile](accounts, "employees")

_, err = profiles.PutKeySet(ctx, "user-1", profile,
    PassportKeySet{}.WithPassport("P111").WithDepartment("eng"))   // typed key set
_, err = profiles.Put(ctx, "user-1", profile,
    map[string]any{"passport": "P111", "department": "eng"})       // raw map — no checking
_ = profiles.Delete(ctx, "user-1")            // mutation id generated
_ = profiles.Clear(ctx)                       // one journaled ClearAll
```

Every mutation carries an idempotent `mutation_id` — generated per call,
or pinned for caller-driven retries.

### Reading

```go
row, _ := profiles.Get(ctx, "user-1",
    whitewater.Strict(500*time.Millisecond))   // bounded-wait for committed frontier
user, found, _ := profiles.Find(ctx, email)    // (V, bool, error) — Optional-style
user, found, _ = profiles.FindBy(ctx, "user_id", userID)  // named-key first match
count, _ := counters.GetOrDefault(ctx, bucket, 0)         // counters, one line

rows, _ := profiles.GetBy(ctx, "department", "eng")       // every match
page, _ := profiles.Page(ctx, afterKey, 256)              // one bounded page
_ = profiles.Iterate(ctx, func(r whitewater.Row[string, Profile]) error {
    return export(r)                                       // primary-key order
})
```

Rows carry freshness evidence: `Get`/`GetBy` read immediately, while
`Strict(d)` bounded-waits for the replica to apply its committed
frontier rather than answering stale. `domain.Store(relative)` binds the
default `Store[string, []byte]`; the byte-level
`domain.StateStore(relative)` facade remains for raw access. See
[docs/stores.md](docs/stores.md) for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. Every endpoint is a `ScopedRef`: `Rel` resolves in-domain; a
`*Feed`/`*Store` handle or `Abs(...)` crosses Domains explicitly. The
Pipe's own name and its input Subscription always stay in the declaring
Domain.

### Simple — forwarding

```go
orders, _ := client.Domain("orders")
_, _ = orders.DefineForwardPipe(ctx, "relay",
    whitewater.Rel("orders-sub"),               // source Subscription
    whitewater.Rel("relayed"))                  // output Feed, same Domain
```

### Enrichment — StateStore lookups

```go
_, _ = orders.DefineEnrichPipe(ctx, "enrich",
    whitewater.Rel("orders-sub"), whitewater.Rel("enriched"),
    whitewater.Abs("crm.customers"),            // cross-Domain lookup store
    whitewater.EnrichOperation(whitewater.KeyExtractor(),  // join on record key
        whitewater.EnrichOptions{Freshness: whitewater.EnrichLatest,
            OnMiss: whitewater.EnrichMissQuarantine}),     // misses → DLQ
    whitewater.Rel("enrich-dlq"))
```

### Time-based — windowing

```go
_, _ = orders.DefineWindowPipe(ctx, "hourly",
    whitewater.Rel("orders-sub"), whitewater.Abs("stats.orders-hourly"),
    whitewater.WindowOperation(time.Hour,           // 1h panes
        whitewater.KeyExtractor(), whitewater.CountAggregation(),
        whitewater.WindowOptions{Time: whitewater.WindowEventTime,
            Watermark: 5 * time.Minute,             // 5m horizon
            Grace:     30 * time.Second,            // 30s late revisions
            Expire:    2 * time.Hour}),             // pane expiry
    whitewater.Rel("hourly-emitted"))
```

### Indexes and observability

```go
// Pipe lifecycle: describe, pause, resume, drop.
pipe, _ := orders.Pipe("relay")
info, _ := pipe.Describe(ctx)   // stage, operation, outputs, status
_ = pipe.Pause(ctx); _ = pipe.Resume(ctx); _ = pipe.Drop(ctx)
pipes, _ := orders.PipeInfos(ctx)

// Index queries: exact, prefix, or range predicates over decoded rows.
page, _ := crm.QueryIndex(ctx, "byregion",
    whitewater.IndexRange([]any{"emea"},
        whitewater.InclusiveBound(0), whitewater.ExclusiveBound(99)),
    50,        // limit
    true)      // strict: wait for the committed target rather than stale
for _, row := range page.Rows { /* row.Key / row.Payload bytes */ }

// Demand observability: typed rows + per-domain rollups.
report, _ := client.DemandFeeds(ctx)
appended, _ := report.Rows[0].Counter("appended_records")
client.DemandReaders(ctx); client.DemandSubscriptions(ctx)
client.DemandIndexes(ctx); client.DemandPipes(ctx)
hot, _ := client.HotKeys(ctx)     // dominant-key evidence per feed
```

Pipe operation payloads (`PipeInfo.Operation`) and demand counter
fields (`DemandRow.Fields`, `HotKey.Evidence`) stay opaque maps so
server-side variants evolve without an SDK bump. `QueryIndex` values
accept the same typed scalars as StateStore named values — floats are
refused because the wire carries exact `i64`/`text`/`bytes`. See
[docs/pipes.md](docs/pipes.md) for the full managed-job guide.

## Errors, cursors, and time

Failures return `*whitewater.Error` carrying `Kind` and the wire `code`;
class helpers select policy:

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

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are signed 64-bit `int64` nanoseconds
and travel as decimal strings on the wire; `time.Duration` covers
window and delay declarations.

## Build

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

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`Commands`, `WCL`, and the `*SessionClient` types are the explicit
escape hatches for the same wire contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `NewClient(apiKey, endpoints, timeout)` | api key, `[]string`, `time.Duration` | Client construction |
| `Healthy(ctx)` | ctx | Liveness check |
| `UseCompression` | — | LZ4 on feeds/writers |
| **Domains — the only door** | | |
| `CreateDomain` / `Domain(name)` / `GetDomain(ctx, name)` | relative name | Declare / lazy / eager handle |
| `Feed(relative)` `FeedForDomain[V]` `FeedOfDomain[K,V]` `RebindFeed` | name, codecs | Typed `Feed[K,V]` handles |
| `CreateFeed` `CreateReaderFeed` `CreateWriterFeed` `CreateSubscription` `CreateReader`/`CreateWriter` builders | relative name | Declare + open inside the Domain |
| `Feeds` `FeedsMatching` `Readers` `Writers` `Subscriptions` `Pipes` `Indexes` `StateStores` | ctx, optional prefix | Domain catalog views |
| `Name` `ID` `Revision` `Describe` `Refresh` | — | Domain metadata |
| `SetDomainSchema` `ClearDomainSchema` (client) `SetFeedSchema` `ClearFeedSchema` `Schema` `FeedSchema` `DeclaredFeedSchema` | `FeedSchema`/`SchemaDecl` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `Records` `Fetch` `Load(start, sink)` | limit, cursor, `ReaderStart` | Committed history reads |
| `Writer` `Reader` `Subscribe` `Consumer` | ctx, relative name, options | Bound sessions + loops |
| **Writing** | | |
| `FeedWriter.Append(k, v)` `AppendEvent` `AppendAll` | ctx, key, value, `FeedEvent` | Blocking write — `AppendResult` on majority commit |
| `SessionEpoch` `UseCompression` `Reopen` | — | Session + lifecycle |
| **Reading** | | |
| `FeedReader.Fetch(limit)` `Ack` `AckCursor` `Close` `SessionEpoch` `ForEach` | ctx, page bound, cursor | Manual + pumped durable-Reader loops |
| `ReaderStart` | `StartBeginning()` `StartNow()` `StartCursor(c)` `StartTimestampNs(ns)` | First-declare start position |
| `feedOps().TemporaryFetch` `Feed.Load` `Feed.Records` | `after_event_time_ns`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.Consumer(name)` → `FeedConsumer` | `StartAt` `MaxPageSize` `Backoff` `OnError` `LastError` | Supervised durable consumer: `Messages`/`Run`/`Close`/`Wait`/`Done` |
| **Subscription members** | | |
| `Domain.MemberClient(sub)` `SubscriptionRef(name).MemberClient()` | `Join` `Claim` `Renew` `Release` `Fetch` `Ack` `Transfer` | Shared progress + RF3 work leases |
| `Feed.Subscribe` `Domain.Subscribe` | reader, feed, handler | Managed pump loops |
| **StateStores** | | |
| `NewStateStore(relative)` `DefineStateStore` `DropStateStore` `Domain.StateStore` `Domain.Store` `StoreForDomain[V]` `StoreOfDomain[K,V]` | spec, source Feed, codecs | Store lifecycle + typed handles |
| `Put` `PutKeySet` `Delete` `Clear` | key, value, named-key postings | Mutations |
| `Get` `GetBy` `Find` `FindBy` `GetOrDefault` `Page` `Iterate` | key, named key, `Strict(d)`/`Limit(n)`/`After(k)` | Row + named-key reads |
| `AddUniqueKey` `AddNonUniqueKey` `AddKey` `NamedKeys` `Drop` | key name | Spec + administration |
| **Indexes** | | |
| `QueryIndex(relative, predicate, limit, strict)` | `IndexExact`/`IndexPrefix`/`IndexRange` + `InclusiveBound`/`ExclusiveBound` | Decoded index rows |
| **Pipes** | | |
| `DefineForwardPipe` `DefineEnrichPipe` `DefineWindowPipe` | name, `ScopedRef` (`Rel`/`Abs`/handles), `EnrichOptions`/`WindowOptions` | Managed jobs |
| `Pipe(relative)` `PipeInfos` | — | `Describe` `Pause` `Resume` `Drop` |
| `CountAggregation` `SumAggregation` `AverageAggregation` | — | Window aggregations (`time.Duration` throughout) |
| **Catalog lifecycle** | | |
| `Create*` `Describe*` `Rename*` `Drop*` `List*` | names | Feed/Writer/Reader/Subscription lifecycle on Domain + `CreateDomain` `DescribeDomain` `RenameDomain` `DropDomain` `ListDomains` + `CreateRole`/`DescribeRole`/`RenameRole`/`DropRole` `ListRoles` `ListGrants` on the client |
| **Observability** | | |
| `DemandFeeds` `DemandReaders` `DemandSubscriptions` `DemandIndexes` `DemandPipes` `HotKeys` `DemandHotKeys` | ctx, optional feed | `DemandReport`/`DemandRow.Counter` + `HotKey` evidence |
| **Escape hatches & errors** | | |
| `WCL(ctx, script)` `WCLWithID` `Commands(ctx, []map)` `Domain.Writer`/`Reader`/`MemberClient`/`FeedOps` | script, raw commands | Byte-level wire access |
| `*Error` | `Kind`, wire `code` | `IsRefused` `IsStaleSession` `IsAmbiguous` `IsConflict` `IsUnavailable` `IsLeaseConflict` |
| `testkit.NewRiverbed(t)` | `*testing.T` | Loopback wire-contract test double |
