# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes run server-side — your process
does not need to stay alive for a Pipe to keep going.

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```go
client := whitewater.NewClient(
    os.Getenv("WHITEWATER_API_KEY"),
    []string{"localhost:7371"},
    30*time.Second,
)

// Pipes are declared inside a Domain — its name resolves there, and so
// does its input Subscription (the Pipe's own durable frontier). Every
// other endpoint is a ScopedRef: Rel("...") composes in-domain, while
// a *Feed/*Store handle or Abs("other.name") points across Domains.
ops, _ := client.CreateDomain(ctx, "ops")
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```go
_, err = ops.CreateSubscription(ctx, "orders.sub",
    whitewater.Rel("orders"), whitewater.StartBeginning())
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```go
spec, err := whitewater.NewStateStore("customers")      // -> ops.customers
if err != nil { /* invalid relative name */ }
spec.AddKey("email", true, whitewater.JsonExtractor("$.email")).
    AddKey("tier", false, whitewater.JsonExtractor("$.tier"))
// the source Feed may cross Domains — pass its *Feed handle or Abs(...)
store, err := ops.DefineStateStore(ctx, spec, whitewater.Rel("customer.changes"))
```

## 2. Forward pipes

```go
_, err = ops.DefineForwardPipe(ctx,
    "orders.relay",                    // pipe name -> ops.orders.relay
    whitewater.Rel("orders.sub"),      // input subscription (same Domain)
    whitewater.Rel("orders.relayed"))  // output feed
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```go
_, err = ops.DefineEnrichPipe(ctx,
    "orders.enrich",
    whitewater.Rel("orders.sub"),
    whitewater.Rel("enriched"),
    whitewater.Rel("customers"), // lookup store — may be Abs()/a *Store
    whitewater.EnrichOperation(
        whitewater.JsonExtractor("$.customerId"),
        whitewater.EnrichOptions{
            Freshness: whitewater.EnrichLatest,     // or EnrichStrict
            Named:     "email",                     // named key; "" = primary
            OnMiss:    whitewater.EnrichMissQuarantine, // Forward | Drop | Quarantine
        }),
    whitewater.Rel("enrich.misses")) // quarantine feed
```

## 4. Window pipes

A window Pipe's output store must be declared before the Pipe — and it
must carry non-unique named keys `key` and `pane` so pane rows stay
enumerable without composite row keys:

```go
panes, err := whitewater.NewStateStore("order.counts")   // -> ops.order.counts
if err != nil {
    // handle
}
panes.AddKey("key", false).AddKey("pane", false)
_, err = ops.DefineStateStore(ctx, panes, nil)

_, err = ops.DefineWindowPipe(ctx,
    "orders.hourly",
    whitewater.Rel("orders.sub"),
    whitewater.Rel("order.counts"), // pane store — may be Abs()/a *Store
    whitewater.WindowOperation(
        time.Hour, // 1h panes
        whitewater.JsonExtractor("$.region"),
        whitewater.SumAggregation(whitewater.JsonExtractor("$.total")),
        whitewater.WindowOptions{
            Time:      whitewater.WindowEventTime, // or WindowIngestTime
            Watermark: 5 * time.Minute,            // event-time close horizon
            Grace:     time.Minute,                // revise emitted panes 1min
            Expire:    24 * time.Hour,             // keep panes 24h
            KeyLimit:  10_000,
        }),
    whitewater.Rel("hourly.emits")) // optional emit feed for closed panes
```

### Lateness models — pick exactly one

- `MaxLate` — ingest-clock bound; later arrivals quarantine into a
  countable `late` row.
- `Watermark` — event-time horizon `max(event_time) − watermark`
  closes a pane once it passes the pane end (refused under
  `WindowIngestTime`); `Grace` lets late events revise an emitted pane
  before counting into `late`.

An emit feed requires a declared lateness model. `WindowOptions`
durations are `time.Duration` — the zero value means "unset"; the SDK
extracts nanoseconds internally for the wire.

### Aggregations

```go
whitewater.CountAggregation()
whitewater.SumAggregation(whitewater.JsonExtractor("$.total"))
whitewater.AverageAggregation(whitewater.JsonExtractor("$.rtt"))
```

## 5. Lifecycle

```go
pipe, _ := ops.Pipe("orders.relay")

info, _ := pipe.Describe(ctx) // stage, operation, outputs, status
_ = pipe.Pause(ctx)           // cooperative; the frontier stays put
_ = pipe.Resume(ctx)          // restarts from the durable frontier
_ = pipe.Drop(ctx)

all, _ := ops.PipeInfos(ctx)
```

`pause` is durable and idempotent — nothing is dropped while paused.
`Describe` returns `stage` (`declared`/`paused`), the opaque `Operation`
node, and output Feed/Store IDs.

## 6. Watching demand

```go
demand, _ := client.DemandPipes(ctx)
for _, row := range demand.Rows {
    fmt.Println(row.Name, row.Stage)
}
```

## 7. Failure semantics

- Declarations commit atomically inside the command batch — submit
  subscription + `define_pipe` in one `client.Commands` call to apply
  both or neither.
- The Pipe's name and input Subscription always live in the declaring
  Domain; endpoints reach across Domains only through an explicit
  `Abs(...)`, a `*Feed`/`*Store` handle, or a `SubscriptionRef` — the
  wire always carries qualified names. See the purchasing → warehouse
  flow in `examples/crossdomain/main.go`.
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
