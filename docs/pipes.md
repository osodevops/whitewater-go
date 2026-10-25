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
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe:

```go
_, err = client.Commands(ctx, []map[string]any{{
    "command": "create_subscription",
    "name":    "orders.sub",
    "feed":    "ops.orders",
    "start":   map[string]any{"kind": "beginning"},
}})
```

`enrich` and `window` pipes need a **StateStore** (see `docs/stores.md`):

```go
spec, err := whitewater.NewStateStore("ops.customers")
if err != nil { /* invalid dotted name */ }
spec.AddKey("email", true, whitewater.JsonExtractor("$.email")).
    AddKey("tier", false, whitewater.JsonExtractor("$.tier"))
store, err := client.StateStores.Define(ctx, spec, "ops.customer.changes")
```

## 2. Forward pipes

```go
_, err = client.DefineForwardPipe(ctx,
    "ops.orders.relay",   // pipe name (dotted)
    "orders.sub",         // source subscription
    "ops.orders.relayed") // output feed
```

## 3. Enrich pipes

Point-lookup enrichment: project a key with an `Extractor`, resolve it
against the store, and copy the matched row into output metadata
(`enrich.payload`, `enrich.matched`). Resolution rides inside the effect
journal — replayed outputs are byte-identical.

```go
_, err = client.DefineEnrichPipe(ctx,
    "ops.orders.enrich",
    "orders.sub",
    "ops.enriched",
    "ops.customers", // lookup store
    whitewater.EnrichOperation(
        whitewater.JsonExtractor("$.customerId"),
        whitewater.EnrichOptions{
            Freshness: whitewater.EnrichLatest,     // or EnrichStrict
            Named:     "email",                     // named key; "" = primary
            OnMiss:    whitewater.EnrichMissQuarantine, // Forward | Drop | Quarantine
        }),
    "ops.enrich.misses") // quarantine feed (required for Quarantine)
```

## 4. Window pipes

A window Pipe's output store must be declared before the Pipe — and it
must carry non-unique named keys `key` and `pane` so pane rows stay
enumerable without composite row keys:

```go
panes, err := whitewater.NewStateStore("ops.order.counts")
if err != nil {
    // handle
}
panes.AddKey("key", false).AddKey("pane", false)
_, err = client.StateStores.Define(ctx, panes, "")

_, err = client.DefineWindowPipe(ctx,
    "ops.orders.hourly",
    "orders.sub",
    "ops.order.counts", // pane store (declared above)
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
    "ops.hourly.emits") // optional emit feed for closed panes
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
pipe := client.Pipe("ops.orders.relay")

info, _ := pipe.Describe(ctx) // stage, operation, outputs, status
_ = pipe.Pause(ctx)           // cooperative; the frontier stays put
_ = pipe.Resume(ctx)          // restarts from the durable frontier
_ = pipe.Drop(ctx)

all, _ := client.Pipes(ctx)
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
- Pipe delivery is at-least-once; effects journal lookups/accumulation so
  replayed outputs are byte-identical.
