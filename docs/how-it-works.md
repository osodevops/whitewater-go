# How the Go SDK works

Everything a caller can do enters through `*whitewater.Client` — and
every HTTP call funnels through one internal `transport`. There are no
other network paths. The SDK is **zero-dependency**: UUIDs are
hand-rolled in `newUUID`, JSON is `encoding/json` only.

## Layers

```
*Client ──────────┬─ WCL / WCLWithID / Commands / Healthy  → POST /v1/admin/*
                  ├─ CreateFeed / DescribeFeed / …         → typed WCL builders
                  ├─ Feeds   → *FeedClient                 → /v1/feeds/*, /v1/readers/temporary/fetch
                  ├─ Writer(name, epoch)                   → /v1/writers/append[-batch]
                  ├─ Reader(name)                          → /v1/readers/open|fetch|ack|close
                  └─ SubscriptionMember(name)              → /v1/subscriptions/members/*
       │                      │
    transport            wire.go / models.go
  (endpoints,         (request building,
   auth, errors)       response mapping)
```

Every method takes a `context.Context` — cancellation and deadlines
propagate to the wire.

### `transport`

Holds the endpoint list and API key. `post`/`get` round-robin the
endpoints, attach `authorization: Bearer <key>`, and map every non-2xx
to a typed `*Error` by the wire `code` field (`errors.go`): `Kind` is
one of `KindStaleSession`, `KindConflict`, `KindLeaseConflict`,
`KindRefused`, `KindUnavailable` (unreachable/timeout), or
`KindAmbiguous` (5xx without a code — the server may or may not have
applied the write; **retry with the same `request_id`**, never a new
one).

### `wire.go` / `models.go`

The only place request bodies are built and responses decoded. Keys,
payloads, and metadata values are base64 `[]byte`; `event_time_ns`/
`ingest_time_ns` are `int64` nanoseconds on decimal strings. `Cursor`
is `type Cursor string` — opaque, never parsed or ordered.

## Resource management — two doors, one room

`c.WCL(ctx, script)` sends raw WCL to `POST /v1/admin/wcl`. The typed
methods in `resources.go` are thin builders over it —
`c.CreateFeed(ctx, "orders.events")` serializes
`CREATE FEED orders.events` and calls `WCL` internally.

Two validators run before interpolation, returning an `error` locally:

- `dottedName` — domains/feeds: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`,
  ≤512 bytes.
- `resourceName` — writers/readers/subscriptions/roles:
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

`ReaderStart` supplies `START AT` positions: `StartBeginning()`,
`StartNow()`, `StartCursor(c)` (quotes escaped), `StartTimestampNs(ns)`.

Every management call returns `ControlExecution` — `RequestID`,
`Revision`, `Authority`, `Warning`, and one `StatementResult` per WCL
statement.

## Reading

Three shapes, in increasing durability:

1. **`c.Feeds.Records(ctx, feed, limit, after)`** — one-shot committed
   history.
2. **`c.Feeds.TemporaryFetch(ctx, feed, options)`** — anonymous
   ephemeral reader; `tail` + `wait_ms` is a server-side long-poll
   returning early when records arrive. Continue with `NextCursor`,
   never the last record's cursor.
3. **`c.Reader(name)`** — durable independent cursor:
   `Open` → `Fetch` → `Ack(deliveredCursor)` → `Close`. Progress
   survives restarts.

`c.SubscriptionMember(sub)` is the lease vocabulary for cooperating
consumers: `Join` → `Claim`/`Renew`/`Release` → `Fetch` → `Ack`.

## Writing

- **Named session**: `c.OpenWriterSession(ctx, name)` returns a
  `session_epoch` in `Results[0].Data`; bind `c.Writer(name, epoch)`
  and `Append` with a stable `RequestID` per logical write.
- **Ad-hoc**: `c.Feeds.Append(ctx, feed, record)` — ephemeral writer
  identity; same request-id dedup rule.

## Testing

Unit tests stub the wire with `net/http/httptest`, asserting
serialized statements and response mapping — no live Riverbed
required. `conformance_test.go` replays the shared
`sdk/spec/conformance` fixtures when `WHITEWATER_ENDPOINTS` +
`WHITEWATER_SPEC_DIR` are set; skipped otherwise.
