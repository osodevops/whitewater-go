package whitewater

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// SchemaDecl is one side of a Feed's schema metadata — format plus
// optional subject/definition text. Formats: "text", "json",
// "avro", "protobuf", "text", "bytes", or "custom.<name>". Records stay schema-neutral
// bytes on the wire; declarations are catalog metadata for tooling,
// validation, and compatibility — not yet an enforced registry.
type SchemaDecl struct {
	Format     string
	Subject    string
	Definition string
}

func (d *SchemaDecl) wire() map[string]any {
	out := map[string]any{"format": d.Format}
	if d.Subject != "" {
		out["subject"] = d.Subject
	}
	if d.Definition != "" {
		out["definition"] = d.Definition
	}
	return out
}

func schemaDeclFrom(row map[string]any) *SchemaDecl {
	if row == nil {
		return nil
	}
	decl := &SchemaDecl{}
	decl.Format, _ = row["format"].(string)
	decl.Subject, _ = row["subject"].(string)
	decl.Definition, _ = row["definition"].(string)
	return decl
}

// FeedSchema is the declared key/value schema pair — either side may be
// nil when undeclared. A Feed's declaration overrides the named sides
// of its Domain's default.
type FeedSchema struct {
	Key   *SchemaDecl
	Value *SchemaDecl
}

func (s *FeedSchema) wire() map[string]any {
	out := map[string]any{}
	if s != nil && s.Key != nil {
		out["key"] = s.Key.wire()
	}
	if s != nil && s.Value != nil {
		out["value"] = s.Value.wire()
	}
	return out
}

func feedSchemaFrom(raw any) *FeedSchema {
	row, ok := raw.(map[string]any)
	if !ok || row == nil {
		return nil
	}
	key, _ := row["key"].(map[string]any)
	value, _ := row["value"].(map[string]any)
	if key == nil && value == nil {
		return nil
	}
	return &FeedSchema{Key: schemaDeclFrom(key), Value: schemaDeclFrom(value)}
}

// Domain is a strongly-typed handle on one Domain — the administrative
// namespace owning every Feed it prefixes (shop owns shop.orders).
//
// Client.Domain returns a lazy handle: Feed name composition costs no
// I/O and the first catalog accessor (Feeds, Readers, Describe, â€¦)
// materializes the Domain's catalog view in one batched control read.
// Client.GetDomain verifies the Domain exists up front. Refresh
// re-pulls the snapshot.
type Domain struct {
	client *Client
	name   string

	mu   sync.Mutex
	snap *domainSnapshot
}

type domainSnapshot struct {
	domainID      string
	describe      json.RawMessage
	revision      int64
	feeds         []string
	feedIDs       map[string]bool
	schema        *FeedSchema
	feedSchemas   map[string]*FeedSchema // effective
	declaredFeed  map[string]*FeedSchema // the Feed's own sides
	readers       []string
	writers       []string
	subscriptions []string
	pipes         []string
	indexes       []string
	stateStores   []string
}

// Domain returns a lazy Domain handle — no I/O until a catalog
// accessor runs; Feed name composition stays free.
func (c *Client) Domain(name string) (*Domain, error) {
	dotted, err := dottedName(name)
	if err != nil {
		return nil, err
	}
	return &Domain{client: c, name: dotted}, nil
}

// GetDomain is Domain plus an immediate snapshot load — it verifies
// the Domain exists now.
func (c *Client) GetDomain(ctx context.Context, name string) (*Domain, error) {
	domain, err := c.Domain(name)
	if err != nil {
		return nil, err
	}
	if err := domain.Refresh(ctx); err != nil {
		return nil, err
	}
	return domain, nil
}

// Name is the Domain's dotted name.
func (d *Domain) Name() string { return d.name }

// ID is the Domain's immutable id — it survives renames.
func (d *Domain) ID(ctx context.Context) (string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return "", err
	}
	return snap.domainID, nil
}

// Revision is the catalog revision the snapshot was taken at.
func (d *Domain) Revision(ctx context.Context) (int64, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return 0, err
	}
	return snap.revision, nil
}

// Describe returns the raw DESCRIBE DOMAIN row — status,
// created_at_ns, history_policy.
func (d *Domain) Describe(ctx context.Context) (json.RawMessage, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.describe, nil
}

// Schema returns the Domain's declared schema default — nil when
// undeclared. Every Feed without its own declaration inherits it.
func (d *Domain) Schema(ctx context.Context) (*FeedSchema, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.schema, nil
}

// FeedSchema returns the effective schema for a Feed — the Feed's
// declared sides layered over the Domain default. nil when nothing
// is declared anywhere (schema-neutral bytes).
func (d *Domain) FeedSchema(ctx context.Context, feed string) (*FeedSchema, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.feedSchemas[feed], nil
}

// DeclaredFeedSchema returns only the sides the Feed itself declares —
// nil when it inherits the Domain default wholesale.
func (d *Domain) DeclaredFeedSchema(ctx context.Context, feed string) (*FeedSchema, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.declaredFeed[feed], nil
}

// Feeds lists the Domain's Feed names
func (d *Domain) Feeds(ctx context.Context) ([]string, error) {
	return d.FeedsMatching(ctx, "")
}

// FeedsMatching lists the Domain's Feed names whose relative form
// (shop.orders -> orders) starts with prefix.
func (d *Domain) FeedsMatching(ctx context.Context, prefix string) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, feed := range snap.feeds {
		if prefix == "" || strings.HasPrefix(d.relative(feed), prefix) {
			out = append(out, feed)
		}
	}
	return out, nil
}

// Readers lists declared Readers on this Domain's Feeds, sorted.
func (d *Domain) Readers(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.readers, nil
}

// Writers lists declared Writers on this Domain's Feeds, sorted.
func (d *Domain) Writers(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.writers, nil
}

// Subscriptions lists declared Subscriptions in this Domain, sorted.
func (d *Domain) Subscriptions(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.subscriptions, nil
}

// Pipes lists declared Pipes in this Domain, sorted.
func (d *Domain) Pipes(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.pipes, nil
}

// Indexes lists declared Indexes in this Domain, sorted.
func (d *Domain) Indexes(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.indexes, nil
}

// StateStores lists declared StateStores in this Domain, sorted.
func (d *Domain) StateStores(ctx context.Context) ([]string, error) {
	snap, err := d.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.stateStores, nil
}

// Refresh re-pulls the catalog snapshot — the next accessor sees
// fresh rows.
func (d *Domain) Refresh(ctx context.Context) error {
	snap, err := d.load(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.snap = snap
	d.mu.Unlock()
	return nil
}

// CreateFeed executes CREATE FEED inside this Domain —
// d.CreateFeed("orders") declares shop.orders. Refresh (or a lazy
// accessor's first load) surfaces it through Feeds/FeedsMatching.
func (d *Domain) CreateFeed(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.createFeed(ctx, full)
}

// CreateReaderFeed executes CREATE READER <name> FROM <feed> inside
// this Domain — d.CreateReaderFeed("audit", "orders") declares
// shop.audit consuming shop.orders. The feed argument is a ScopedRef:
// Rel("orders") resolves in-domain while a *Feed handle or
// Abs("other.events") binds a Reader across Domains.
func (d *Domain) CreateReaderFeed(
	ctx context.Context, name string, feed ScopedRef, start ReaderStart,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	target, err := feed.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.createReader(ctx, full, target, start)
}

// CreateWriterFeed executes CREATE WRITER <name> TO <feed> inside this
// Domain — same scoping as CreateReaderFeed.
func (d *Domain) CreateWriterFeed(
	ctx context.Context, name string, feed ScopedRef,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	target, err := feed.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.createWriter(ctx, full, target)
}

// CreateReader starts a fluent Reader declaration on relative inside
// this Domain — d.CreateReader("orders") binds shop.orders:
//
//	reader, err := shop.CreateReader("orders")
//	reader, err = reader.ReaderID("fulfillment").   // the Kafka groupId analogue
//		StartPoint(StartBeginning()).
//		Open(ctx)
//
// Typed feeds use NewReaderBuilder(client, domain, feed) directly.
func (d *Domain) CreateReader(relative string) (*ReaderBuilder[string, string], error) {
	feed, err := d.Feed(relative)
	if err != nil {
		return nil, err
	}
	return NewReaderBuilder(d.client, d, feed), nil
}

// CreateSubscription executes CREATE SUBSCRIPTION <name> FROM <feed>
// inside this Domain — the Subscription and its progress are owned by
// this Domain; the source Feed may be in another via a *Feed handle or
// Abs(...) — e.g. warehouse.CreateSubscription(ctx, "fulfil",
// purchasing.Feed("orders"), StartBeginning()).
func (d *Domain) CreateSubscription(
	ctx context.Context, name string, feed ScopedRef, start ReaderStart,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	source, err := feed.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.createSubscription(ctx, full, source, start)
}

// SubscriptionRef returns a scoped reference to a Subscription inside
// this Domain — for use as a Pipe's input in another declaration or
// for member access through MemberClient.
func (d *Domain) SubscriptionRef(relative string) (SubscriptionRef, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return SubscriptionRef{}, err
	}
	return SubscriptionRef{name: full}, nil
}

// Writer binds a named-Writer session inside this Domain — epoch from
// OpenWriterSession. Raw session client; prefer Feed.Writer or
// WriterBuilder for the typed route.
func (d *Domain) Writer(name string, sessionEpoch int64) (*WriterSessionClient, error) {
	full, err := d.resolve(name)
	if err != nil {
		return nil, err
	}
	return d.client.writer(full, sessionEpoch), nil
}

// Reader binds a named-Reader session inside this Domain —
// open/fetch/ack/close with explicit session state.
func (d *Domain) Reader(name string) (*ReaderSessionClient, error) {
	full, err := d.resolve(name)
	if err != nil {
		return nil, err
	}
	return d.client.reader(full), nil
}

// MemberClient binds the raw subscription-member client inside this
// Domain — join/claim/renew/release/fetch/ack under fenced leases.
func (d *Domain) MemberClient(subscription string) (*SubscriptionMemberClient, error) {
	full, err := d.resolve(subscription)
	if err != nil {
		return nil, err
	}
	return d.client.subscriptionMember(full), nil
}

// Subscribe opens a durable subscription inside this Domain: declares
// the Reader when missing, then runs open → fetch → deliver → ack on a
// background goroutine. The feed may point at another Domain through a
// *Feed handle or Abs(...).
func (d *Domain) Subscribe(
	ctx context.Context,
	reader string,
	feed ScopedRef,
	start ReaderStart,
	onRecords func([]Record) error,
	onError func(error),
) (*Subscription, error) {
	full, err := d.resolve(reader)
	if err != nil {
		return nil, err
	}
	source, err := feed.scopedName(d)
	if err != nil {
		return nil, err
	}
	return d.client.subscribe(ctx, full, source, start,
		subscribeCapacity, onRecords, onError), nil
}

// OpenWriterSession executes OPEN WRITER SESSION <name> inside this
// Domain — the epoch comes back in the execution's data.
func (d *Domain) OpenWriterSession(ctx context.Context, writer string) (ControlExecution, error) {
	full, err := d.resolve(writer)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.openWriterSession(ctx, full)
}

// SeekReader executes SEEK READER <name> TO <start> inside this Domain.
func (d *Domain) SeekReader(ctx context.Context, reader string, start ReaderStart) (ControlExecution, error) {
	full, err := d.resolve(reader)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.seekReader(ctx, full, start)
}

// DescribeFeed executes DESCRIBE FEED <name> inside this Domain.
func (d *Domain) DescribeFeed(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.describeFeed(ctx, full)
}

// DescribeWriter executes DESCRIBE WRITER <name> inside this Domain.
func (d *Domain) DescribeWriter(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.describeWriter(ctx, full)
}

// DescribeReader executes DESCRIBE READER <name> inside this Domain.
func (d *Domain) DescribeReader(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.describeReader(ctx, full)
}

// DescribeSubscription executes DESCRIBE SUBSCRIPTION <name> inside
// this Domain.
func (d *Domain) DescribeSubscription(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.describeSubscription(ctx, full)
}

// RenameFeed executes RENAME FEED <current> TO <renamed> — both names
// resolve inside this Domain.
func (d *Domain) RenameFeed(ctx context.Context, current, renamed string) (ControlExecution, error) {
	cur, err := d.resolve(current)
	if err != nil {
		return ControlExecution{}, err
	}
	nxt, err := d.resolve(renamed)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.renameFeed(ctx, cur, nxt)
}

// RenameWriter executes RENAME WRITER <current> TO <renamed> in-domain.
func (d *Domain) RenameWriter(ctx context.Context, current, renamed string) (ControlExecution, error) {
	cur, err := d.resolve(current)
	if err != nil {
		return ControlExecution{}, err
	}
	nxt, err := d.resolve(renamed)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.renameWriter(ctx, cur, nxt)
}

// RenameReader executes RENAME READER <current> TO <renamed> in-domain.
func (d *Domain) RenameReader(ctx context.Context, current, renamed string) (ControlExecution, error) {
	cur, err := d.resolve(current)
	if err != nil {
		return ControlExecution{}, err
	}
	nxt, err := d.resolve(renamed)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.renameReader(ctx, cur, nxt)
}

// RenameSubscription executes RENAME SUBSCRIPTION <current> TO
// <renamed> in-domain.
func (d *Domain) RenameSubscription(ctx context.Context, current, renamed string) (ControlExecution, error) {
	cur, err := d.resolve(current)
	if err != nil {
		return ControlExecution{}, err
	}
	nxt, err := d.resolve(renamed)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.renameSubscription(ctx, cur, nxt)
}

// DropFeed executes DROP FEED <name> inside this Domain.
func (d *Domain) DropFeed(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.dropFeed(ctx, full)
}

// DropWriter executes DROP WRITER <name> inside this Domain.
func (d *Domain) DropWriter(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.dropWriter(ctx, full)
}

// DropReader executes DROP READER <name> inside this Domain.
func (d *Domain) DropReader(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.dropReader(ctx, full)
}

// DropSubscription executes DROP SUBSCRIPTION <name> inside this Domain.
func (d *Domain) DropSubscription(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.dropSubscription(ctx, full)
}

// SetFeedSchema executes SET SCHEMA ON FEED <feed> inside this Domain.
func (d *Domain) SetFeedSchema(ctx context.Context, relative string, schema *FeedSchema) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.setFeedSchema(ctx, full, schema)
}

// ClearFeedSchema executes DROP SCHEMA ON FEED <feed> inside this
// Domain.
func (d *Domain) ClearFeedSchema(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.clearFeedSchema(ctx, full)
}

// CreateWriter starts a fluent Writer declaration on relative inside
// this Domain — d.CreateWriter("orders") binds shop.orders. Typed
// feeds use NewWriterBuilder(client, domain, feed) directly.
func (d *Domain) CreateWriter(relative string) (*WriterBuilder[string, string], error) {
	feed, err := d.Feed(relative)
	if err != nil {
		return nil, err
	}
	return NewWriterBuilder(d.client, d, feed), nil
}

// Feed binds relative inside this Domain — d.Feed("orders") resolves
// shop.orders. An already-qualified name ("shop.orders") is accepted
// as-is. Pure name composition — no I/O.
func (d *Domain) Feed(relative string) (*Feed[string, string], error) {
	return FeedOfDomain(d, relative, StringCodec(), StringCodec())
}

// FeedForDomain binds a Feed inside domain with a typed payload Codec —
// the Domain-first typed binding.
func FeedForDomain[V any](domain *Domain, relative string) (*Feed[string, V], error) {
	return FeedOfDomain(domain, relative, StringCodec(), CodecFor[V]())
}

// FeedOfDomain binds a Feed inside domain under explicit codecs —
// the Domain-first explicit-codec binding.
func FeedOfDomain[K comparable, V any](
	domain *Domain, relative string, keyCodec Codec[K], valueCodec Codec[V],
) (*Feed[K, V], error) {
	full, err := domain.resolve(relative)
	if err != nil {
		return nil, err
	}
	return &Feed[K, V]{
		client: domain.client, feedClient: domain.client.feeds,
		name: full, keyCodec: keyCodec, valueCodec: valueCodec,
	}, nil
}

// ScopedRef names a resource endpoint for a Pipe declaration, a
// Subscription's source Feed, or a StateStore's source — the
// cross-Domain vocabulary. Implementations:
//
//	Rel("orders")              — resolves inside the declaring Domain
//	Abs("other.events")        — keeps a fully-qualified name
//	*Feed[K,V], *StateStore, *Store[K,V], SubscriptionRef — carry
//	                             their absolute names
//
// The Domain owns the declaration either way — only the endpoint moves.
type ScopedRef interface {
	scopedName(d *Domain) (string, error)
}

// Scoped is the explicit-reference form of ScopedRef — Rel for
// in-Domain names, Abs for qualified cross-Domain names.
type Scoped struct {
	name     string
	absolute bool
}

// Rel names a resource inside the declaring Domain — the readable
// default for every endpoint.
func Rel(name string) Scoped { return Scoped{name: name} }

// Abs names a resource by its full Domain-qualified name — the
// deliberate cross-Domain edge.
func Abs(name string) Scoped { return Scoped{name: name, absolute: true} }

// scopedName resolves this Scoped — Rel composes inside d, Abs keeps
// its qualified name.
func (s Scoped) scopedName(d *Domain) (string, error) {
	if s.absolute {
		return dottedName(s.name)
	}
	return d.resolve(s.name)
}

// Scoped resolves a ScopedRef to its wire name inside this Domain.
func (d *Domain) Scoped(ref ScopedRef) (string, error) {
	return ref.scopedName(d)
}

// SubscriptionRef carries a Subscription's absolute name — a Pipe
// input, a member-lease target, or a cross-Domain edge.
type SubscriptionRef struct {
	client *Client
	name   string
}

// Name is the Subscription's absolute Domain-qualified name.
func (r SubscriptionRef) Name() string { return r.name }

// scopedName returns the carried qualified name.
func (r SubscriptionRef) scopedName(d *Domain) (string, error) { return r.name, nil }

// MemberClient binds the raw member client for this Subscription.
func (r SubscriptionRef) MemberClient() *SubscriptionMemberClient {
	return r.client.subscriptionMember(r.name)
}

// Feed implements ScopedRef — its name is already Domain-qualified.
func (f *Feed[K, V]) scopedName(d *Domain) (string, error) { return f.name, nil }

// StateStore implements ScopedRef — its name is already Domain-qualified.
func (s *StateStore) scopedName(d *Domain) (string, error) { return s.name, nil }

// Store implements ScopedRef — its name is already Domain-qualified.
func (s *Store[K, V]) scopedName(d *Domain) (string, error) {
	return s.store.name, nil
}

// resolve maps a relative name to its full dotted Feed name inside
// this Domain — "orders" -> "shop.orders".
func (d *Domain) resolve(relative string) (string, error) {
	if relative == "" {
		return "", fmt.Errorf("feed name is required")
	}
	full := relative
	if !strings.HasPrefix(relative, d.name+".") {
		full = d.name + "." + relative
	}
	return dottedName(full)
}

// relative strips this Domain's prefix — shop.orders -> orders.
func (d *Domain) relative(full string) string {
	return strings.TrimPrefix(full, d.name+".")
}

func (d *Domain) snapshot(ctx context.Context) (*domainSnapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.snap == nil {
		snap, err := d.load(ctx)
		if err != nil {
			return nil, err
		}
		d.snap = snap
	}
	return d.snap, nil
}

// load pulls the whole Domain view in ONE batched control read:
// DESCRIBE DOMAIN plus every SHOW listing. Rows join on space_id —
// readers and writers join through their Feed's feed_id.
func (d *Domain) load(ctx context.Context) (*domainSnapshot, error) {
	execution, err := d.client.Commands(ctx, []map[string]any{
		{"command": "describe", "kind": "domain", "name": d.name},
		{"command": "show", "kind": "feeds"},
		{"command": "show", "kind": "readers"},
		{"command": "show", "kind": "writers"},
		{"command": "show", "kind": "subscriptions"},
		{"command": "show", "kind": "pipes"},
		{"command": "show", "kind": "indexes"},
		{"command": "show", "kind": "state_stores"},
	})
	if err != nil {
		return nil, err
	}
	if len(execution.Results) < 8 {
		return nil, fmt.Errorf("domain snapshot returned %d results, want 8",
			len(execution.Results))
	}

	var describe struct {
		SpaceID string          `json:"space_id"`
		Schema  json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(execution.Results[0].Data, &describe); err != nil {
		return nil, fmt.Errorf("decode domain describe: %w", err)
	}
	if describe.SpaceID == "" {
		return nil, fmt.Errorf("domain %q is not declared", d.name)
	}

	var feedRows []struct {
		Name            string         `json:"name"`
		FeedID          string         `json:"feed_id"`
		SpaceID         string         `json:"space_id"`
		Schema          map[string]any `json:"schema"`
		EffectiveSchema map[string]any `json:"effective_schema"`
	}
	if err := json.Unmarshal(execution.Results[1].Data, &feedRows); err != nil {
		return nil, fmt.Errorf("decode feeds listing: %w", err)
	}
	snap := &domainSnapshot{
		domainID:     describe.SpaceID,
		describe:     execution.Results[0].Data,
		revision:     execution.Revision,
		feedIDs:      map[string]bool{},
		feedSchemas:  map[string]*FeedSchema{},
		declaredFeed: map[string]*FeedSchema{},
	}
	var domainSchema map[string]any
	if len(describe.Schema) > 0 && string(describe.Schema) != "null" {
		_ = json.Unmarshal(describe.Schema, &domainSchema)
	}
	snap.schema = feedSchemaFrom(domainSchema)
	for _, row := range feedRows {
		if row.SpaceID == snap.domainID {
			snap.feeds = append(snap.feeds, row.Name)
			snap.feedIDs[row.FeedID] = true
			snap.feedSchemas[row.Name] = feedSchemaFrom(row.EffectiveSchema)
			snap.declaredFeed[row.Name] = feedSchemaFrom(row.Schema)
		}
	}
	sort.Strings(snap.feeds)

	scoped := func(index int, field string, inScope func(string) bool) ([]string, error) {
		var rows []map[string]any
		if err := json.Unmarshal(execution.Results[index].Data, &rows); err != nil {
			return nil, fmt.Errorf("decode listing %d: %w", index, err)
		}
		var names []string
		for _, row := range rows {
			scope, _ := row[field].(string)
			name, _ := row["name"].(string)
			if scope != "" && name != "" && inScope(scope) {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		return names, nil
	}
	inFeed := func(id string) bool { return snap.feedIDs[id] }
	inDomain := func(id string) bool { return id == snap.domainID }

	if snap.readers, err = scoped(2, "feed_id", inFeed); err != nil {
		return nil, err
	}
	if snap.writers, err = scoped(3, "feed_id", inFeed); err != nil {
		return nil, err
	}
	if snap.subscriptions, err = scoped(4, "space_id", inDomain); err != nil {
		return nil, err
	}
	if snap.pipes, err = scoped(5, "space_id", inDomain); err != nil {
		return nil, err
	}
	if snap.indexes, err = scoped(6, "space_id", inDomain); err != nil {
		return nil, err
	}
	if snap.stateStores, err = scoped(7, "space_id", inDomain); err != nil {
		return nil, err
	}
	return snap, nil
}

// FeedOps returns the raw FeedClient — the byte-level escape hatch for
// committed reads, anonymous temporary fetches, and ad-hoc appends.
func (d *Domain) FeedOps() *FeedClient { return d.client.feeds }

// Pipe returns the lifecycle handle for a Pipe inside this Domain —
// Describe/Pause/Resume/Drop.
func (d *Domain) Pipe(relative string) (*Pipe, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return nil, err
	}
	return d.client.pipe(full), nil
}

// PipeInfos lists declared Pipes inside this Domain.
func (d *Domain) PipeInfos(ctx context.Context) ([]PipeInfo, error) {
	all, err := d.client.pipes(ctx)
	if err != nil {
		return nil, err
	}
	prefix := d.name + "."
	var out []PipeInfo
	for _, info := range all {
		if strings.HasPrefix(info.Name, prefix) {
			out = append(out, info)
		}
	}
	return out, nil
}

// DefineForwardPipe declares a copy Pipe inside this Domain — every
// endpoint is a ScopedRef: Rel("...") resolves in-domain, while a
// *Feed/SubscriptionRef handle or Abs(...) points across Domains. The
// input Subscription must belong to this Domain (it is the Pipe's own
// durable frontier); the output Feed may live anywhere.
func (d *Domain) DefineForwardPipe(
	ctx context.Context, name string, subscription, outputFeed ScopedRef,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	sub, err := subscription.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	out, err := outputFeed.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.defineForwardPipe(ctx, full, sub, out)
}

// DefineEnrichPipe declares point-lookup enrichment inside this
// Domain — every endpoint follows the ScopedRef rule.
func (d *Domain) DefineEnrichPipe(
	ctx context.Context,
	name string,
	subscription, outputFeed, store ScopedRef,
	op PipeOperation,
	quarantineFeed ScopedRef,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	sub, err := subscription.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	out, err := outputFeed.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	lookup, err := store.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	quarantine := ""
	if quarantineFeed != nil {
		if quarantine, err = quarantineFeed.scopedName(d); err != nil {
			return ControlExecution{}, err
		}
	}
	return d.client.defineEnrichPipe(ctx, full, sub, out, lookup, op, quarantine)
}

// DefineWindowPipe declares rolling-window aggregation inside this
// Domain — every endpoint follows the ScopedRef rule.
func (d *Domain) DefineWindowPipe(
	ctx context.Context,
	name string,
	subscription, outputStore ScopedRef,
	op PipeOperation,
	emitFeed ScopedRef,
) (ControlExecution, error) {
	full, err := d.resolve(name)
	if err != nil {
		return ControlExecution{}, err
	}
	sub, err := subscription.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	view, err := outputStore.scopedName(d)
	if err != nil {
		return ControlExecution{}, err
	}
	emit := ""
	if emitFeed != nil {
		if emit, err = emitFeed.scopedName(d); err != nil {
			return ControlExecution{}, err
		}
	}
	return d.client.defineWindowPipe(ctx, full, sub, view, op, emit)
}

// StateStore binds a StateStore spec/facade inside this Domain —
// d.StateStore("profiles") composes shop.profiles. Pass the result to
// DefineStateStore to declare it, or use it for row operations.
func (d *Domain) StateStore(relative string) (*StateStore, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return nil, err
	}
	return d.client.stateStore(full)
}

// Store binds a typed Store facade inside this Domain under explicit
// codecs.
func (d *Domain) Store(relative string) (*Store[string, []byte], error) {
	full, err := d.resolve(relative)
	if err != nil {
		return nil, err
	}
	return d.client.store(full)
}

// StoreForDomain binds a Store inside domain with string keys and a
// CodecFor[V] payload — the typed companion of Store.
func StoreForDomain[V any](domain *Domain, relative string) (*Store[string, V], error) {
	bound, err := domain.StateStore(relative)
	if err != nil {
		return nil, err
	}
	return StoreOfBound[string, V](bound, StringCodec(), CodecFor[V]()), nil
}

// StoreOfDomain binds a Store inside domain under explicit codecs.
func StoreOfDomain[K comparable, V any](
	domain *Domain, relative string, keyCodec Codec[K], valueCodec Codec[V],
) (*Store[K, V], error) {
	bound, err := domain.StateStore(relative)
	if err != nil {
		return nil, err
	}
	return StoreOfBound[K, V](bound, keyCodec, valueCodec), nil
}

// DefineStateStore declares spec inside this Domain — the spec's name
// resolves in-domain; source names the Feed it materializes from
// (nil = manual), and may point at another Domain through a *Feed
// handle or Abs(...). Returns the bound store facade.
func (d *Domain) DefineStateStore(
	ctx context.Context, spec *StateStore, source ScopedRef,
) (*StateStore, error) {
	if spec == nil {
		return nil, fmt.Errorf("define takes a StateStore spec — use Domain.StateStore(name)")
	}
	full, err := d.resolve(spec.name)
	if err != nil {
		return nil, err
	}
	scopedSpec := *spec
	scopedSpec.name = full
	var src string
	if source != nil {
		if src, err = source.scopedName(d); err != nil {
			return nil, err
		}
	}
	return d.client.stateStores.Define(ctx, &scopedSpec, src)
}

// DropStateStore executes DROP STATESTORE <name> inside this Domain.
func (d *Domain) DropStateStore(ctx context.Context, relative string) (ControlExecution, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return ControlExecution{}, err
	}
	return d.client.stateStores.Drop(ctx, full)
}

// QueryIndex executes an index query inside this Domain — the index
// name resolves in-domain.
func (d *Domain) QueryIndex(
	ctx context.Context, relative string, predicate IndexPredicate, limit int, strict bool,
) (IndexPage, error) {
	full, err := d.resolve(relative)
	if err != nil {
		return IndexPage{}, err
	}
	return d.client.queryIndex(ctx, full, predicate, limit, strict)
}

// requireOwnedFeed verifies feed lives under the Domain's prefix —
// called by the fluent Reader/Writer builders so a feed bound to
// another Domain fails locally.
func (d *Domain) requireOwnedFeed(feed string) error {
	if !strings.HasPrefix(feed, d.name+".") {
		return fmt.Errorf("feed %q is outside Domain %q", feed, d.name)
	}
	return nil
}

// ReaderBuilder is the fluent declaration + session-open of a durable
// named Reader — the typed companion of Domain navigation:
//
//	reader, err := whitewater.NewReaderBuilder(client, shop, orders).
//	    ReaderID("fulfillment").          // the Kafka groupId analogue
//	    StartPoint(whitewater.StartBeginning()).
//	    MaxPageSize(256).
//	    Open(ctx)                          // *FeedReader[K,V]
type ReaderBuilder[K comparable, V any] struct {
	client      *Client
	domain      *Domain
	feed        *Feed[K, V]
	readerID    string
	start       ReaderStart
	maxPageSize int
}

// NewReaderBuilder starts a fluent Reader declaration on feed inside
// domain — feed must live under the Domain's prefix.
func NewReaderBuilder[K comparable, V any](
	client *Client, domain *Domain, feed *Feed[K, V],
) *ReaderBuilder[K, V] {
	return &ReaderBuilder[K, V]{
		client: client, domain: domain, feed: feed,
		start: StartBeginning(), maxPageSize: 128,
	}
}

// ReaderID sets the durable Reader identity — the Kafka groupId
// analogue. Required before Open/Declare.
func (b *ReaderBuilder[K, V]) ReaderID(id string) *ReaderBuilder[K, V] {
	b.readerID = id
	return b
}

// StartPoint sets the first-time start position — applies only when
// the Reader is first declared; an existing Reader resumes its
// acknowledged frontier (the auto.offset.reset mapping).
func (b *ReaderBuilder[K, V]) StartPoint(start ReaderStart) *ReaderBuilder[K, V] {
	b.start = start
	return b
}

// MaxPageSize bounds the records delivered per fetch page — the
// default is 128.
func (b *ReaderBuilder[K, V]) MaxPageSize(maxPageSize int) *ReaderBuilder[K, V] {
	b.maxPageSize = maxPageSize
	return b
}

// Declare creates the catalog Reader without opening a session —
// fails when the name is already declared.
func (b *ReaderBuilder[K, V]) Declare(ctx context.Context) (ControlExecution, error) {
	id, err := b.requireID()
	if err != nil {
		return ControlExecution{}, err
	}
	if err := b.domain.requireOwnedFeed(b.feed.Name()); err != nil {
		return ControlExecution{}, err
	}
	return b.client.createReader(ctx, id, b.feed.Name(), b.start)
}

// Open declares the Reader when missing (an existing declaration
// keeps its start and durable progress), opens a session, and binds
// the Feed's codecs — Fetch returns decoded FeedMessages.
func (b *ReaderBuilder[K, V]) Open(ctx context.Context) (*FeedReader[K, V], error) {
	id, err := b.requireID()
	if err != nil {
		return nil, err
	}
	if err := b.domain.requireOwnedFeed(b.feed.Name()); err != nil {
		return nil, err
	}
	if err := b.client.ensureReader(ctx, id, b.feed.Name(), b.start); err != nil {
		return nil, err
	}
	sessionClient := b.client.reader(id)
	session, err := sessionClient.Open(ctx, b.maxPageSize)
	if err != nil {
		return nil, err
	}
	return &FeedReader[K, V]{
		client: sessionClient, session: session,
		keyCodec: b.feed.keyCodec, valueCodec: b.feed.valueCodec,
	}, nil
}

func (b *ReaderBuilder[K, V]) requireID() (string, error) {
	if b.readerID == "" {
		return "", fmt.Errorf("ReaderID(...) is required — the durable consume identity" +
			" (the Kafka groupId analogue)")
	}
	// Reader identities are Domain resources — "fulfillment" inside
	// "shop" declares shop.fulfillment, never a global name.
	return b.domain.resolve(b.readerID)
}

// WriterBuilder is the fluent declaration + session-open of a durable
// named Writer:
//
//	writer, err := whitewater.NewWriterBuilder(client, shop, orders).
//	    WriterID("orders.ingest").
//	    Open(ctx)                          // *FeedWriter[K,V]
type WriterBuilder[K comparable, V any] struct {
	client   *Client
	domain   *Domain
	feed     *Feed[K, V]
	writerID string
}

// NewWriterBuilder starts a fluent Writer declaration on feed inside
// domain — feed must live under the Domain's prefix.
func NewWriterBuilder[K comparable, V any](
	client *Client, domain *Domain, feed *Feed[K, V],
) *WriterBuilder[K, V] {
	return &WriterBuilder[K, V]{client: client, domain: domain, feed: feed}
}

// WriterID sets the durable Writer identity. Required before
// Open/Declare.
func (b *WriterBuilder[K, V]) WriterID(id string) *WriterBuilder[K, V] {
	b.writerID = id
	return b
}

// Declare creates the catalog Writer without opening a session —
// fails when the name is already declared.
func (b *WriterBuilder[K, V]) Declare(ctx context.Context) (ControlExecution, error) {
	id, err := b.requireID()
	if err != nil {
		return ControlExecution{}, err
	}
	if err := b.domain.requireOwnedFeed(b.feed.Name()); err != nil {
		return ControlExecution{}, err
	}
	return b.client.createWriter(ctx, id, b.feed.Name())
}

// Open declares the Writer when missing, opens a session (the fencing
// epoch stays inside the handle), and binds the Feed's codecs.
func (b *WriterBuilder[K, V]) Open(ctx context.Context) (*FeedWriter[K, V], error) {
	id, err := b.requireID()
	if err != nil {
		return nil, err
	}
	if err := b.domain.requireOwnedFeed(b.feed.Name()); err != nil {
		return nil, err
	}
	if _, err := b.client.createWriter(ctx, id, b.feed.Name()); err != nil {
		if !strings.Contains(err.Error(), "exists") {
			return nil, err
		}
	}
	epoch, err := b.feed.openWriterSession(ctx, id)
	if err != nil {
		return nil, err
	}
	return &FeedWriter[K, V]{
		session:  b.client.writer(id, epoch),
		keyCodec: b.feed.keyCodec, valueCodec: b.feed.valueCodec,
	}, nil
}

func (b *WriterBuilder[K, V]) requireID() (string, error) {
	if b.writerID == "" {
		return "", fmt.Errorf("WriterID(...) is required")
	}
	// Writer identities are Domain resources — same rule as ReaderID.
	return b.domain.resolve(b.writerID)
}
