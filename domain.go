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

// Feed binds relative inside this Domain — d.Feed("orders") resolves
// shop.orders. An already-qualified name ("shop.orders") is accepted
// as-is. Pure name composition — no I/O.
func (d *Domain) Feed(relative string) (*Feed[string, []byte], error) {
	return FeedOfDomain(d, relative, StringCodec(), BytesCodec())
}

// FeedForDomain binds a Feed inside domain with a typed payload Codec —
// the Domain companion of FeedFor.
func FeedForDomain[V any](domain *Domain, relative string) (*Feed[string, V], error) {
	return FeedOfDomain(domain, relative, StringCodec(), CodecFor[V]())
}

// FeedOfDomain binds a Feed inside domain under explicit codecs —
// the Domain companion of FeedOf.
func FeedOfDomain[K comparable, V any](
	domain *Domain, relative string, keyCodec Codec[K], valueCodec Codec[V],
) (*Feed[K, V], error) {
	full, err := domain.resolve(relative)
	if err != nil {
		return nil, err
	}
	return &Feed[K, V]{
		client: domain.client, feedClient: domain.client.Feeds,
		name: full, keyCodec: keyCodec, valueCodec: valueCodec,
	}, nil
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
//	    Capacity(256).
//	    Open(ctx)                          // *FeedReader[K,V]
type ReaderBuilder[K comparable, V any] struct {
	client   *Client
	domain   *Domain
	feed     *Feed[K, V]
	readerID string
	start    ReaderStart
	capacity int
}

// NewReaderBuilder starts a fluent Reader declaration on feed inside
// domain — feed must live under the Domain's prefix.
func NewReaderBuilder[K comparable, V any](
	client *Client, domain *Domain, feed *Feed[K, V],
) *ReaderBuilder[K, V] {
	return &ReaderBuilder[K, V]{
		client: client, domain: domain, feed: feed,
		start: StartBeginning(), capacity: 128,
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

// Capacity bounds each fetch page — the default is 128.
func (b *ReaderBuilder[K, V]) Capacity(capacity int) *ReaderBuilder[K, V] {
	b.capacity = capacity
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
	return b.client.CreateReader(ctx, id, b.feed.Name(), b.start)
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
	sessionClient := b.client.Reader(id)
	session, err := sessionClient.Open(ctx, b.capacity)
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
	return b.readerID, nil
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
	return b.client.CreateWriter(ctx, id, b.feed.Name())
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
	if _, err := b.client.CreateWriter(ctx, id, b.feed.Name()); err != nil {
		if !strings.Contains(err.Error(), "exists") {
			return nil, err
		}
	}
	epoch, err := b.feed.openWriterSession(ctx, id)
	if err != nil {
		return nil, err
	}
	return &FeedWriter[K, V]{
		session:  b.client.Writer(id, epoch),
		keyCodec: b.feed.keyCodec, valueCodec: b.feed.valueCodec,
	}, nil
}

func (b *WriterBuilder[K, V]) requireID() (string, error) {
	if b.writerID == "" {
		return "", fmt.Errorf("WriterID(...) is required")
	}
	return b.writerID, nil
}
