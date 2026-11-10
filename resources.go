package whitewater

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// ReaderStartKind identifies a start-position flavor.
type ReaderStartKind int

const (
	StartKindBeginning ReaderStartKind = iota
	StartKindNow
	StartKindCursor
	StartKindTimestamp
)

// ReaderStart is a Reader/Subscription start position — the WCL
// START AT operand. Build with the Start* constructors.
type ReaderStart struct {
	// Kind discriminates the start flavor.
	Kind ReaderStartKind
	// Cursor is set for StartKindCursor.
	Cursor *Cursor
	// TimestampNs is set for StartKindTimestamp.
	TimestampNs int64
}

// WCL renders the START AT operand, e.g. BEGINNING.
func (s ReaderStart) WCL() string {
	switch s.Kind {
	case StartKindNow:
		return "NOW"
	case StartKindCursor:
		if s.Cursor == nil {
			return "CURSOR ''"
		}
		return "CURSOR '" + strings.ReplaceAll(string(*s.Cursor), "'", "''") + "'"
	case StartKindTimestamp:
		return fmt.Sprintf("TIMESTAMP %d", s.TimestampNs)
	default:
		return "BEGINNING"
	}
}

// StartBeginning reads from the oldest retained record.
func StartBeginning() ReaderStart { return ReaderStart{Kind: StartKindBeginning} }

// StartNow reads from records appended after creation.
func StartNow() ReaderStart { return ReaderStart{Kind: StartKindNow} }

// StartCursor resumes from an opaque Cursor previously returned by
// the Riverbed.
func StartCursor(cursor Cursor) ReaderStart {
	return ReaderStart{Kind: StartKindCursor, Cursor: &cursor}
}

// StartTimestampNs reads from a signed Unix epoch nanosecond timestamp.
func StartTimestampNs(nanos int64) ReaderStart {
	return ReaderStart{Kind: StartKindTimestamp, TimestampNs: nanos}
}

var (
	dottedNameRe = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`)
)

func dottedName(name string) (string, error) {
	if len(name) > 512 || !dottedNameRe.MatchString(name) {
		return "", fmt.Errorf("invalid dotted name: %q", name)
	}
	return name, nil
}

// Resource names (writers, readers, subscriptions, roles) share the same
// dotted grammar the server validates for every resource kind.
func resourceName(name string) (string, error) {
	return dottedName(name)
}

// ---- Typed resource management ---------------------------------------------
// Convenience over WCL: each method builds one statement and executes it.
// Names are validated before interpolation, so a bad value fails locally
// rather than reaching the Control Plane.

// CreateDomain executes CREATE DOMAIN <name> and returns the Domain
// handle — the front door every Feed, Writer, Reader, Subscription,
// Pipe, and StateStore hangs from.
func (c *Client) CreateDomain(ctx context.Context, name string) (*Domain, error) {
	n, err := dottedName(name)
	if err != nil {
		return nil, err
	}
	if _, err := c.WCL(ctx, "CREATE DOMAIN "+n); err != nil {
		return nil, err
	}
	return &Domain{client: c, name: n}, nil
}

// SetDomainSchema executes SET SCHEMA ON DOMAIN <domain> — declares
// the record schema metadata every Feed in the Domain inherits.
func (c *Client) SetDomainSchema(
	ctx context.Context, domain string, schema *FeedSchema,
) (ControlExecution, error) {
	return c.setSchema(ctx, "domain", domain, schema)
}

// setFeedSchema executes SET SCHEMA ON FEED <feed> — overrides the
// Domain's declared schema for the named sides only. The app-facing
// route is Domain.SetFeedSchema, which resolves the name in-domain.
func (c *Client) setFeedSchema(
	ctx context.Context, feed string, schema *FeedSchema,
) (ControlExecution, error) {
	return c.setSchema(ctx, "feed", feed, schema)
}

// ClearDomainSchema executes DROP SCHEMA ON DOMAIN <domain>.
func (c *Client) ClearDomainSchema(ctx context.Context, domain string) (ControlExecution, error) {
	return c.clearSchema(ctx, "domain", domain)
}

// clearFeedSchema executes DROP SCHEMA ON FEED <feed> — the Feed
// reverts to inheriting its Domain's declared schema. The app-facing
// route is Domain.ClearFeedSchema.
func (c *Client) clearFeedSchema(ctx context.Context, feed string) (ControlExecution, error) {
	return c.clearSchema(ctx, "feed", feed)
}

func (c *Client) setSchema(
	ctx context.Context, kind, name string, schema *FeedSchema,
) (ControlExecution, error) {
	dotted, err := dottedName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	if schema == nil {
		return ControlExecution{}, fmt.Errorf("schema is required")
	}
	return c.Commands(ctx, []map[string]any{{
		"command": "set_schema",
		"scope":   map[string]any{"kind": kind, "name": dotted},
		"schema":  schema.wire(),
	}})
}

func (c *Client) clearSchema(ctx context.Context, kind, name string) (ControlExecution, error) {
	dotted, err := dottedName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.Commands(ctx, []map[string]any{{
		"command": "clear_schema",
		"scope":   map[string]any{"kind": kind, "name": dotted},
	}})
}

// createFeed executes CREATE FEED <dotted.name> — internal; the public
// route is Domain.CreateFeed, which resolves the name in-domain.
func (c *Client) createFeed(ctx context.Context, name string) (ControlExecution, error) {
	n, err := dottedName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "CREATE FEED "+n)
}

// createWriter executes CREATE WRITER <name> TO <feed> — internal;
// the public route is Domain.CreateWriter / Domain.Writer.
func (c *Client) createWriter(ctx context.Context, name, feed string) (ControlExecution, error) {
	n, err := resourceName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	f, err := dottedName(feed)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "CREATE WRITER "+n+" TO "+f)
}

// createReader executes CREATE READER <name> FROM <feed> START AT
// <start> — internal; the public route is Domain.CreateReader.
func (c *Client) createReader(
	ctx context.Context, name, feed string, start ReaderStart,
) (ControlExecution, error) {
	return c.createFrom(ctx, "READER", name, feed, start)
}

// createSubscription executes CREATE SUBSCRIPTION <name> FROM <feed>
// START AT <start> — internal; the public route is
// Domain.CreateSubscription.
func (c *Client) createSubscription(
	ctx context.Context, name, feed string, start ReaderStart,
) (ControlExecution, error) {
	return c.createFrom(ctx, "SUBSCRIPTION", name, feed, start)
}

func (c *Client) createFrom(
	ctx context.Context, kind, name, feed string, start ReaderStart,
) (ControlExecution, error) {
	n, err := resourceName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	f, err := dottedName(feed)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "CREATE "+kind+" "+n+" FROM "+f+" START AT "+start.WCL())
}

// CreateRole executes CREATE ROLE <name>.
func (c *Client) CreateRole(ctx context.Context, name string) (ControlExecution, error) {
	n, err := resourceName(name)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "CREATE ROLE "+n)
}

// OpenWriterSession executes OPEN WRITER SESSION <name>; the returned
// epoch feeds Client.Writer.
func (c *Client) openWriterSession(ctx context.Context, writer string) (ControlExecution, error) {
	n, err := resourceName(writer)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "OPEN WRITER SESSION "+n)
}

// SeekReader executes SEEK READER <name> TO <start>, repositioning a
// durable Reader.
func (c *Client) seekReader(
	ctx context.Context, reader string, start ReaderStart,
) (ControlExecution, error) {
	n, err := resourceName(reader)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "SEEK READER "+n+" TO "+start.WCL())
}

func (c *Client) describe(ctx context.Context, kind, name string, dotted bool) (ControlExecution, error) {
	n, err := c.nameFor(name, dotted)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "DESCRIBE "+kind+" "+n)
}

func (c *Client) rename(ctx context.Context, kind, current, renamed string, dotted bool) (ControlExecution, error) {
	cur, err := c.nameFor(current, dotted)
	if err != nil {
		return ControlExecution{}, err
	}
	next, err := c.nameFor(renamed, dotted)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "RENAME "+kind+" "+cur+" TO "+next)
}

func (c *Client) drop(ctx context.Context, kind, name string, dotted bool) (ControlExecution, error) {
	n, err := c.nameFor(name, dotted)
	if err != nil {
		return ControlExecution{}, err
	}
	return c.WCL(ctx, "DROP "+kind+" "+n)
}

func (c *Client) nameFor(name string, dotted bool) (string, error) {
	if dotted {
		return dottedName(name)
	}
	return resourceName(name)
}

// DescribeDomain executes DESCRIBE DOMAIN <name>.
func (c *Client) DescribeDomain(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "DOMAIN", name, true)
}

// DescribeFeed executes DESCRIBE FEED <dotted.name>.
func (c *Client) describeFeed(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "FEED", name, true)
}

// DescribeWriter executes DESCRIBE WRITER <name>.
func (c *Client) describeWriter(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "WRITER", name, false)
}

// DescribeReader executes DESCRIBE READER <name>.
func (c *Client) describeReader(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "READER", name, false)
}

// DescribeSubscription executes DESCRIBE SUBSCRIPTION <name>.
func (c *Client) describeSubscription(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "SUBSCRIPTION", name, false)
}

// DescribeRole executes DESCRIBE ROLE <name>.
func (c *Client) DescribeRole(ctx context.Context, name string) (ControlExecution, error) {
	return c.describe(ctx, "ROLE", name, false)
}

// RenameDomain executes RENAME DOMAIN <current> TO <renamed>.
func (c *Client) RenameDomain(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "DOMAIN", current, renamed, true)
}

// RenameFeed executes RENAME FEED <current> TO <renamed>.
func (c *Client) renameFeed(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "FEED", current, renamed, true)
}

// RenameWriter executes RENAME WRITER <current> TO <renamed>.
func (c *Client) renameWriter(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "WRITER", current, renamed, false)
}

// RenameReader executes RENAME READER <current> TO <renamed>.
func (c *Client) renameReader(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "READER", current, renamed, false)
}

// RenameSubscription executes RENAME SUBSCRIPTION <current> TO <renamed>.
func (c *Client) renameSubscription(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "SUBSCRIPTION", current, renamed, false)
}

// RenameRole executes RENAME ROLE <current> TO <renamed>.
func (c *Client) RenameRole(ctx context.Context, current, renamed string) (ControlExecution, error) {
	return c.rename(ctx, "ROLE", current, renamed, false)
}

// DropDomain executes DROP DOMAIN <name>.
func (c *Client) DropDomain(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "DOMAIN", name, true)
}

// DropFeed executes DROP FEED <dotted.name>.
func (c *Client) dropFeed(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "FEED", name, true)
}

// DropWriter executes DROP WRITER <name>.
func (c *Client) dropWriter(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "WRITER", name, false)
}

// DropReader executes DROP READER <name>.
func (c *Client) dropReader(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "READER", name, false)
}

// DropSubscription executes DROP SUBSCRIPTION <name>.
func (c *Client) dropSubscription(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "SUBSCRIPTION", name, false)
}

// DropRole executes DROP ROLE <name>.
func (c *Client) DropRole(ctx context.Context, name string) (ControlExecution, error) {
	return c.drop(ctx, "ROLE", name, false)
}

// ListDomains executes SHOW DOMAINS; rows live in Results[0].Data.
func (c *Client) ListDomains(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW DOMAINS")
}

// ListFeeds executes SHOW FEEDS.
func (c *Client) listFeeds(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW FEEDS")
}

// ListWriters executes SHOW WRITERS.
func (c *Client) listWriters(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW WRITERS")
}

// ListReaders executes SHOW READERS.
func (c *Client) listReaders(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW READERS")
}

// ListSubscriptions executes SHOW SUBSCRIPTIONS.
func (c *Client) listSubscriptions(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW SUBSCRIPTIONS")
}

// ListRoles executes SHOW ROLES.
func (c *Client) ListRoles(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW ROLES")
}

// ListGrants executes SHOW GRANTS.
func (c *Client) ListGrants(ctx context.Context) (ControlExecution, error) {
	return c.WCL(ctx, "SHOW GRANTS")
}
