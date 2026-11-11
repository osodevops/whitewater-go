// Command crossdomain shows a Pipe whose endpoints span Domains —
// purchase orders captured in the "purchasing" Domain relay into the
// "warehouse" Domain for fulfilment, with the warehouse's own profile
// store enriching each order on the way through.
//
// The rule: the Pipe's NAME and its input Subscription always live in
// the declaring Domain (the durable frontier is owned there); the
// Subscription's source Feed and the Pipe's outputs may live in
// another Domain, referenced through that Domain's handles or
// whitewater.Abs(...) — an explicit ScopedRef, never a bare string.
//
//	purchasing.orders          — where orders land
//	warehouse.fulfil           — subscription owned by warehouse,
//	                             sourcing purchasing's feed
//	warehouse.relay            — the Pipe, owned by warehouse
//	warehouse.orders.inbound   — output feed in warehouse
//	warehouse.orders.misses    — quarantine feed in warehouse
//	warehouse.profiles         — lookup store in warehouse
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
)

func main() {
	ctx := context.Background()
	client := whitewater.NewClient(
		"whitewater-local-development-admin-key",
		[]string{
			"http://localhost:7371",
			"http://localhost:7372",
			"http://localhost:7373",
		},
		30*time.Second,
	)

	purchasing, err := client.CreateDomain(ctx, "purchasing")
	if err != nil {
		log.Fatal(err)
	}
	warehouse, err := client.CreateDomain(ctx, "warehouse")
	if err != nil {
		log.Fatal(err)
	}

	// Orders land in the purchasing Domain — the handle carries its
	// qualified name for the cross-Domain subscription below.
	if _, err := purchasing.CreateFeed(ctx, "orders"); err != nil {
		log.Fatal(err)
	}
	orders, err := purchasing.Feed("orders")
	if err != nil {
		log.Fatal(err)
	}

	// The warehouse owns its fulfilment resources.
	for _, feed := range []string{"orders.inbound", "orders.misses"} {
		if _, err := warehouse.CreateFeed(ctx, feed); err != nil {
			log.Fatal(err)
		}
	}
	profiles, err := whitewater.NewStateStore("profiles")
	if err != nil {
		log.Fatal(err)
	}
	profiles.AddUniqueKey("email", whitewater.JsonExtractor("$.email"))
	if _, err := warehouse.DefineStateStore(ctx, profiles, nil); err != nil {
		log.Fatal(err)
	}

	// The Subscription belongs to warehouse — that is where its durable
	// progress lives — but its SOURCE FEED is purchasing's, passed as a
	// handle (an explicit ScopedRef; whitewater.Abs("purchasing.orders")
	// says the same thing as a string).
	if _, err := warehouse.CreateSubscription(ctx, "fulfil",
		orders, whitewater.StartBeginning()); err != nil {
		log.Fatal(err)
	}

	// warehouse.relay enriches each purchase order against the
	// warehouse's own profile store and drops misses on its own
	// quarantine feed — every endpoint in-domain, only the input
	// crossed Domains (inside the subscription above).
	if _, err := warehouse.DefineEnrichPipe(ctx, "relay",
		whitewater.Rel("fulfil"),
		whitewater.Rel("orders.inbound"),
		whitewater.Rel("profiles"),
		whitewater.EnrichOperation(
			whitewater.JsonExtractor("$.customer_id"),
			whitewater.EnrichOptions{
				Named:  "email",
				OnMiss: whitewater.EnrichMissQuarantine,
			}),
		whitewater.Rel("orders.misses")); err != nil {
		log.Fatal(err)
	}
	fmt.Println("warehouse.relay declared — purchasing.orders -> warehouse.orders.inbound")

	// Same topology, written with absolute references instead of the
	// handle: identical wire, equally deliberate.
	if _, err := warehouse.DefineForwardPipe(ctx, "audit.trail",
		whitewater.Rel("fulfil"),
		whitewater.Abs("purchasing.orders.archive")); err != nil {
		log.Fatal(err)
	}
	fmt.Println("warehouse.audit.trail declared — Abs() endpoint spelled out")
}
