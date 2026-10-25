// Command definepipes declares managed Pipes — the server-side
// forward/enrich/window processing steps driven off a Subscription —
// then walks one through its lifecycle (describe, pause, resume).
//
// A Pipe replaces the hand-rolled consumer loop in ../enrichusers: the
// lookup, the output append, and replay safety move server-side. The
// SDK surface stays pure declaration — feeds, stores, subscriptions,
// never partitions.
//
// Prerequisites (against the dev cluster): none — the program
// provisions its domain, feeds, subscription, and lookup store before
// declaring pipes. The window output store is managed by the Pipe and
// must NOT pre-exist.
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

	// Provisioning: domain + feeds + the source Subscription every Pipe
	// consumes + the enrichment lookup store.
	for _, step := range []func() error{
		func() error { _, err := client.CreateDomain(ctx, "pipedemo"); return err },
		func() error { _, err := client.CreateFeed(ctx, "pipedemo.updates"); return err },
		func() error { _, err := client.CreateFeed(ctx, "pipedemo.updates.copy"); return err },
		func() error { _, err := client.CreateFeed(ctx, "pipedemo.updates.enriched"); return err },
		func() error { _, err := client.CreateFeed(ctx, "pipedemo.updates.dlq"); return err },
		func() error { _, err := client.CreateFeed(ctx, "pipedemo.updates.panes"); return err },
		func() error {
			_, err := client.CreateSubscription(
				ctx, "pipedemo.updates.sub", "pipedemo.updates", whitewater.StartBeginning())
			return err
		},
		func() error {
			profiles, err := whitewater.NewStateStore("pipedemo.profiles")
			if err != nil {
				return err
			}
			_, err = client.StateStores.Define(ctx, profiles, "")
			return err
		},
		func() error {
			// A window Pipe's output store must pre-declare non-unique
			// 'key' and 'pane' named keys — pane rows post their group
			// key and pane index under them so the view stays enumerable.
			panes, err := whitewater.NewStateStore("pipedemo.minute.counts")
			if err != nil {
				return err
			}
			panes.AddKey("key", false).AddKey("pane", false)
			_, err = client.StateStores.Define(ctx, panes, "")
			return err
		},
	} {
		if err := step(); err != nil {
			log.Fatal(err)
		}
	}

	// 1. forward — the copy Pipe: subscription -> output Feed verbatim.
	if _, err := client.DefineForwardPipe(
		ctx, "pipedemo.relay", "pipedemo.updates.sub", "pipedemo.updates.copy"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("declared pipedemo.relay (forward)")

	// 2. enrich — point-lookup join against pipedemo.profiles keyed by a
	//    JSON projection; misses route to the quarantine feed.
	enrich := whitewater.EnrichOperation(
		whitewater.JsonExtractor("user_id"),
		whitewater.EnrichOptions{
			Freshness: whitewater.EnrichLatest,
			OnMiss:    whitewater.EnrichMissQuarantine,
		})
	if _, err := client.DefineEnrichPipe(
		ctx, "pipedemo.enrich", "pipedemo.updates.sub", "pipedemo.updates.enriched",
		"pipedemo.profiles", enrich, "pipedemo.updates.dlq"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("declared pipedemo.enrich (enrich, quarantine-on-miss)")

	// 3. window — one-minute counts per record Key into the managed
	//    pipedemo.minute.counts view; closed panes emit to a feed.
	window := whitewater.WindowOperation(
		time.Minute,
		whitewater.KeyExtractor(),
		whitewater.CountAggregation(),
		whitewater.WindowOptions{
			Time:    whitewater.WindowEventTime,
			MaxLate: 30 * time.Second,
			Expire:  time.Hour,
		})
	if _, err := client.DefineWindowPipe(
		ctx, "pipedemo.minutecounts", "pipedemo.updates.sub", "pipedemo.minute.counts",
		window, "pipedemo.updates.panes"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("declared pipedemo.minutecounts (window count)")

	// Lifecycle: describe the catalog row, then pause and resume — the
	// same drive verbs every Pipe honors.
	pipe := client.Pipe("pipedemo.enrich")
	info, err := pipe.Describe(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("pipe %s stage=%s status=%s\n", info.Name, info.Stage, info.Status)
	if err := pipe.Pause(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("paused — in-flight effects finish, the frontier stays put")
	if err := pipe.Resume(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("resumed — driving continues from the durable frontier")
}
