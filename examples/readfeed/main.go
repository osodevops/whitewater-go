// Command readfeed is durable consumption: a named Reader owns an
// independent cursor on the Riverbed, so progress survives client
// restarts — the "subscribe" pattern for applications, versus the
// ephemeral tail in ../tailfeed.
//
// For a managed loop — auto-provisioning, retry policy, and a
// Close-able handle — use client.Subscribe instead of this explicit
// form.
//
// The loop below is explicit — Whitewater Readers pull rather than push:
//
//	Open -> [ Fetch -> process -> Ack ] -> Close
//
// Ack is the commit point: acknowledged progress is durable, so a
// restarted client resumes after the last acked position. Process
// records BEFORE acking them — ack-then-crash loses those records.
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

	// One-time provisioning: the Reader declaration is durable Control
	// Plane state, not a per-session object.
	for _, step := range []func() error{
		func() error { _, err := client.CreateDomain(ctx, "demo"); return err },
		func() error { _, err := client.CreateFeed(ctx, "demo.tail"); return err },
		func() error {
			_, err := client.CreateReader(ctx, "demo-reader", "demo.tail", whitewater.StartBeginning())
			return err
		},
	} {
		if err := step(); err != nil {
			log.Fatal(err)
		}
	}

	reader := client.Reader("demo-reader")

	// Open fences this client as the session owner. Another client
	// opening the same Reader supersedes this session (stale_epoch on
	// the next Fetch).
	session, err := reader.Open(ctx, 64)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_, _ = reader.Close(context.Background(), session) // release the fence
	}()

	for {
		// Fetch returns immediately with whatever is committed — a Reader
		// is not a long-poll. For "sit and wait" behavior, use
		// TemporaryFetch(Tail, WaitMs) or pace empty fetches.
		page, err := reader.Fetch(ctx, session, 50)
		if err != nil {
			log.Fatal(err)
		}

		for _, record := range page.Records {
			fmt.Printf("%s @ %d\n", record.Payload, record.IngestTimeNs)
		}

		// Commit durable progress AFTER processing the page. The page's
		// DeliveredCursor covers every range the fetch drew from — ack
		// it, not the last record's cursor.
		if page.DeliveredCursor != nil {
			if session, err = reader.Ack(ctx, session, *page.DeliveredCursor); err != nil {
				log.Fatal(err)
			}
		}

		if len(page.Records) == 0 {
			time.Sleep(500 * time.Millisecond) // poll cadence for live reads
		}
	}
}
