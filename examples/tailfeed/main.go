// Command tailfeed is ephemeral consumption: an anonymous temporary
// Reader with server-side tail + wait — the "subscribe" pattern for
// live views, UIs, and REPLs. Nothing is provisioned; the reader
// vanishes when the process exits.
//
// The loop:
//
//	TemporaryFetch(Tail, WaitMs) -> records -> NextCursor -> repeat
//
// WaitMs makes each call a long-poll: the Node holds the request until
// records commit or the wait expires, then answers — so the loop does
// NOT burn a request per poll interval; it sits inside the fetch.
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

	if _, err := client.CreateDomain(ctx, "demo"); err != nil {
		log.Fatal(err)
	}
	if _, err := client.CreateFeed(ctx, "demo.tail"); err != nil {
		log.Fatal(err)
	}

	var after *whitewater.Cursor // nil -> feed tail position

	for {
		// Blocks up to WaitMs server-side; returns early when records land.
		page, err := client.Feeds.TemporaryFetch(ctx, "demo.tail",
			whitewater.TemporaryFetchOptions{
				Tail:   true,
				WaitMs: 2500,
				Limit:  50,
				After:  after,
			})
		if err != nil {
			log.Fatal(err)
		}

		for _, record := range page.Records {
			fmt.Printf("%s @ %d\n", record.Payload, record.IngestTimeNs)
		}

		// Continue from the PAGE frontier, not the last record's cursor —
		// the page can be filtered or empty while the frontier advanced.
		after = page.NextCursor
	}
}
