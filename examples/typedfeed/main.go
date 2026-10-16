// Command typedfeed is the codec-bound Feed[K,V] flow: bind a feed to
// key/payload codecs once, then Append(key, value) encodes and every
// Fetch/Records decodes — application code never touches bytes.
//
//	orders.Writer("ingest")   — declares the Writer + opens a session
//	orders.Reader("audit", 64) — declares the Reader + opens a session
//	writer.Append / reader.Fetch / reader.Ack
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
)

type Order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

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

	for _, step := range []func() error{
		func() error { _, err := client.CreateDomain(ctx, "demo"); return err },
		func() error { _, err := client.CreateFeed(ctx, "demo.orders"); return err },
	} {
		if err := step(); err != nil {
			log.Fatal(err)
		}
	}

	// String keys + JSON payloads — both codecs inferred from the types.
	orders, err := whitewater.FeedFor[Order](client, "demo.orders")
	if err != nil {
		log.Fatal(err)
	}

	// Declares "ingest" when missing and opens its epoch-fenced session.
	writer, err := orders.Writer(ctx, "ingest")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := writer.Append(ctx, "order-1", Order{ID: "order-1", Total: 42}); err != nil {
		log.Fatal(err)
	}
	if _, err := writer.Append(ctx, "order-2", Order{ID: "order-2", Total: 7}); err != nil {
		log.Fatal(err)
	}

	// Declares "audit" at BEGINNING when missing and opens a session;
	// Fetch returns decoded messages — Order, not []byte.
	reader, err := orders.Reader(ctx, "audit", 64)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close(ctx)

	for {
		page, err := reader.Fetch(ctx, 0)
		if err != nil {
			log.Fatal(err)
		}
		for _, m := range page.Records {
			key := ""
			if m.Key != nil {
				key = *m.Key
			}
			fmt.Printf("%s seq=%d %s total=%d\n",
				m.Cursor, m.Sequence, key, m.Value.Total)
		}
		if len(page.Records) == 0 {
			break
		}
		if _, err := reader.Ack(ctx); err != nil {
			log.Fatal(err)
		}
	}
}
