# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **return `true` = processed, return an error
= replay** — the SDK acks a page only after every record in it has
been handled.

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
)

func main() {
	// Ctrl-C is the off-switch — signal ctx drives reader.ForEach.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := whitewater.NewClient(
		os.Getenv("WHITEWATER_API_KEY"),
		[]string{"http://localhost:7371", "http://localhost:7372"},
		5*time.Second)

	// ---- provision ---------------------------------------------------
	if err := client.CreateDomain(ctx, "shop"); err != nil {
		log.Fatal(err)
	}
	shop, err := client.GetDomain(ctx, "shop")
	if err != nil {
		log.Fatal(err)
	}
	if err := shop.CreateFeed(ctx, "orders"); err != nil {
		log.Fatal(err)
	}
	if err := shop.CreateFeed(ctx, "orders.dlq"); err != nil {
		log.Fatal(err)
	}

	// ---- produce (usually a different service) -----------------------
	wbuilder, _ := shop.CreateWriter("orders")
	writer, err := wbuilder.WriterID("orders.ingest").Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := writer.Append(ctx, "order-1", []byte(`{"total":42}`)); err != nil {
		log.Fatal(err)
	}

	// ---- consume -----------------------------------------------------
	dbuilder, _ := shop.CreateWriter("orders.dlq")
	deadLetter, err := dbuilder.WriterID("fulfillment.dlq").Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	rbuilder, _ := shop.CreateReader("orders")
	reader, err := rbuilder.
		ReaderID("fulfillment").               // durable id — survives restarts
		StartPoint(whitewater.StartBeginning()). // first-declare only
		MaxPageSize(250).                      // max records per fetch page
		Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	// The goroutine IS the hidden thread — ForEach blocks it alone.
	go func() {
		err := reader.ForEach(ctx, func(m whitewater.FeedMessage[string, []byte]) (bool, error) {
			err := ordersDB.Upsert(orderID(m), order(m)) // idempotent side effect
			switch {
			case err == nil:
				return true, nil
			case permanent(err):
				var key string
				if m.Key != nil {
					key = *m.Key
				}
				deadLetter.Append(ctx, key, m.Value)    // quarantine → continue → acked
				return true, nil
			default:
				return false, err                        // transient → stop → replay
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("reader pump died: %v", err)      // supervise: restart or alert
		}
	}()

	<-ctx.Done()      // service runs until interrupted
}
```

## Notes

- `ReaderID` is durable application state — the Kafka `groupId`
  analogue. `StartPoint` applies only on first declare; restarts
  resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `MaxPageSize(250)` bounds both the page and the replay window.
- Manual mode stays available: `Fetch` → process → `Ack`. Never ack
  before processing — that's at-most-once.
