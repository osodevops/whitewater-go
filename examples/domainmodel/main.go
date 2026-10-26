// Command domainmodel is the strongly-typed Domain front door:
// materialized catalog views, relative Feed resolution, and the
// fluent Reader/Writer builders.
//
//	shop.Feeds / Readers / Writers — one batched snapshot, cached
//	shop.Feed("orders") — pure name composition, no I/O
//	NewReaderBuilder(client, shop, orders).ReaderID(...).Open(ctx)
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
		30*time.Second)

	if _, err := client.WCL(ctx, `
		CREATE DOMAIN shop;
		CREATE FEED shop.orders;
		CREATE FEED shop.returns;`); err != nil {
		log.Fatal(err)
	}

	// Materialized Domain view — one batched control read, cached.
	shop, err := client.GetDomain(ctx, "shop")
	if err != nil {
		log.Fatal(err)
	}
	feeds, _ := shop.Feeds(ctx)
	fmt.Println("feeds:", feeds)
	filtered, _ := shop.FeedsMatching(ctx, "ord")
	fmt.Println("filtered:", filtered)

	orders, err := shop.Feed("orders")
	if err != nil {
		log.Fatal(err)
	}

	// Fluent Writer — declares if missing, opens a session, codecs bound.
	writer, err := whitewater.NewWriterBuilder(client, shop, orders).
		WriterID("shop.ingest").
		Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := writer.Append(ctx, "order-1", []byte(`{"total":42}`)); err != nil {
		log.Fatal(err)
	}

	// Fluent Reader — ReaderID is the durable consume identity (the
	// Kafka groupId analogue); StartPoint applies at first declaration.
	reader, err := whitewater.NewReaderBuilder(client, shop, orders).
		ReaderID("shop.fulfillment").
		StartPoint(whitewater.StartBeginning()).
		MaxPageSize(64).
		Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close(ctx)
	for {
		page, err := reader.Fetch(ctx, 0)
		if err != nil {
			log.Fatal(err)
		}
		if len(page.Records) == 0 {
			break
		}
		for _, m := range page.Records {
			key := ""
			if m.Key != nil {
				key = *m.Key
			}
			fmt.Printf("delivered %s -> %s\n", key, string(m.Value))
		}
		if _, err := reader.Ack(ctx); err != nil {
			log.Fatal(err)
		}
	}

	if err := shop.Refresh(ctx); err != nil {
		log.Fatal(err)
	}
	readers, _ := shop.Readers(ctx)
	fmt.Println("readers now in shop:", readers)
}
