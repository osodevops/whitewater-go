// Command enrichusers is a permanent enrichment consumer -- the
// Whitewater equivalent of a Kafka consumer loop, with the plumbing
// removed:
//
//	consumer := feed.Consumer("enricher")
//	for msg := range consumer.Messages(ctx) {
//	    process(msg)
//	}
//
// feed.Consumer(name) binds the durable named Reader (Kafka's
// consumer-group equivalent): fetch -> deliver -> ack runs on a
// supervised loop inside the SDK -- transient failures retry with
// backoff, a fenced session re-opens, and progress resumes from the
// server-side cursor across restarts. No offset management, ever.
//
// Prerequisites (against the dev cluster): domain "users", feeds
// users.update.request / users.update.enriched.request /
// users.update.dlq, StateStore users.profiles with named key "user_id",
// manual store users.stats.
//
// Replay honesty: between "enriched append committed" and "input acked"
// a crash replays the input -- so every effect carries a RequestID
// derived from the input MessageID, which the server dedups. Replay
// becomes a no-op instead of a duplicate. Poison pills are the app's
// job: this example routes them to the DLQ.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	whitewater "github.com/osodevops/whitewater-go"
)

type UpdateRequest struct {
	RequestID string            `json:"request_id"`
	UserEmail string            `json:"user_email"`
	UserID    string            `json:"user_id"`
	Changes   map[string]string `json:"changes"`
}

type EnrichedUpdate struct {
	RequestID   string            `json:"request_id"`
	UserID      string            `json:"user_id"`
	Email       string            `json:"email"`
	DisplayName string            `json:"display_name"`
	Region      string            `json:"region"`
	Changes     map[string]string `json:"changes"`
}

type UserProfile struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Region      string `json:"region"`
}

type DeadLetter struct {
	Reason          string         `json:"reason"`
	SourceFeed      string         `json:"source_feed"`
	SourceMessageID string         `json:"source_message_id"`
	SourceCursor    string         `json:"source_cursor"`
	Original        *UpdateRequest `json:"original"`
}

var (
	outWriter      *whitewater.FeedWriter[string, EnrichedUpdate]
	dlqWriter      *whitewater.FeedWriter[string, DeadLetter]
	users          *whitewater.Store[string, UserProfile]
	counters       *whitewater.Store[string, int64]
	exampleContext = context.Background()
)

func main() {
	// SIGINT/SIGTERM -> cancel -> Messages drains, the session closes.
	ctx, stop := signal.NotifyContext(
		exampleContext, os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := whitewater.NewClient(
		"whitewater-local-development-admin-key",
		[]string{
			"http://localhost:7371",
			"http://localhost:7372",
			"http://localhost:7373",
		},
		30*time.Second,
	)

	var err error
	in, err := whitewater.FeedFor[UpdateRequest](client, "users.update.request")
	if err != nil {
		log.Fatal(err)
	}
	out, err := whitewater.FeedFor[EnrichedUpdate](client, "users.update.enriched.request")
	if err != nil {
		log.Fatal(err)
	}
	dlq, err := whitewater.FeedFor[DeadLetter](client, "users.update.dlq")
	if err != nil {
		log.Fatal(err)
	}
	users, err = whitewater.StoreFor[UserProfile](client, "users.profiles")
	if err != nil {
		log.Fatal(err)
	}
	counters, err = whitewater.StoreFor[int64](client, "users.stats")
	if err != nil {
		log.Fatal(err)
	}
	outWriter, err = out.Writer(ctx, "enricher")
	if err != nil {
		log.Fatal(err)
	}
	dlqWriter, err = dlq.Writer(ctx, "enricher")
	if err != nil {
		log.Fatal(err)
	}

	// The permanent loop: durable "enricher" Reader, supervised retries,
	// per-record delivery, ack after each page's records are delivered.
	consumer := in.Consumer("enricher")
	for msg := range consumer.Messages(ctx) {
		if err := process(ctx, msg); err != nil {
			// A processing failure here only skips the DLQ/counters --
			// the page stays unacked unless Close lands, so it replays.
			log.Printf("process %s: %v", msg.Cursor, err)
		}
	}
	if err := consumer.Err(); err != nil {
		log.Fatal(err) // startup refusal -- fix and redeploy
	}
}

func process(
	ctx context.Context, msg whitewater.FeedMessage[string, UpdateRequest],
) error {
	req := msg.Value

	// 1. primary-key lookup by email -- value only, no row unwrapping
	profile, found, err := users.Find(ctx, req.UserEmail, false, 0)
	if err != nil {
		return err
	}

	// 2. named-key fallback -- the declared "user_id" index on the store
	if !found && req.UserID != "" {
		profile, found, err = users.FindBy(ctx, "user_id", req.UserID, false, 0)
		if err != nil {
			return err
		}
	}

	// Idempotency key for every effect this input produces: a replay
	// after a crash re-sends the same RequestID, which the server
	// dedups to the original append -- no double-emit, ever.
	effectID := deterministicID("enrich", msg.MessageID)

	if !found {
		_, err = dlqWriter.AppendEvent(ctx, whitewater.FeedEvent[string, DeadLetter]{
			RequestID: effectID,
			Key:       &req.UserEmail,
			Value: DeadLetter{
				Reason:          "user not found",
				SourceFeed:      "users.update.request",
				SourceMessageID: msg.MessageID,
				SourceCursor:    string(msg.Cursor),
				Original:        &req,
			},
			Metadata: whitewater.TextMetadata(
				map[string]string{"reason": "enrichment-miss"}),
		})
		return err
	}

	_, err = outWriter.AppendEvent(ctx, whitewater.FeedEvent[string, EnrichedUpdate]{
		RequestID: effectID,
		Key:       &profile.UserID,
		Value: EnrichedUpdate{
			RequestID:   req.RequestID,
			UserID:      profile.UserID,
			Email:       profile.Email,
			DisplayName: profile.DisplayName,
			Region:      profile.Region,
			Changes:     req.Changes,
		},
	})
	if err != nil {
		return err
	}

	// 3. counters -- bucket keys roll up day/week/month. RMW is safe
	//    here because this service is the only writer to these keys.
	now := time.Now().UTC()
	year, week := now.ISOWeek()
	for _, bucket := range []string{
		fmt.Sprintf("updates.day.%s", now.Format("2006-01-02")),
		fmt.Sprintf("updates.week.%d-W%02d", year, week),
		fmt.Sprintf("updates.month.%s", now.Format("2006-01")),
	} {
		count, err := counters.GetOrDefault(ctx, bucket, int64(0))
		if err != nil {
			return err
		}
		if _, err := counters.Put(ctx, bucket, count+1, nil, ""); err != nil {
			return err
		}
	}
	return nil
}

// deterministicID derives a stable effect identity from the input
// MessageID -- replay-safe like Java's nameUUIDFromBytes.
func deterministicID(scope, messageID string) string {
	sum := sha256.Sum256([]byte(scope + ":" + messageID))
	return hex.EncodeToString(sum[:16])
}
