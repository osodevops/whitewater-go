package whitewater

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Live store smoke against a running Riverbed — create Domain + Feed,
// append keyed records with metadata, then LoadStore a projected
// history read and named-key lookups.
//
// Runs only when WHITEWATER_ENDPOINTS is set (comma-separated), e.g.
// http://localhost:7371,http://localhost:7372,http://localhost:7373;
// WHITEWATER_API_KEY defaults to the documented development key.
func TestLiveStoreCreateWriteLoadAndQuery(t *testing.T) {
	endpoints := os.Getenv("WHITEWATER_ENDPOINTS")
	if endpoints == "" {
		t.Skip("WHITEWATER_ENDPOINTS unset")
	}
	apiKey := os.Getenv("WHITEWATER_API_KEY")
	if apiKey == "" {
		apiKey = "whitewater-local-development-admin-key"
	}
	client := NewClient(apiKey, strings.Split(endpoints, ","), 10*time.Second)
	ctx := context.Background()

	domain := fmt.Sprintf("smoke.go%x", time.Now().UnixNano()&0x7fffffff)
	feed := domain + ".events"
	ok, err := client.Healthy(ctx)
	if err != nil || !ok {
		t.Fatalf("healthy = %v, %v", ok, err)
	}
	if _, err := client.CreateDomain(ctx, domain); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateFeed(ctx, feed); err != nil {
		t.Fatal(err)
	}

	session := fmt.Sprintf("00000000-0000-7000-8000-%012x", time.Now().UnixNano()&0xffffffffffff)
	rows := [][3]string{
		{"emp-1", "alice", "eng"},
		{"emp-2", "bob", "eng"},
		{"emp-3", "cara", "ops"},
	}
	base := time.Now().UnixNano() & 0xffffffffffff
	for i, row := range rows {
		_, err := client.Feeds.Append(ctx, feed, FeedAppend{
			RequestID:       fmt.Sprintf("00000000-0000-7000-8000-%012x", base+int64(i)+1),
			WriterSessionID: session,
			WriterEpoch:     1,
			Sequence:        int64(i + 1),
			Key:             []byte(row[0]),
			Payload:         []byte(row[1]),
			Metadata:        map[string][]byte{"department": []byte(row[2])},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	store := NewRecordStore("employee-store").AddKey("department")
	loaded, err := client.LoadStore(store).FromFeed(
		ctx, feed, StartBeginning(),
		func(s *RecordStore, r Record) {
			s.Upsert(r).WithKey(r.Key).WithNamedKey("department", r.Metadata["department"])
		})
	if err != nil {
		t.Fatal(err)
	}
	if loaded != 3 || store.Len() != 3 {
		t.Fatalf("loaded=%d size=%d, want 3/3", loaded, store.Len())
	}
	if rec, ok := store.GetString("emp-1"); !ok || string(rec.Payload) != "alice" {
		t.Fatalf("primary get: %v %v", rec.Payload, ok)
	}
	if got := store.GetByString("department", "eng"); len(got) != 2 {
		t.Fatalf("eng postings = %d, want 2", len(got))
	}
	if got := store.GetByString("department", "ops"); len(got) != 1 {
		t.Fatalf("ops postings = %d, want 1", len(got))
	}

	copy := NewRecordStore("employee-copy")
	store.Copy(copy)
	if len(copy.GetByString("department", "eng")) != 2 {
		t.Fatal("copy must carry named postings")
	}
	if !store.Delete([]byte("emp-1")) || len(store.GetByString("department", "eng")) != 1 {
		t.Fatal("delete must strip named postings")
	}

	visited := make(chan struct{}, 2)
	store.ForEachParallel(2, func(entry StoreEntry) {
		if len(entry.PrimaryKey) == 0 {
			t.Error("missing primary key")
		}
		visited <- struct{}{}
	})
	<-visited
	<-visited
}
