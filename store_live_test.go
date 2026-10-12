package whitewater

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Live StateStore smoke against a running Riverbed — create a Domain,
// declare a named-key StateStore, then Put/Get/GetBy/Iterate/Clear/Drop
// through the facade against the node-side store.
//
// Runs only when WHITEWATER_ENDPOINTS is set (comma- or
// semicolon-separated), e.g.
// http://localhost:7371,http://localhost:7372,http://localhost:7373;
// WHITEWATER_API_KEY defaults to the documented development key.
func TestLiveNamedKeyStoreLifecycle(t *testing.T) {
	endpoints := os.Getenv("WHITEWATER_ENDPOINTS")
	if endpoints == "" {
		t.Skip("WHITEWATER_ENDPOINTS unset")
	}
	apiKey := os.Getenv("WHITEWATER_API_KEY")
	if apiKey == "" {
		apiKey = "whitewater-local-development-admin-key"
	}
	parts := strings.FieldsFunc(endpoints, func(r rune) bool {
		return r == ',' || r == ';'
	})
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if !strings.Contains(p, "://") {
			p = "http://" + p
		}
		parts[i] = p
	}
	client := NewClient(apiKey, parts, 10*time.Second)
	ctx := context.Background()

	domain := fmt.Sprintf("smoke.go%x", time.Now().UnixNano()&0x7fffffff)
	name := domain + ".employees"
	ok, err := client.Healthy(ctx)
	if err != nil || !ok {
		t.Fatalf("healthy = %v, %v", ok, err)
	}
	if _, err := client.CreateDomain(ctx, domain); err != nil {
		t.Fatal(err)
	}

	spec, err := NewStateStore(name)
	if err != nil {
		t.Fatal(err)
	}
	spec.AddKey("passport", true).AddKey("department", false)
	store, err := client.StateStores.Define(ctx, spec, "")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Put(ctx, []byte("user-1"), []byte("profile-1"),
		map[string]any{"passport": "P111", "department": "eng"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, []byte("user-2"), []byte("profile-2"),
		map[string]any{"passport": "P222", "department": "eng"}, ""); err != nil {
		t.Fatal(err)
	}

	found, err := store.Get(ctx, []byte("user-1"), true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || string(found.Payload) != "profile-1" {
		t.Fatalf("row = %+v", found)
	}
	if found.Freshness.Behind {
		t.Fatal("strict read must not report behind")
	}

	team, err := store.GetBy(ctx, "department", "eng", 0, true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(team.Rows) != 2 {
		t.Fatalf("department rows = %d, want 2", len(team.Rows))
	}

	solo, err := store.GetBy(ctx, "passport", "P222", 0, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(solo.Rows) != 1 || string(solo.Rows[0].Key) != "user-2" {
		t.Fatalf("passport rows = %+v", solo.Rows)
	}

	var keys []string
	if err := store.Iterate(ctx, 256, func(row StoreRow) error {
		keys = append(keys, string(row.Key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("iterate keys = %v", keys)
	}

	if _, err := store.Clear(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if found, err := store.Get(ctx, []byte("user-1"), false, 0); err != nil || found != nil {
		t.Fatalf("cleared get = %+v, %v", found, err)
	}
	empty, err := store.GetBy(ctx, "department", "eng", 0, true, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Rows) != 0 {
		t.Fatalf("cleared get_by rows = %d", len(empty.Rows))
	}

	if _, err := client.StateStores.Drop(ctx, name); err != nil {
		t.Fatal(err)
	}
}
