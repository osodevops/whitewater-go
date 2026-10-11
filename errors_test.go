package whitewater

import (
	"encoding/json"
	"errors"
	"testing"
)

func body(t *testing.T, raw string) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		t.Fatalf("fixture json: %v", err)
	}
	return object
}

func TestWireCodesMapToTypedKinds(t *testing.T) {
	stale := mapError(409, body(t, `{"error":"epoch superseded","code":"stale_epoch"}`))
	if stale.Kind != KindStaleSession || stale.Code != "stale_epoch" {
		t.Errorf("stale_epoch mapped to %+v", stale)
	}
	for _, code := range []string{"lease.busy", "lease.stale_lease", "lease.capacity"} {
		err := mapError(409, body(t, `{"error":"x","code":"`+code+`"}`))
		if err.Kind != KindLeaseConflict || err.Code != code {
			t.Errorf("%s mapped to %+v", code, err)
		}
	}
}

func TestStatusClassIsTheFallbackWithoutACode(t *testing.T) {
	cases := []struct {
		status int
		kind   ErrorKind
	}{
		{409, KindConflict},
		{503, KindUnavailable},
		{500, KindAmbiguous},
		{502, KindAmbiguous},
		{400, KindRefused},
	}
	for _, test := range cases {
		err := mapError(test.status, body(t, `{"error":"x"}`))
		if err.Kind != test.kind {
			t.Errorf("status %d mapped to %v", test.status, err.Kind)
		}
	}
}

func TestPredicatesMatchKinds(t *testing.T) {
	var err error = mapError(409, body(t, `{"error":"x","code":"stale_epoch"}`))
	if !IsStaleSession(err) || IsLeaseConflict(err) || IsConflict(err) {
		t.Error("predicate mismatch for stale_epoch")
	}
	var plain error = errors.New("unrelated")
	if IsStaleSession(plain) || IsUnavailable(plain) {
		t.Error("plain errors must not classify")
	}
}
