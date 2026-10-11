package whitewater

import (
	"errors"
	"fmt"
)

// ErrorKind classifies a Riverbed failure for retry policy.
type ErrorKind int

const (
	// KindRefused means the request was rejected; fix it before retrying.
	KindRefused ErrorKind = iota
	// KindConflict covers fencing and name conflicts; inspect Code.
	KindConflict
	// KindStaleSession means the session epoch was superseded; re-open.
	KindStaleSession
	// KindLeaseConflict means a work lease was lost, stale, or contended.
	KindLeaseConflict
	// KindUnavailable covers transient Node or Control Plane outages;
	// retrying after backoff is safe.
	KindUnavailable
	// KindAmbiguous covers 5xx results that may have committed; dedup
	// retries are required to disambiguate.
	KindAmbiguous
)

// Error is the single typed failure surface. Kind selects the retry
// class; Code carries the machine-readable wire code when the Riverbed
// supplied one (e.g. "lease.stale_lease", "writer.stale_sequence").
type Error struct {
	Kind    ErrorKind
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s (code %s)", e.Message, e.Code)
	}
	if e.Status != 0 {
		return fmt.Sprintf("%s (http %d)", e.Message, e.Status)
	}
	return e.Message
}

func errorOf(kind ErrorKind, status int, code, message string) *Error {
	if message == "" {
		message = fmt.Sprintf("http %d", status)
	}
	return &Error{Kind: kind, Status: status, Code: code, Message: message}
}

func hasKind(err error, kind ErrorKind) bool {
	var target *Error
	return errors.As(err, &target) && target.Kind == kind
}

// IsStaleSession reports whether the session epoch was superseded.
func IsStaleSession(err error) bool { return hasKind(err, KindStaleSession) }

// IsLeaseConflict reports lease loss, staleness, or contention.
func IsLeaseConflict(err error) bool { return hasKind(err, KindLeaseConflict) }

// IsConflict reports fencing/name conflicts other than lease codes.
func IsConflict(err error) bool { return hasKind(err, KindConflict) }

// IsUnavailable reports a transient outage; backoff-retry is safe.
func IsUnavailable(err error) bool { return hasKind(err, KindUnavailable) }

// IsAmbiguous reports a result that may have committed; dedup-retry.
func IsAmbiguous(err error) bool { return hasKind(err, KindAmbiguous) }

// IsRefused reports outright rejection; fix the request first.
func IsRefused(err error) bool { return hasKind(err, KindRefused) }

// mapError maps a (status, body) pair to a typed *Error, mirroring the
// shared classification in sdk/spec/PROTOCOL.md.
func mapError(status int, body map[string]any) *Error {
	code, _ := body["code"].(string)
	message, _ := body["error"].(string)
	switch code {
	case "stale_epoch":
		return errorOf(KindStaleSession, status, code, message)
	case "lease.busy", "lease.stale_lease", "lease.capacity":
		return errorOf(KindLeaseConflict, status, code, message)
	}
	switch {
	case status == 409:
		return errorOf(KindConflict, status, code, message)
	case status == 503 || status == 0:
		return errorOf(KindUnavailable, status, code, message)
	case status >= 500:
		return errorOf(KindAmbiguous, status, code, message)
	default:
		return errorOf(KindRefused, status, code, message)
	}
}
