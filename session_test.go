package alitycs

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func newClockSession(timeout time.Duration) (*sessionManager, *time.Time) {
	current := time.UnixMilli(1_000_000)
	m := &sessionManager{timeout: timeout, now: func() time.Time { return current }}
	m.session = m.create("")
	m.activity = current
	return m, &current
}

func advance(clock *time.Time, d time.Duration) {
	*clock = clock.Add(d)
}

func TestSessionFreshTouchKeepsIdentity(t *testing.T) {
	m, clock := newClockSession(30 * time.Minute)

	first := m.next()
	advance(clock, time.Minute)
	second := m.next()

	if first.ID != second.ID || first.AnonymousID != second.AnonymousID {
		t.Fatalf("fresh activity rotated the session: %+v vs %+v", first, second)
	}
}

func TestSessionExpiryRotatesSessionKeepsAnonymous(t *testing.T) {
	m, clock := newClockSession(30 * time.Minute)

	before := m.next()
	advance(clock, 31*time.Minute) // strictly past the timeout
	after := m.next()

	if after.ID == before.ID {
		t.Errorf("expired session kept its ID %q", before.ID)
	}
	if after.AnonymousID != before.AnonymousID {
		t.Errorf("expiry rotated anonymousId %q -> %q; only reset may do that", before.AnonymousID, after.AnonymousID)
	}
}

func TestSessionSetUserIDAndReset(t *testing.T) {
	m, _ := newClockSession(30 * time.Minute)

	m.setUserID("usr_1842")
	if got := m.snapshot(); got.UserID != "usr_1842" {
		t.Fatalf("snapshot userId = %q, want usr_1842", got.UserID)
	}

	resetState := m.reset()
	if strings.HasPrefix(resetState.UserID, "usr_") || resetState.UserID != "" {
		t.Fatalf("reset kept userId %q", resetState.UserID)
	}
	fresh := m.next()
	if fresh.UserID != "" {
		t.Errorf("post-reset events would still carry userId %q", fresh.UserID)
	}
	if fresh.AnonymousID == "" || !strings.HasPrefix(fresh.AnonymousID, prefixAnon) {
		t.Errorf("post-reset anonymousId = %q", fresh.AnonymousID)
	}
}

func TestSessionIDsAreUniqueAndPrefixed(t *testing.T) {
	m, clock := newClockSession(time.Minute)
	seen := map[string]bool{}
	for i := 0; i < 25; i++ {
		advance(clock, 2*time.Minute) // expire between touches
		state := m.next()
		if seen[state.ID] {
			t.Fatalf("session id %s repeated", state.ID)
		}
		seen[state.ID] = true
		if !strings.HasPrefix(state.ID, prefixSession) {
			t.Fatalf("session id %q lacks prefix", state.ID)
		}
	}
}

func TestSessionConcurrentAccessIsSafe(t *testing.T) {
	m, _ := newClockSession(time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _ = m.next() }()
		go func() { defer wg.Done(); m.setUserID("usr_racing") }()
		go func() { defer wg.Done(); _ = m.snapshot() }()
	}
	wg.Wait()
}
