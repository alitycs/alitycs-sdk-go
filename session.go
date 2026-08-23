package alitycs

import (
	"sync"
	"time"
)

// sessionData is a point-in-time copy of the session state.
type sessionData struct {
	ID          string
	AnonymousID string
	UserID      string
}

// sessionManager owns the session ID, the anonymous ID and the identified
// user. The session rotates after an inactivity timeout (keeping the
// anonymous ID); Reset rotates both and clears the user.
type sessionManager struct {
	mu       sync.Mutex
	timeout  time.Duration
	now      func() time.Time
	session  sessionData
	activity time.Time
}

func newSessionManager(timeout time.Duration) *sessionManager {
	now := time.Now
	m := &sessionManager{timeout: timeout, now: now}
	m.session = m.create("")
	m.activity = now()
	return m
}

// next records activity (rotating the session first if expired) and returns
// the resulting state atomically.
func (m *sessionManager) next() sessionData {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now().Sub(m.activity) > m.timeout {
		m.session = m.create(m.session.AnonymousID)
	}
	m.activity = m.now()
	return m.session
}

// snapshot returns a copy of the current session state without recording
// activity.
func (m *sessionManager) snapshot() sessionData {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.session
}

// setUserID identifies the current session.
func (m *sessionManager) setUserID(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.session.UserID = userID
	m.activity = m.now()
}

// reset starts a fresh anonymous identity and clears the identified user.
func (m *sessionManager) reset() sessionData {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.session = m.create("")
	m.activity = m.now()
	return m.session
}

// create builds a new session. An empty previousAnonymousID generates a fresh
// anonymous ID; otherwise the existing one carries over.
func (m *sessionManager) create(previousAnonymousID string) sessionData {
	anonymousID := previousAnonymousID
	if anonymousID == "" {
		anonymousID = prefixAnon + generateID()
	}
	return sessionData{
		ID:          prefixSession + generateID(),
		AnonymousID: anonymousID,
	}
}
