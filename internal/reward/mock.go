package reward

import (
	"crypto/hmac"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

type MockPublisher struct {
	Secret         string
	LoseFirstReply bool

	mu      sync.Mutex
	calls   map[string]int
	credits map[string]Event
}

func (m *MockPublisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil || !hmac.Equal([]byte(r.Header.Get("X-Reward-Signature")), []byte(signature(m.Secret, body))) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}

	var event Event
	if err := json.Unmarshal(body, &event); err != nil || event.Amount <= 0 ||
		!identifier.MatchString(event.Publisher) || !identifier.MatchString(event.EventID) {
		http.Error(w, "bad reward", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key != idempotencyKey(event) {
		http.Error(w, "bad idempotency key", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	if m.calls == nil {
		m.calls = make(map[string]int)
		m.credits = make(map[string]Event)
	}
	if existing, credited := m.credits[key]; credited && existing != event {
		m.mu.Unlock()
		http.Error(w, "key reused for a different reward", http.StatusConflict)
		return
	}
	m.calls[key]++
	first := m.calls[key] == 1
	if first {
		m.credits[key] = event
	}
	m.mu.Unlock()

	if first && m.LoseFirstReply {
		// Credit before waiting. The dispatcher's deadline cancels the first request.
		<-r.Context().Done()
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *MockPublisher) Counts() (calls, credits int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, count := range m.calls {
		calls += count
	}
	return calls, len(m.credits)
}
