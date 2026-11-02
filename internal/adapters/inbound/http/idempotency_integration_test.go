//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func pgKeyedPost(t *testing.T, srv *httptest.Server, key, body string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/process-paths", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, resp.Header, string(readAll(t, resp))
}

// TestPostgresIdempotency drives the REAL router over real Postgres: fresh
// key, byte-identical replay, reuse -> 409, missing -> 400, a cached 4xx.
func TestPostgresIdempotency(t *testing.T) {
	srv := newPostgresRouter(t)
	body := `{"id":"idem-1","name":"Idem","steps":["PICK","PACK"]}`

	s1, h1, b1 := pgKeyedPost(t, srv, "pg-k-1", body)
	if s1 != http.StatusCreated || h1.Get("Idempotency-Replayed") != "" {
		t.Fatalf("fresh: %d %q %s", s1, h1.Get("Idempotency-Replayed"), b1)
	}
	s2, h2, b2 := pgKeyedPost(t, srv, "pg-k-1", body)
	if s2 != s1 || b2 != b1 || h2.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay: %d %q %s", s2, h2.Get("Idempotency-Replayed"), b2)
	}
	s3, _, b3 := pgKeyedPost(t, srv, "pg-k-1", `{"id":"idem-2","name":"Other","steps":["PACK"]}`)
	if s3 != http.StatusConflict || !strings.Contains(b3, "/idempotency-key-reused") {
		t.Fatalf("reuse: %d %s", s3, b3)
	}
	s4, _, b4 := pgKeyedPost(t, srv, "", body)
	if s4 != http.StatusBadRequest || !strings.Contains(b4, "/idempotency-key-required") {
		t.Fatalf("missing: %d %s", s4, b4)
	}
	bad := `{"id":"idem-3","name":"Bad","steps":[]}`
	c1, _, cb1 := pgKeyedPost(t, srv, "pg-k-bad", bad)
	c2, ch2, cb2 := pgKeyedPost(t, srv, "pg-k-bad", bad)
	if c1 != http.StatusUnprocessableEntity || c2 != c1 || cb1 != cb2 || ch2.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("cached 4xx: %d/%d", c1, c2)
	}
}

// TestPostgresIdempotency_ConcurrentSameKeyOneEffect fires N same-key
// process-path registrations: every answer is the
// same 201 (the loser replays the winner's committed answer).
func TestPostgresIdempotency_ConcurrentSameKeyOneEffect(t *testing.T) {
	srv := newPostgresRouter(t)
	body := `{"id":"idem-c","name":"C","steps":["PICK"]}`
	var wg sync.WaitGroup
	results := make([]string, 8)
	statuses := make([]int, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i], _, results[i] = pgKeyedPost(t, srv, "pg-k-conc", body)
		}()
	}
	wg.Wait()
	for i := range results {
		if statuses[i] != http.StatusCreated || results[i] != results[0] {
			t.Fatalf("answer %d: %d %s (first %s)", i, statuses[i], results[i], results[0])
		}
	}
}
