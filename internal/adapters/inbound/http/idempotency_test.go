package http_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// newKeyedServer is the process-path surface behind the real Idempotency-Key
// middleware (memory UnitOfWork + store).
func newKeyedServer(t *testing.T) (*httptest.Server, *memory.ProcessPathRepo) {
	t.Helper()
	paths := memory.NewProcessPathRepo()
	s := &inboundhttp.Server{
		RegisterProcessPath: &usecases.RegisterProcessPath{Repo: paths},
		UnitOfWork:          memory.NewUnitOfWork(),
		Idempotency:         memory.NewIdempotencyStore(),
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return srv, paths
}

func keyedPost(t *testing.T, srv *httptest.Server, path, key, body string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	status, h, b := doRequest(t, req)
	return status, h, string(b)
}

const pathBody = `{"id":"p1","name":"P1","steps":["PICK"]}`

func TestIdempotency_FreshThenByteIdenticalReplay(t *testing.T) {
	srv, _ := newKeyedServer(t)
	s1, h1, b1 := keyedPost(t, srv, "/process-paths", "k-1", pathBody)
	if s1 != http.StatusCreated || h1.Get("Idempotency-Replayed") != "" {
		t.Fatalf("first: %d replayed=%q %s", s1, h1.Get("Idempotency-Replayed"), b1)
	}
	s2, h2, b2 := keyedPost(t, srv, "/process-paths", "k-1", pathBody)
	if s2 != s1 || b2 != b1 || h2.Get("Idempotency-Replayed") != "true" || h2.Get("Content-Type") != h1.Get("Content-Type") {
		t.Fatalf("replay: %d %q %q vs %q", s2, h2.Get("Idempotency-Replayed"), b2, b1)
	}
}

func TestIdempotency_SameKeyDifferentRequestIs409Reused(t *testing.T) {
	srv, _ := newKeyedServer(t)
	keyedPost(t, srv, "/process-paths", "k-2", pathBody)
	status, _, body := keyedPost(t, srv, "/process-paths", "k-2", `{"id":"p2","name":"P2","steps":["PACK"]}`)
	if status != http.StatusConflict || !strings.Contains(body, "/idempotency-key-reused") {
		t.Fatalf("different body: %d %s", status, body)
	}
	// The key is global: the same key on another ROUTE is reuse too.
	status, _, body = keyedPost(t, srv, "/process-capacities", "k-2", pathBody)
	if status != http.StatusConflict || !strings.Contains(body, "/idempotency-key-reused") {
		t.Fatalf("different route: %d %s", status, body)
	}
}

func TestIdempotency_MissingAndInvalidKey(t *testing.T) {
	srv, _ := newKeyedServer(t)
	status, _, body := keyedPost(t, srv, "/process-paths", "", pathBody)
	if status != http.StatusBadRequest || !strings.Contains(body, "/idempotency-key-required") {
		t.Fatalf("missing: %d %s", status, body)
	}
	for name, key := range map[string]string{"too long": strings.Repeat("a", 129), "non-ascii": "chave-é", "control": "a\tb"} {
		status, _, body := keyedPost(t, srv, "/process-paths", key, pathBody)
		if status != http.StatusBadRequest || !strings.Contains(body, "/idempotency-key-invalid") {
			t.Fatalf("%s: %d %s", name, status, body)
		}
	}
	if status, _, _ := keyedPost(t, srv, "/process-paths", strings.Repeat("a", 128), pathBody); status != http.StatusCreated {
		t.Fatalf("128 chars must be accepted, got %d", status)
	}
}

func TestIdempotency_4xxIsCachedAndReplayed(t *testing.T) {
	srv, _ := newKeyedServer(t)
	bad := `{"id":"p3","name":"P3","steps":[]}`
	s1, _, b1 := keyedPost(t, srv, "/process-paths", "k-4", bad)
	s2, h2, b2 := keyedPost(t, srv, "/process-paths", "k-4", bad)
	if s1 != http.StatusUnprocessableEntity || s2 != s1 || b1 != b2 || h2.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("4xx replay: %d/%d %q", s1, s2, h2.Get("Idempotency-Replayed"))
	}
}

func TestIdempotency_5xxIsNotStored(t *testing.T) {
	calls := 0
	s := &inboundhttp.Server{UnitOfWork: memory.NewUnitOfWork(), Idempotency: memory.NewIdempotencyStore()}
	h := s.ExportIdempotent(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	})
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
		req.Header.Set("Idempotency-Key", "k-5")
		h(rec, req)
		if rec.Code != http.StatusInternalServerError || rec.Header().Get("Idempotency-Replayed") != "" {
			t.Fatalf("attempt %d: %d", i, rec.Code)
		}
	}
	if calls != 2 {
		t.Fatalf("a 5xx must re-execute on retry, handler ran %d times", calls)
	}
}

func TestIdempotency_ConcurrentSameKeyRunsOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	s := &inboundhttp.Server{UnitOfWork: memory.NewUnitOfWork(), Idempotency: memory.NewIdempotencyStore()}
	h := s.ExportIdempotent(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	})
	var wg sync.WaitGroup
	replays := make([]string, 8)
	for i := range replays {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
			req.Header.Set("Idempotency-Key", "k-6")
			h(rec, req)
			replays[i] = rec.Header().Get("Idempotency-Replayed")
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("exactly one effect expected, handler ran %d times", calls)
	}
	n := 0
	for _, r := range replays {
		if r == "true" {
			n++
		}
	}
	if n != len(replays)-1 {
		t.Fatalf("all but the winner replay, got %d replays", n)
	}
}

// TestRetiredRoutesAreGone pins the REST guideline renames: the old route must
// answer 404 or 405.
func TestRetiredRoutesAreGone(t *testing.T) {
	srv, _ := newKeyedServer(t)
	for _, rt := range []struct{ method, path string }{
		{http.MethodPost, "/capacity-plans/x/publish"},
	} {
		req, _ := http.NewRequest(rt.method, srv.URL+rt.path, strings.NewReader("{}"))
		req.Header.Set("Idempotency-Key", "k-gone")
		status, _, _ := doRequest(t, req)
		if status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 404/405", rt.method, rt.path, status)
		}
	}
}
