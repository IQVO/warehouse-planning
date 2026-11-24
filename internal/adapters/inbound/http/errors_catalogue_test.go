package http_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
)

// adrTableRows parses the class table of docs/adr/0026 (| `slug` | status |
// class |) into the same shape as the catalogue accessor.
func adrTableRows(t *testing.T) map[[2]string]string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller info")
	}
	adr := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "docs", "adr", "0026-idempotency-replay-classes.md")
	raw, err := os.ReadFile(adr)
	if err != nil {
		t.Fatalf("read ADR 0026: %v", err)
	}
	rows := make(map[[2]string]string)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) != 7 || fields[0] != "|" || fields[2] != "|" || fields[4] != "|" || fields[6] != "|" {
			continue
		}
		slug := strings.Trim(fields[1], "`")
		status, class := fields[3], fields[5]
		if slug == "---" || slug == "Slug" {
			continue
		}
		var code int
		for _, c := range []struct {
			text string
			num  int
		}{
			{"400", http.StatusBadRequest}, {"404", http.StatusNotFound}, {"409", http.StatusConflict},
			{"422", http.StatusUnprocessableEntity}, {"500", http.StatusInternalServerError}, {"503", http.StatusServiceUnavailable},
		} {
			if status == c.text {
				code = c.num
			}
		}
		if code == 0 {
			t.Fatalf("ADR row with unknown status: %q", line)
		}
		rows[[2]string{slug, status}] = class
	}
	if len(rows) == 0 {
		t.Fatal("no class rows parsed from ADR 0026: table format drifted?")
	}
	return rows
}

// TestTheADRClassTableMatchesTheCatalogue holds docs/adr/0026's class table
// equal to the production catalogue, both directions: a problem type added or
// reclassified without the ADR (or vice versa) fails here.
func TestTheADRClassTableMatchesTheCatalogue(t *testing.T) {
	adr := adrTableRows(t)
	catalogue := inboundhttp.ExportProblemTypesForTest()
	if len(catalogue) == 0 {
		t.Fatal("the catalogue accessor returned no problem types")
	}
	for _, p := range catalogue {
		key := [2]string{p.Slug, statusNum(p.Status)}
		want, ok := adr[key]
		if !ok {
			t.Errorf("problem type %s (%s) is not in ADR 0026's table", p.Slug, key[1])
			continue
		}
		if want != p.Class {
			t.Errorf("problem type %s (%s): ADR says %s, catalogue says %s", p.Slug, key[1], want, p.Class)
		}
	}
	seen := make(map[[2]string]bool)
	for _, p := range catalogue {
		seen[[2]string{p.Slug, statusNum(p.Status)}] = true
	}
	for key, class := range adr {
		if !seen[key] {
			t.Errorf("ADR 0026 classifies %s (%s) as %s, but the catalogue has no such problem type", key[0], key[1], class)
		}
	}
}

// statusNum maps an HTTP status to its three-digit text (the ADR table's form).
func statusNum(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "400"
	case http.StatusNotFound:
		return "404"
	case http.StatusConflict:
		return "409"
	case http.StatusUnprocessableEntity:
		return "422"
	case http.StatusInternalServerError:
		return "500"
	case http.StatusServiceUnavailable:
		return "503"
	}
	return "other"
}

// TestEveryCatalogueEntryDeclaresAReplayClass is the unit twin of the
// integration template's first test: an entry without a class (or with a
// misspelt one) fails, so a new problem type is classified by the PR that
// adds it.
func TestEveryCatalogueEntryDeclaresAReplayClass(t *testing.T) {
	for _, p := range inboundhttp.ExportProblemTypesForTest() {
		switch p.Class {
		case "FINAL", "OUTCOME", "RETRYABLE":
		default:
			t.Errorf("problem type %q (%d) declares no replay class (%q)", p.Slug, p.Status, p.Class)
		}
		if p.Status >= 500 && p.Class != "RETRYABLE" {
			t.Errorf("problem type %q answers %d: a 5xx is never stored, so its class is RETRYABLE, got %q", p.Slug, p.Status, p.Class)
		}
	}
}

// TestProblemInfoLiteralsLiveInErrorsGo walks the package's non-test sources
// and refuses any problemInfo composite literal outside errors.go: a problem
// answered around the catalogue has no class and would be released as the
// RETRYABLE fallback without anyone deciding that.
func TestProblemInfoLiteralsLiveInErrorsGo(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller info")
	}
	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "errors.go" {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if ident, ok := lit.Type.(*ast.Ident); ok && ident.Name == "problemInfo" {
				t.Errorf("%s: a problemInfo literal outside errors.go has no catalogue entry (and no replay class); register it in errors.go", name)
			}
			return true
		})
	}
}

// keyedMemoryServer builds the middleware over the memory store (the unit-test
// double) with a custom handler.
func keyedMemoryServer(t *testing.T, next http.HandlerFunc) http.HandlerFunc {
	t.Helper()
	s := &inboundhttp.Server{UnitOfWork: memory.NewUnitOfWork(), Idempotency: memory.NewIdempotencyStore()}
	return s.ExportIdempotent(next)
}

func keyedUnitPost(h http.HandlerFunc, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/capacity-plans", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

// TestRetryableRefusalIsNotStoredAndTheRetryExecutes: a refusal whose problem
// type is RETRYABLE leaves no key row (memory store), and the same key
// executes again — here succeeding and then replaying.
func TestRetryableRefusalIsNotStoredAndTheRetryExecutes(t *testing.T) {
	calls := 0
	var h http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			inboundhttp.ExportRefuseForTest(w, r, "process-path-not-found")
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}
	mw := keyedMemoryServer(t, h)

	first := keyedUnitPost(mw, "k-retry-unit")
	if first.Code != http.StatusNotFound || !strings.Contains(first.Body.String(), "process-path-not-found") {
		t.Fatalf("first = %d %s, want 404 process-path-not-found", first.Code, first.Body.String())
	}
	retry := keyedUnitPost(mw, "k-retry-unit")
	if retry.Code != http.StatusCreated || retry.Header().Get("Idempotency-Replayed") != "" || calls != 2 {
		t.Fatalf("retry = %d replayed=%q calls=%d, want a fresh 201 after 2 calls", retry.Code, retry.Header().Get("Idempotency-Replayed"), calls)
	}
	replay := keyedUnitPost(mw, "k-retry-unit")
	if replay.Code != http.StatusCreated || replay.Header().Get("Idempotency-Replayed") != "true" || calls != 2 {
		t.Fatalf("replay = %d replayed=%q calls=%d, want the stored 201 replayed", replay.Code, replay.Header().Get("Idempotency-Replayed"), calls)
	}
}

// TestUnclassifiedWritten4xxIsReleasedLikeRetryable: a 4xx written around the
// production writer (no class mark) is released — the documented fallback — so
// the key stays free and the retry executes.
func TestUnclassifiedWritten4xxIsReleasedLikeRetryable(t *testing.T) {
	calls := 0
	var h http.HandlerFunc = func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{{"type":"https://errors.example/hand-made","status":409}}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
	mw := keyedMemoryServer(t, h)

	first := keyedUnitPost(mw, "k-unclassified")
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d, want the hand-written 409", first.Code)
	}
	retry := keyedUnitPost(mw, "k-unclassified")
	if retry.Code != http.StatusCreated || calls != 2 {
		t.Fatalf("retry = %d calls=%d, want the key released and the retry executed", retry.Code, calls)
	}
}
