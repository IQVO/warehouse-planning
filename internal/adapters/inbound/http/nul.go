package http

import (
	"bytes"
	"io"
	"net/http"
	"strings"
)

// maxBodyBytes caps how much of a request body rejectNUL buffers to look for
// a NUL; a larger body is passed on untouched for the handler to refuse.
const maxBodyBytes = 1 << 20

// The problems rejectNUL answers with.
var (
	notFoundResource   = problemInfo{"resource-not-found", "Resource not found"}
	malformedBody      = problemInfo{"malformed-request-body", "Request body must not contain a NUL character"}
	malformedQueryNull = problemInfo{"malformed-request-query", "Query string must not contain a NUL character"}
)

// rejectNUL turns a NUL character in a request into a client error before it
// can reach a Postgres text parameter. PostgreSQL text cannot hold U+0000, and
// a statement that carries one aborts the surrounding transaction (SQLSTATE
// 22021, then 25P02 for the next statement): on the sibling services that
// became a 500 on the real runtime, hidden for as long as the in-memory
// adapters served the contract run (ADR 0016).
//
//   - a NUL in the URL path can never name a resource: 404 resource-not-found;
//   - a NUL in the query string (raw byte or %00) or in a JSON body (a raw
//     byte or the \u0000 escape) is a malformed request: 400.
func rejectNUL(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.ContainsRune(r.URL.Path, 0) || strings.ContainsRune(r.URL.RawPath, 0) || strings.Contains(r.URL.RawPath, "%00") {
			writeProblem(w, http.StatusNotFound, notFoundResource, "no resource matches the requested path", r.URL.Path)
			return
		}
		if strings.ContainsRune(r.URL.RawQuery, 0) || strings.Contains(r.URL.RawQuery, "%00") {
			writeProblem(w, http.StatusBadRequest, malformedQueryNull, "a NUL character (U+0000) is not allowed in the query string", r.URL.Path)
			return
		}
		if r.Body != nil && r.Body != http.NoBody {
			orig := r.Body
			body, err := io.ReadAll(io.LimitReader(orig, maxBodyBytes+1))
			if err != nil {
				_ = orig.Close()
				writeProblem(w, http.StatusBadRequest, malformedBody, "the request body could not be read", r.URL.Path)
				return
			}
			if containsNUL(body) {
				_ = orig.Close()
				writeProblem(w, http.StatusBadRequest, malformedBody, "a NUL character (U+0000) is not allowed in a request body", r.URL.Path)
				return
			}
			// Hand the handler everything: the buffered prefix, then the rest
			// of the original body (only non-empty above maxBodyBytes).
			r.Body = struct {
				io.Reader
				io.Closer
			}{io.MultiReader(bytes.NewReader(body), orig), orig}
		}
		next.ServeHTTP(w, r)
	})
}

// containsNUL reports whether a JSON document carries U+0000, either as a raw
// 0x00 byte or as the six-character escape \u0000. A backslash that is itself
// escaped (\\u0000, a literal backslash followed by the text u0000) is not a
// NUL: the escape only counts when it is preceded by an even number of
// backslashes.
func containsNUL(body []byte) bool {
	if bytes.IndexByte(body, 0) >= 0 {
		return true
	}
	const escape = `\u0000`
	for from := 0; ; {
		i := bytes.Index(body[from:], []byte(escape))
		if i < 0 {
			return false
		}
		i += from
		slashes := 0
		for j := i - 1; j >= 0 && body[j] == '\\'; j-- {
			slashes++
		}
		if slashes%2 == 0 {
			return true
		}
		from = i + len(escape)
	}
}
