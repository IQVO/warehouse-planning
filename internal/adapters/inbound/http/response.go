package http

import (
	"encoding/json"
	"net/http"
)

// statusKey is the JSON key of the minimal {"status": "..."} health payloads.
const statusKey = "status"

// writeJSON encodes v as the response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON decodes r's body into dest, writing a 400 problem+json
// response and returning false on malformed JSON.
func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeProblem(w, http.StatusBadRequest, problemMalformedJSON, err.Error(), r.URL.Path)
		return false
	}
	return true
}

// writeError maps a typed domain/application error to its RFC 7807
// problem+json response.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

// problemDetail is the RFC 7807 application/problem+json body.
type problemDetail struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

// writeProblem writes an RFC 7807 application/problem+json response. When w
// is the idempotency middleware's recorder it also marks the replay class of
// the problem being answered (docs/adr/0026): that mark, and nothing else,
// decides whether the buffered refusal is stored (FINAL, OUTCOME) or released
// (RETRYABLE, and the fallback for any unmarked writer).
func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	if rec, ok := w.(*recorder); ok {
		rec.class = info.class
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetail{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}
