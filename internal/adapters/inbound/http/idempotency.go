package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/domain/idempotency"
)

const (
	// IdempotencyKeyHeader is the request header carrying the client's key.
	IdempotencyKeyHeader = "Idempotency-Key"
	// IdempotencyReplayedHeader is set (to "true") on a replayed answer only.
	IdempotencyReplayedHeader = "Idempotency-Replayed"

	maxIdempotencyKeyLen = 128
)

// errServerAnswer aborts the unit of work so a 5xx answer rolls back with the
// aggregate change and is never stored: the retry re-executes.
var errServerAnswer = errors.New("5xx answers are not stored")

// idempotent wraps a POST/PATCH handler (fleet REST API guideline section 2).
// The key row, the aggregate change and the outbox rows commit in ONE unit of
// work. A Server without UnitOfWork/Idempotency (unit-test servers; cmd/api
// always wires both) runs the handler unwrapped.
func (s *Server) idempotent(next http.HandlerFunc) http.HandlerFunc {
	if s.UnitOfWork == nil || s.Idempotency == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "Request body could not be read"}, err.Error(), r.URL.Path)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		sum := sha256.Sum256(body)
		fingerprint := r.Method + " " + r.URL.Path + " " + hex.EncodeToString(sum[:])

		rec := &recorder{header: http.Header{}, status: http.StatusOK}
		replayed := false
		doErr := s.UnitOfWork.Do(r.Context(), func(ctx context.Context) error {
			var err error
			replayed, err = s.runKeyed(ctx, next, rec, r, key, fingerprint)
			return err
		})
		if doErr != nil && !errors.Is(doErr, errServerAnswer) {
			writeProblem(w, http.StatusInternalServerError, problemInfo{"internal-error", "An unexpected internal error occurred"}, "idempotency store failure", r.URL.Path)
			return
		}
		rec.flush(w, replayed)
	}
}

// runKeyed is the body of the unit of work: replay or reject a seen key, else
// execute next and store its deterministic answer. It reports whether the
// answer is a replay.
func (s *Server) runKeyed(ctx context.Context, next http.HandlerFunc, rec *recorder, r *http.Request, key, fingerprint string) (bool, error) {
	stored, err := s.Idempotency.Lock(ctx, key)
	if err != nil {
		return false, err
	}
	if stored != nil {
		if stored.Fingerprint != fingerprint {
			writeProblem(rec, http.StatusConflict, problemInfo{"idempotency-key-reused", "Idempotency-Key was already used with a different request"}, "the Idempotency-Key was first used with a different method, path or body", r.URL.Path)
			return false, nil
		}
		rec.load(stored)
		return true, nil
	}
	next(rec, r.WithContext(ctx))
	if rec.status >= http.StatusInternalServerError {
		return false, errServerAnswer
	}
	return false, s.Idempotency.Save(ctx, rec.record(key, fingerprint))
}

// idempotencyKey validates the header: absent -> 400 idempotency-key-required,
// not 1-128 printable ASCII (or repeated) -> 400 idempotency-key-invalid.
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	vals := r.Header.Values(IdempotencyKeyHeader)
	if len(vals) == 0 || (len(vals) == 1 && vals[0] == "") {
		writeProblem(w, http.StatusBadRequest, problemInfo{"idempotency-key-required", "The Idempotency-Key header is required"}, "send an Idempotency-Key header (1-128 printable ASCII characters, e.g. a UUID)", r.URL.Path)
		return "", false
	}
	if len(vals) > 1 || !validIdempotencyKey(vals[0]) {
		writeProblem(w, http.StatusBadRequest, problemInfo{"idempotency-key-invalid", "The Idempotency-Key header is malformed"}, "the Idempotency-Key must be a single value of 1-128 printable ASCII characters", r.URL.Path)
		return "", false
	}
	return vals[0], true
}

func validIdempotencyKey(k string) bool {
	if len(k) < 1 || len(k) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

// recorder buffers a handler's answer so it can be stored before it is sent.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) WriteHeader(status int)      { r.status = status }
func (r *recorder) Write(p []byte) (int, error) { return r.body.Write(p) }

func (r *recorder) record(key, fingerprint string) idempotency.Record {
	return idempotency.Record{
		Key: key, Fingerprint: fingerprint, Status: r.status,
		ContentType: r.header.Get("Content-Type"),
		Location:    r.header.Get("Location"),
		ETag:        r.header.Get("ETag"),
		Body:        append([]byte(nil), r.body.Bytes()...),
	}
}

func (r *recorder) load(rec *idempotency.Record) {
	r.status = rec.Status
	r.header = http.Header{}
	for k, v := range map[string]string{"Content-Type": rec.ContentType, "Location": rec.Location, "ETag": rec.ETag} {
		if v != "" {
			r.header.Set(k, v)
		}
	}
	r.body.Reset()
	r.body.Write(rec.Body)
}

func (r *recorder) flush(w http.ResponseWriter, replayed bool) {
	for k, v := range r.header {
		w.Header()[k] = v
	}
	if replayed {
		w.Header().Set(IdempotencyReplayedHeader, "true")
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body.Bytes())
}
