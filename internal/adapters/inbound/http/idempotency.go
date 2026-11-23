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

// errRetryableAnswer aborts the unit of work so a RETRYABLE (or unclassified)
// refusal rolls back exactly like a 5xx: nothing is stored, the key row rolls
// back with it and the transaction-scoped advisory lock releases, so the same
// key may be tried again (fleet replay-class rule, docs/adr/0026).
var errRetryableAnswer = errors.New("retryable refusals are not stored")

// errAbortedSuccess fails a 2xx answer whose writes died with an aborted
// transaction: it must be neither returned nor cached.
var errAbortedSuccess = errors.New("success answer over an aborted transaction")

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
			writeProblem(w, http.StatusBadRequest, problemMalformedRequestBody, err.Error(), r.URL.Path)
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
		if doErr != nil && !errors.Is(doErr, errServerAnswer) && !errors.Is(doErr, errRetryableAnswer) {
			writeProblem(w, http.StatusInternalServerError, problemInternalError, "idempotency store failure", r.URL.Path)
			return
		}
		rec.flush(w, replayed)
	}
}

// runKeyed is the body of the unit of work: replay or reject a seen key, else
// execute next and store its deterministic answer. It reports whether the
// answer is a replay.
//
// The handler runs behind a SAVEPOINT. A mapped domain conflict (a racing
// request losing on a unique index, 23505 -> 409) fails a statement on the
// server and leaves the shared transaction ABORTED even though the handler
// answers a truthful 4xx; recording that outcome would then hit 25P02 and the
// client would see a 500. Releasing the savepoint exposes the abort: the
// store rolls back to the savepoint, discarding only the handler's partial
// writes and healing the transaction, and the 4xx outcome is recorded and
// committed like any other. A 2xx answer over an aborted transaction is a lie
// (its writes are gone), so the unit of work fails (500, nothing cached) and
// the same key retries the real work.
//
// A 4xx refusal is stored only when its problem type's replay class says so
// (docs/adr/0026): FINAL and OUTCOME answers are recorded exactly as before;
// a RETRYABLE answer -- and, as a safe fallback, any 4xx the production
// writer did not mark with a class -- returns errRetryableAnswer, which rolls
// the unit of work back (handler writes, key row and the transaction-scoped
// advisory lock with it) so the key is free and a retry executes again.
func (s *Server) runKeyed(ctx context.Context, next http.HandlerFunc, rec *recorder, r *http.Request, key, fingerprint string) (bool, error) {
	stored, err := s.Idempotency.Lock(ctx, key)
	if err != nil {
		return false, err
	}
	if stored != nil {
		if stored.Fingerprint != fingerprint {
			writeProblem(rec, http.StatusConflict, problemIdempotencyKeyReused, "the Idempotency-Key was first used with a different method, path or body", r.URL.Path)
			return false, nil
		}
		rec.load(stored)
		return true, nil
	}
	if err := s.Idempotency.Savepoint(ctx); err != nil {
		return false, err
	}
	next(rec, r.WithContext(ctx))
	if rec.status >= http.StatusInternalServerError {
		return false, errServerAnswer
	}
	if rec.status >= http.StatusBadRequest && !rec.storesAnswer() {
		return false, errRetryableAnswer
	}
	aborted, err := s.Idempotency.ReleaseSavepoint(ctx)
	if err != nil {
		return false, err
	}
	if aborted && rec.status < http.StatusBadRequest {
		return false, errAbortedSuccess
	}
	return false, s.Idempotency.Save(ctx, rec.record(key, fingerprint))
}

// idempotencyKey validates the header: absent -> 400 idempotency-key-required,
// not 1-128 printable ASCII (or repeated) -> 400 idempotency-key-invalid.
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	vals := r.Header.Values(IdempotencyKeyHeader)
	if len(vals) == 0 || (len(vals) == 1 && vals[0] == "") {
		writeProblem(w, http.StatusBadRequest, problemIdempotencyKeyRequired, "send an Idempotency-Key header (1-128 printable ASCII characters, e.g. a UUID)", r.URL.Path)
		return "", false
	}
	if len(vals) > 1 || !validIdempotencyKey(vals[0]) {
		writeProblem(w, http.StatusBadRequest, problemIdempotencyKeyInvalid, "the Idempotency-Key must be a single value of 1-128 printable ASCII characters", r.URL.Path)
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
// The production problem writer marks class on it (writeProblem), which is how
// the middleware learns the replay class of a refusal without holding any list
// of slugs: an unmarked 4xx is treated as RETRYABLE (released, not stored).
type recorder struct {
	header http.Header
	status int
	class  replayClass
	body   bytes.Buffer
}

// storesAnswer reports whether the buffered 4xx answer is stored under the
// key: FINAL and OUTCOME refusals are; an empty class (a writer that bypassed
// the catalogue) is the RETRYABLE fallback.
func (r *recorder) storesAnswer() bool {
	return r.class == replayFinal || r.class == replayOutcome
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
