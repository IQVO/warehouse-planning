// Package idempotency holds the stored answer to an Idempotency-Key as a plain
// value; keeping it outside application/ports lets that package stay
// interfaces-only.
package idempotency

// Record is the answer first given to an Idempotency-Key: the request
// fingerprint (method + path + sha256(body)) it was computed for and the
// response to replay.
type Record struct {
	Key         string
	Fingerprint string
	Status      int
	ContentType string
	Location    string
	ETag        string
	Body        []byte
}
