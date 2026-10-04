package http

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// osGetenv is a thin wrapper kept so this package's tests never need to
// touch real process environment behavior beyond what os.Getenv already
// does -- here purely to keep the import list for corsAllowedOrigins
// obvious at a glance.
func osGetenv(key string) string { return os.Getenv(key) }

// splitAndTrim splits a comma-separated string and trims each entry.
func splitAndTrim(raw string) []string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}

// secondsToDuration converts a float seconds value (as carried over JSON)
// into a time.Duration.
func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// nonNilStrings returns s, or an empty (non-nil) slice, so a list field
// always serializes as [] and never null.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// parseOptionalFloatQueryParam parses a query parameter as *float64,
// returning (nil, nil) when the parameter is absent/empty -- the caller
// distinguishes "not provided" from "provided but malformed".
func parseOptionalFloatQueryParam(raw string) (*float64, error) {
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, err
	}
	return &v, nil
}
