//go:build integration

package postgres_test

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

var upMigrationRx = regexp.MustCompile(`^(\d{4})_.+\.up\.sql$`)

// latestMigrationVersion is the highest NNNN of the *.up.sql files in dir, so
// a migration test that walks down from the top never has to be edited when a
// new migration lands.
func latestMigrationVersion(t *testing.T, dir string) uint {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var latest uint
	for _, e := range entries {
		m := upMigrationRx.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseUint(m[1], 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		latest = max(latest, uint(n))
	}
	if latest == 0 {
		t.Fatalf("no NNNN_*.up.sql migration in %s", dir)
	}
	return latest
}
