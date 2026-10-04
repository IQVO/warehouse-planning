package main

import (
	"context"
	"errors"
	"testing"
)

func TestRun_FailsFastWithoutTheAnalyticalDatabase(t *testing.T) {
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	if err := run(); !errors.Is(err, errMissingAnalyticsURL) {
		t.Fatalf("run = %v", err)
	}
}

func TestGetenv(t *testing.T) {
	t.Setenv("PLANNING_REPORTS_TEST_SET", "v")
	if got := getenv("PLANNING_REPORTS_TEST_SET", "d"); got != "v" {
		t.Errorf("set = %q", got)
	}
	if got := getenv("PLANNING_REPORTS_TEST_UNSET", "d"); got != "d" {
		t.Errorf("unset = %q", got)
	}
}

func TestNewLogger_LevelMapping(t *testing.T) {
	if !newLogger("debug").Enabled(context.Background(), -4) || newLogger("info").Enabled(context.Background(), -4) || newLogger("").Enabled(context.Background(), -4) {
		t.Error("debug/info mapping is wrong")
	}
	if newLogger("warn").Enabled(context.Background(), 0) || !newLogger("warning").Enabled(context.Background(), 4) || newLogger("error").Enabled(context.Background(), 4) {
		t.Error("warn/error mapping is wrong")
	}
}
