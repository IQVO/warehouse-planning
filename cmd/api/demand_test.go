package main

import (
	"testing"
)

func TestDemandConfigFromEnv(t *testing.T) {
	cases := []struct {
		name        string
		group, site string
		wantEnabled bool
		wantErr     bool
	}{
		{"unset group is OFF whatever the site", "", "", false, false},
		{"a site alone does not enable it", "", "SIM1", false, false},
		{"group and site enable it", "warehouse-planning-demand", "SIM1", true, false},
		{"a group without a site is a configuration error", "warehouse-planning-demand", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envDemandConsumerGroup, tc.group)
			t.Setenv(envDemandSiteID, tc.site)
			cfg, enabled, err := demandConfigFromEnv()
			if (err != nil) != tc.wantErr || enabled != tc.wantEnabled {
				t.Fatalf("enabled=%v err=%v, want enabled=%v err=%v", enabled, err, tc.wantEnabled, tc.wantErr)
			}
			if enabled && (cfg.group != tc.group || cfg.siteID != tc.site) {
				t.Fatalf("cfg = %+v", cfg)
			}
		})
	}
}

// Unset DEMAND_CONSUMER_GROUP is the off switch: no consumer, nothing to
// close, no error -- the service behaves as before the read model existed.
func TestStartDemandConsumer_OffByDefault(t *testing.T) {
	t.Setenv(envDemandConsumerGroup, "")
	t.Setenv("KAFKA_BROKERS", "broker.invalid:9092")
	consumers, closeFn, err := startDemandConsumer(adapters{}, quietLogger())
	if err != nil || len(consumers) != 0 {
		t.Fatalf("consumers=%d err=%v, want none", len(consumers), err)
	}
	closeFn()
}

func TestStartDemandConsumer_WithoutBrokersStaysOffEvenWhenConfigured(t *testing.T) {
	t.Setenv(envDemandConsumerGroup, "warehouse-planning-demand")
	t.Setenv(envDemandSiteID, "SIM1")
	t.Setenv("KAFKA_BROKERS", "")
	consumers, closeFn, err := startDemandConsumer(adapters{}, quietLogger())
	if err != nil || len(consumers) != 0 {
		t.Fatalf("consumers=%d err=%v, want none (disabled, not fatal)", len(consumers), err)
	}
	closeFn()
}

func TestStartDemandConsumer_GroupWithoutSiteRefusesToStart(t *testing.T) {
	t.Setenv(envDemandConsumerGroup, "warehouse-planning-demand")
	t.Setenv(envDemandSiteID, "")
	t.Setenv("KAFKA_BROKERS", "broker.invalid:9092")
	if _, _, err := startDemandConsumer(adapters{}, quietLogger()); err == nil {
		t.Fatal("a consumer group without a site must be a boot error, not demand attributed to nowhere")
	}
}

func TestJoinClosersRunsEveryCloserInOrder(t *testing.T) {
	var got []int
	joinClosers(func() { got = append(got, 1) }, func() { got = append(got, 2) })()
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v", got)
	}
}
