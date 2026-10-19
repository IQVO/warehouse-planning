package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Kafka down -> not ready, Postgres up. The broker address is one nobody
// listens on; the real broker check runs.
func TestNewReadiness_BrokerDownIsNotReady(t *testing.T) {
	g := newReadiness(func(context.Context) error { return nil }, []string{"127.0.0.1:1"})
	if got := g.Failing(context.Background()); !reflect.DeepEqual(got, []string{"kafka"}) {
		t.Fatalf("Failing = %v, want [kafka]", got)
	}
}

// Postgres down -> not ready, and it is named.
func TestNewReadiness_PostgresDownIsNamed(t *testing.T) {
	g := newReadiness(func(context.Context) error { return errors.New("connection refused") }, []string{"127.0.0.1:1"})
	got := g.Failing(context.Background())
	if !reflect.DeepEqual(got, []string{"postgres", "kafka"}) {
		t.Fatalf("Failing = %v, want [postgres kafka]", got)
	}
}
