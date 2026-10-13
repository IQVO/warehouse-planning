#!/usr/bin/env bash
# Chart render assertions (gap fixes D12, D14 and D21, 2026-10-12).
# Run: bash charts/warehouse-planning/tests/render_test.sh   (needs helm)
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=warehouse-planning
# Dummy DSNs, no password: the chart refuses to render without a database source.
base=(--set database.url=postgres://u@example.invalid:5432/db)
analytics=(--set kafka.enabled=true --set analytics.enabled=true
  --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a
  --set analytics.database.reportsUrl=postgres://reports@example.invalid:5432/a)

fail() { echo "FAIL: $*" >&2; exit 1; }

configmap="$(helm template "$release" "$chart_dir" "${base[@]}" --show-only templates/configmap.yaml)"
api_deployment="$(helm template "$release" "$chart_dir" "${base[@]}" --show-only templates/deployment.yaml)"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"
[[ -n "$configmap_name" ]] || fail "could not read the ConfigMap name"

# D14: every config.* value is rendered into the chart ConfigMap, and the api
# Deployment -- the only workload whose binary (cmd/api) reads those keys
# (HTTP_ADDR, LOG_LEVEL, EVENT_PUBLISHER, the consumer groups,
# OUTBOX_RELAY_INTERVAL, OUTBOX_MAX_ATTEMPTS, SHUTDOWN_DRAIN_DELAY,
# CORS_ALLOWED_ORIGINS, KAFKA_BROKERS) -- must mount it with envFrom, or a
# value that is only in the ConfigMap silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config.* value reaches the ConfigMap the api mounts.
tuned="$(helm template "$release" "$chart_dir" "${base[@]}" --set config.outboxRelayInterval=7s \
  --set config.outboxMaxAttempts=5 --set config.shutdownDrainDelay=9s --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_RELAY_INTERVAL: "7s"' <<<"$tuned" || fail "config.outboxRelayInterval is not rendered into the ConfigMap"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"
grep -q 'SHUTDOWN_DRAIN_DELAY: "9s"' <<<"$tuned" || fail "config.shutdownDrainDelay is not rendered into the ConfigMap"

# D12 / ADR 0013: the relay attempt cap defaults to 20 and 0 (unlimited) is
# rendered as is.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 (ADR 0013)"
grep -A1 'name: OUTBOX_MAX_ATTEMPTS' <<<"$api_deployment" | grep -q 'value: "20"' \
  || fail "api Deployment does not set OUTBOX_MAX_ATTEMPTS"
unlimited="$(helm template "$release" "$chart_dir" "${base[@]}" --set config.outboxMaxAttempts=0 --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "0"' <<<"$unlimited" || fail "config.outboxMaxAttempts=0 is not rendered"

# The other workloads read their own explicit env and must NOT inherit the
# api's ConfigMap (reports would bind the api's HTTP_ADDR, mcp and the
# projector have their own listen addresses).
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml; do
  if helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# D21 / ADR 0015: every backend Deployment gets ENVIRONMENT
# (deployment.environment.name), SERVICE_VERSION and, with otel.enabled, the
# OTLP endpoint; otel.enabled=false drops the endpoint.
for template in deployment.yaml mcp-deployment.yaml projector-deployment.yaml reports-deployment.yaml; do
  rendered="$(helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true \
    --set environment=staging --set image.tag=1.2.3 --show-only "templates/$template")"
  grep -A1 'name: ENVIRONMENT' <<<"$rendered" | grep -q 'value: "staging"' || fail "$template does not set ENVIRONMENT from .Values.environment"
  grep -A1 'name: SERVICE_VERSION' <<<"$rendered" | grep -q 'value: "1.2.3"' || fail "$template does not set SERVICE_VERSION from image.tag"
  grep -A1 'name: OTEL_EXPORTER_OTLP_ENDPOINT' <<<"$rendered" \
    | grep -q 'value: "otel-collector.observability.svc.cluster.local:4317"' \
    || fail "$template does not set OTEL_EXPORTER_OTLP_ENDPOINT with otel.enabled"
  if helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true --set otel.enabled=false \
    --show-only "templates/$template" | grep -q 'name: OTEL_EXPORTER_OTLP_ENDPOINT'; then
    fail "$template renders OTEL_EXPORTER_OTLP_ENDPOINT with otel.enabled=false"
  fi
done

# SHUTDOWN_DRAIN_DELAY: only cmd/api reads it; rendered there when set,
# absent by default (the binary's 5s default applies).
grep -q 'name: SHUTDOWN_DRAIN_DELAY' <<<"$api_deployment" && fail "SHUTDOWN_DRAIN_DELAY must not be rendered while empty"
drain="$(helm template "$release" "$chart_dir" "${base[@]}" --set config.shutdownDrainDelay=9s --show-only templates/deployment.yaml)"
grep -A1 'name: SHUTDOWN_DRAIN_DELAY' <<<"$drain" | grep -q 'value: "9s"' || fail "api Deployment does not set SHUTDOWN_DRAIN_DELAY"

echo "PASS: warehouse-planning chart render assertions"
