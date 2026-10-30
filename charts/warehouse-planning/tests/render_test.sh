#!/usr/bin/env bash
# Chart render assertions (gap fixes D12, D14 and D21, 2026-10-12).
# Run: bash charts/warehouse-planning/tests/render_test.sh   (needs helm)
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=warehouse-planning
# Dummy DSNs, no password. The chart refuses to render without the real
# runtime's mandatory values (ADR 0016): a database source, the Kafka brokers,
# every consumer group and the demand site.
base=(--set database.url=postgres://u@example.invalid:5432/db
  --set kafka.brokers=kafka.example.invalid:9092
  --set config.laborCapacityConsumerGroup=g-labor --set config.storageCapacityConsumerGroup=g-storage
  --set demand.consumerGroup=g-demand --set demand.siteId=SIM1
  --set inbound.consumerGroup=g-inbound --set observed.consumerGroup=g-observed)
analytics=(--set analytics.enabled=true --set analytics.projector.consumerGroup=g-analytics
  --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a
  --set analytics.database.reportsUrl=postgres://reports@example.invalid:5432/a)

repo_dir="$(cd "$chart_dir/../.." && pwd)"

fail() { echo "FAIL: $*" >&2; exit 1; }

# env_value <rendered yaml> <VAR>: prints the value of the env entry `- name: VAR`
# (quotes stripped); exits non-zero when the variable is not rendered.
env_value() {
  awk -v n="$2" '$1 == "-" && $2 == "name:" && $3 == n { getline; sub(/^ *value: */, ""); gsub(/"/, ""); print; found = 1; exit } END { exit !found }' <<<"$1"
}

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
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml a2a-deployment.yaml; do
  if helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true --set a2a.enabled=true --show-only "templates/$template" | grep -q 'configMapRef:'; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# D21 / ADR 0015: every backend Deployment gets ENVIRONMENT
# (deployment.environment.name), SERVICE_VERSION and, with otel.enabled, the
# OTLP endpoint; otel.enabled=false drops the endpoint.
for template in deployment.yaml mcp-deployment.yaml a2a-deployment.yaml projector-deployment.yaml reports-deployment.yaml; do
  rendered="$(helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true --set a2a.enabled=true \
    --set environment=staging --set image.tag=1.2.3 --show-only "templates/$template")"
  grep -A1 'name: ENVIRONMENT' <<<"$rendered" | grep -q 'value: "staging"' || fail "$template does not set ENVIRONMENT from .Values.environment"
  grep -A1 'name: SERVICE_VERSION' <<<"$rendered" | grep -q 'value: "1.2.3"' || fail "$template does not set SERVICE_VERSION from image.tag"
  grep -A1 'name: OTEL_EXPORTER_OTLP_ENDPOINT' <<<"$rendered" \
    | grep -q 'value: "otel-collector.observability.svc.cluster.local:4317"' \
    || fail "$template does not set OTEL_EXPORTER_OTLP_ENDPOINT with otel.enabled"
  if helm template "$release" "$chart_dir" "${base[@]}" "${analytics[@]}" --set mcp.enabled=true --set a2a.enabled=true --set otel.enabled=false \
    --show-only "templates/$template" | grep -q 'name: OTEL_EXPORTER_OTLP_ENDPOINT'; then
    fail "$template renders OTEL_EXPORTER_OTLP_ENDPOINT with otel.enabled=false"
  fi
done

# SHUTDOWN_DRAIN_DELAY: only cmd/api reads it; rendered there when set,
# absent by default (the binary's 5s default applies).
grep -q 'name: SHUTDOWN_DRAIN_DELAY' <<<"$api_deployment" && fail "SHUTDOWN_DRAIN_DELAY must not be rendered while empty"
drain="$(helm template "$release" "$chart_dir" "${base[@]}" --set config.shutdownDrainDelay=9s --show-only templates/deployment.yaml)"
grep -A1 'name: SHUTDOWN_DRAIN_DELAY' <<<"$drain" | grep -q 'value: "9s"' || fail "api Deployment does not set SHUTDOWN_DRAIN_DELAY"

# ADR 0016: Postgres and Kafka are the only runtime. Each mandatory value is
# refused at render time with a message naming it; nothing silently degrades.
refuses() { # <needle> <helm args...>
  local needle="$1"; shift
  local out
  if out="$(helm template "$release" "$chart_dir" "$@" 2>&1)"; then
    fail "the chart rendered although it must refuse ($needle)"
  fi
  grep -q -- "$needle" <<<"$out" || fail "the render was refused for another reason than '$needle': $out"
}
without() { # drop one mandatory --set from base
  local drop="$1" out=() i=0
  while ((i < ${#base[@]})); do
    if [[ "${base[i]}" == "--set" && "${base[i+1]}" == "$drop="* ]]; then i=$((i + 2)); continue; fi
    out+=("${base[i]}"); i=$((i + 1))
  done
  printf '%s\n' "${out[@]}"
}
refuse_without() { # <value path> <needle>
  local args=()
  while IFS= read -r line; do args+=("$line"); done < <(without "$1")
  refuses "$2" "${args[@]}"
}
refuse_without database.url "requires database.url or database.existingSecret"
refuse_without kafka.brokers "requires kafka.brokers to be set (KAFKA_BROKERS"
refuse_without config.laborCapacityConsumerGroup "LABOR_CAPACITY_CONSUMER_GROUP"
refuse_without config.storageCapacityConsumerGroup "STORAGE_CAPACITY_CONSUMER_GROUP"
refuse_without demand.consumerGroup "DEMAND_CONSUMER_GROUP"
refuse_without demand.siteId "DEMAND_SITE_ID"
refuse_without inbound.consumerGroup "INBOUND_DEMAND_CONSUMER_GROUP"
refuse_without observed.consumerGroup "OBSERVED_CAPACITY_CONSUMER_GROUP"
refuses "kafka.enabled is false" "${base[@]}" --set kafka.enabled=false
refuses "the only publisher is" "${base[@]}" --set config.eventPublisher=log
refuses "sets EVENT_PUBLISHER=log" "${base[@]}" --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=log'
refuses "a2a.extraEnv sets EVENT_PUBLISHER=log" "${base[@]}" --set a2a.enabled=true --set 'a2a.extraEnv[0].name=EVENT_PUBLISHER' --set 'a2a.extraEnv[0].value=log'
refuses "terminationGracePeriodSeconds is 5" "${base[@]}" --set terminationGracePeriodSeconds=5
refuses "analytics.projector.consumerGroup is required" "${base[@]}" --set analytics.enabled=true \
  --set analytics.database.projectorUrl=postgres://projector@example.invalid:5432/a
# A legacy `kafka.enabled: true` (warehouse-infra still sets it) renders.
helm template "$release" "$chart_dir" "${base[@]}" --set kafka.enabled=true >/dev/null || fail "kafka.enabled=true must still render"

# ADR 0018: the A2A agent (cmd/a2a). Off by default; on, it runs /app/a2a on
# :8093 (8080 api, 8090 mcp, 8091 projector admin, 8092 reports), reads the same
# DATABASE_URL secret and advertises a dialable, absolute Agent Card URL.
a2a_dep="$(helm template "$release" "$chart_dir" "${base[@]}" --set a2a.enabled=true --show-only templates/a2a-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$a2a_dep" | grep -q 'secretKeyRef:' || fail "the a2a DATABASE_URL must come from a Secret"
[[ "$(env_value "$a2a_dep" A2A_ADDR || true)" == ":8093" ]] || fail "the a2a agent must listen on :8093 (8090 is mcp, 8091 the projector admin, 8092 the reports API)"
grep -q 'command: \["/app/a2a"\]' <<<"$a2a_dep" || fail "the a2a Deployment must run /app/a2a"
# The Agent Card must advertise a dialable absolute URL: it defaults to the
# release's in-cluster Service DNS (namespace-derived, so pattern-match) and
# stays overridable.
a2a_url="$(env_value "$a2a_dep" A2A_AGENT_URL || true)" # a missing var must reach the fail message below, not abort under set -e
[[ "$a2a_url" =~ ^http://[a-z0-9.-]+-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/$ ]] || fail "A2A_AGENT_URL default is not the in-cluster Service DNS: $a2a_url"
a2a_override="$(helm template "$release" "$chart_dir" "${base[@]}" --set a2a.enabled=true --set a2a.agentUrl=http://gw.example/api/a2a/warehouse-planning/ --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$a2a_override" A2A_AGENT_URL || true)" == "http://gw.example/api/a2a/warehouse-planning/" ]] || fail "a2a.agentUrl does not override A2A_AGENT_URL"
# Every variable cmd/a2a's boot config names as REQUIRED is rendered, derived
# from the Go source so a new mandatory variable fails until the chart has it.
a2a_vars="$(find "$repo_dir/cmd/a2a" -name '*.go' ! -name '*_test.go' -print0 | xargs -0 grep -hoE '[A-Z][A-Z0-9_]+ is required' | sed -E 's/ is required//' | sort -u || true)"
[[ -n "$a2a_vars" ]] || fail "found no mandatory variables for cmd/a2a; the scan is looking in the wrong place"
for var in $a2a_vars; do
  grep -q "name: $var\$" <<<"$a2a_dep" || fail "the a2a Deployment does not render $var, which cmd/a2a requires"
done
# A2A is off by default: no a2a resource renders unless asked for.
if helm template "$release" "$chart_dir" "${base[@]}" --show-only templates/a2a-deployment.yaml 2>/dev/null | grep -q 'kind: Deployment'; then fail "the a2a Deployment rendered with default values"; fi

# The api always gets the mandatory env, whatever the values (no conditional env).
for env in KAFKA_BROKERS DEMAND_CONSUMER_GROUP DEMAND_SITE_ID LABOR_CAPACITY_CONSUMER_GROUP STORAGE_CAPACITY_CONSUMER_GROUP INBOUND_DEMAND_CONSUMER_GROUP OBSERVED_CAPACITY_CONSUMER_GROUP EVENT_PUBLISHER DATABASE_URL; do
  grep -q "name: $env\$" <<<"$api_deployment" || fail "the api Deployment does not set $env"
done
grep -A1 'name: EVENT_PUBLISHER' <<<"$api_deployment" | grep -q 'value: "kafka"' || fail "EVENT_PUBLISHER must default to kafka"
# ADR 0017: the two new consumer groups come from their dedicated values.
grep -A1 'name: INBOUND_DEMAND_CONSUMER_GROUP' <<<"$api_deployment" | grep -q 'value: "g-inbound"' \
  || fail "INBOUND_DEMAND_CONSUMER_GROUP does not come from inbound.consumerGroup"
grep -A1 'name: OBSERVED_CAPACITY_CONSUMER_GROUP' <<<"$api_deployment" | grep -q 'value: "g-observed"' \
  || fail "OBSERVED_CAPACITY_CONSUMER_GROUP does not come from observed.consumerGroup"

# warehouse-infra's CURRENT values (verbatim copies, refreshed from its
# origin/develop) still render and provide every env the binaries require.
infra=(-f "$chart_dir/tests/warehouse-infra-platform-values.yaml" -f "$chart_dir/tests/warehouse-infra-values.yaml"
  -f "$chart_dir/tests/warehouse-infra-kind-resources-values.yaml" --set image.tag=local-test)
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)" \
  || fail "the chart does not render with warehouse-infra's current values"
for env in DATABASE_URL KAFKA_BROKERS EVENT_PUBLISHER LABOR_CAPACITY_CONSUMER_GROUP STORAGE_CAPACITY_CONSUMER_GROUP DEMAND_CONSUMER_GROUP DEMAND_SITE_ID INBOUND_DEMAND_CONSUMER_GROUP OBSERVED_CAPACITY_CONSUMER_GROUP; do
  grep -q "name: $env\$" <<<"$infra_api" || fail "warehouse-infra's values do not provide $env to the api"
done
grep -A1 'name: KAFKA_BROKERS' <<<"$infra_api" | grep -q 'value: "kafka.warehouse-systems.svc.cluster.local:9092"' \
  || fail "KAFKA_BROKERS does not come from the platform brokers"
infra_projector="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)" \
  || fail "the projector does not render with warehouse-infra's current values"
for env in ANALYTICS_DATABASE_URL KAFKA_BROKERS ANALYTICS_CONSUMER_GROUP; do
  grep -q "name: $env\$" <<<"$infra_projector" || fail "warehouse-infra's values do not provide $env to the projector"
done
infra_mcp="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/mcp-deployment.yaml)" \
  || fail "the MCP server does not render with warehouse-infra's current values"
grep -q 'name: DATABASE_URL$' <<<"$infra_mcp" || fail "warehouse-infra's values do not provide DATABASE_URL to the MCP server"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/reports-deployment.yaml >/dev/null \
  || fail "the reports reader does not render with warehouse-infra's current values"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-deployment.yaml >/dev/null \
  || fail "the A2A agent does not render with warehouse-infra's current values"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-route.yaml | grep -q "replacePrefixMatch: /$" \
  || fail "infra values: the a2a route must rewrite to /"

# /readyz is real on the api, the projector and the reports reader (probes).
grep -A2 'readinessProbe:' <<<"$infra_api" | grep -q 'path: /readyz' || fail "the api readinessProbe must be /readyz"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/reports-deployment.yaml \
  | grep -A3 'readinessProbe:' | grep -q 'path: /readyz' || fail "the reports readinessProbe must be /readyz"

echo "PASS: warehouse-planning chart render assertions"
