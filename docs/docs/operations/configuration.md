---
id: configuration
title: Configuration
sidebar_label: Configuration
---

# Configuration

Every binary is configured only through environment variables. There are no
config files and no command-line flags. This page lists each variable each
binary reads, taken from its composition root (`cmd/<binary>/main.go`) and the
few adapters that read the environment themselves. An empty value counts as
unset: every lookup goes through a `getenv(key, fallback)` helper or an
explicit `== ""` check.

**Postgres and Kafka are the only runtime** ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)).
A variable marked **yes** under *Required* is a boot error that names it when it
is unset, raised before the listener binds; there is no in-memory fallback, no
log sink and no consumer that is off. The chart refuses to render without the
matching values (`helm template` names the value).

The Helm chart (`charts/warehouse-planning`) renders most variables from
dedicated values. The **Chart value** column names the key. "not rendered"
means the chart never sets the variable, so the binary's default applies
unless you add it through `extraEnv` (the `api` component), `mcp.extraEnv`, or
a values override in `warehouse-infra`.

Every `config.*` value is also rendered into the chart ConfigMap
(`<release>-warehouse-planning`), and the `api` Deployment mounts that
ConfigMap with `envFrom` (gap fix D14), so a key that only lives in the
ConfigMap still reaches `cmd/api`; an explicit `env` entry wins over it. The
`mcp`, `analytics-projector` and `analytics-reports` Deployments do **not**
mount it: they read only their own explicit `env` (the reports binary would
otherwise bind the api's `HTTP_ADDR`). `charts/warehouse-planning/tests/render_test.sh`
asserts both, in CI (`chart-tests` job).

The OTel environment is rendered on all four backend Deployments
([ADR 0015](/docs/adr/0015-projector-telemetry-and-chart-otel-env)):
`ENVIRONMENT` from `environment` (default `local`), `SERVICE_VERSION` from
`image.tag` (falling back to the chart `appVersion`) and
`OTEL_EXPORTER_OTLP_ENDPOINT` from `otel.endpoint` (default
`otel-collector.observability.svc.cluster.local:4317`, the in-cluster
Collector) while `otel.enabled` (default `true`). `warehouse-infra` sets none
of them for this service, so the chart defaults apply in the kind cluster.

## `cmd/api` (REST, Kafka consumers, outbox relay)

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST server. | `config.httpAddr` | `cmd/api/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` (or `warning`) or `error`, case-insensitive. Any other value means `info`. Logs are JSON on stdout. | `config.logLevel` | `cmd/api/main.go` |
| `DATABASE_URL` | none | **yes** | Postgres DSN of the OLTP database (pgxpool). Unset is a boot error (`DATABASE_URL is required: ...`); there is no in-memory mode ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)). The chart refuses to render without `database.url` or `database.existingSecret`. | `database.url` / `database.existingSecret` + `database.existingSecretKey` | `cmd/api/runtime.go` |
| `MIGRATIONS_DATABASE_URL` | the value of `DATABASE_URL` | no | DSN used **only** by the golang-migrate step at boot. In the kind cluster `DATABASE_URL` goes through PgBouncer (transaction pooling), which cannot hold golang-migrate's session-scoped `pg_advisory_lock`, so this one is a direct Postgres DSN ([ADR 0010](/docs/adr/0010-migrations-over-direct-connection)). The runtime pool never uses it. | `database.migrationsExistingSecretKey` (rendered with `optional: true`) | `cmd/api/main.go` |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | Directory of the OLTP `*.up.sql` files, relative to the working directory. The image copies them to `/app/migrations`. | `config.migrationsPath` (`migrations`) | `cmd/api/main.go` |
| `KAFKA_BROKERS` | none | **yes** | Comma-separated broker list (each entry trimmed, empty entries dropped). Unset or blank is a boot error (`KAFKA_BROKERS is required: ...`). It feeds the three consumers, the outbox relay and the `/readyz` broker check. | `kafka.brokers` (required; a legacy `kafka.enabled: true` still renders, `false` is refused) | `cmd/api/runtime.go` |
| `LABOR_CAPACITY_CONSUMER_GROUP` | none | **yes** | Consumer group of the labor consumer on `warehouse.workforce.events`. No default on purpose, so that a local process never joins the live group; unset is a boot error. | `config.laborCapacityConsumerGroup` (required) | `cmd/api/runtime.go` |
| `STORAGE_CAPACITY_CONSUMER_GROUP` | none | **yes** | Consumer group of the facility consumer on `warehouse.facility.events`. No default; unset is a boot error. | `config.storageCapacityConsumerGroup` (required) | `cmd/api/runtime.go` |
| `DEMAND_CONSUMER_GROUP` | none | **yes** | Consumer group of the order-demand consumer on `warehouse.order-management.events`. The consumer is always on ([ADR 0004](/docs/adr/0004-demand-ingestion-from-order-management) made it opt-in; [ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime) removed the off switch). No default; unset is a boot error. | `demand.consumerGroup` (required) | `cmd/api/runtime.go` |
| `DEMAND_SITE_ID` | none | **yes** | The one site every consumed order, dock appointment and task completion is attributed to, because those events carry no site. Unset is a boot error. | `demand.siteId` (required) | `cmd/api/runtime.go` |
| `INBOUND_DEMAND_CONSUMER_GROUP` | none | **yes** | Consumer group of the dock-appointment consumer on `warehouse.inbound-receiving.events` (`DockAppointmentBooked`, `DockAppointmentCancelled`; [ADR 0017](/docs/adr/0017-inbound-labour-demand-and-observed-capacity-feedback)). Always on ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)); no default; unset is a boot error. | `inbound.consumerGroup` (required; warehouse-infra pins `warehouse-planning-inbound-demand`) | `cmd/api/runtime.go` |
| `OBSERVED_CAPACITY_CONSUMER_GROUP` | none | **yes** | Consumer group of the task-completion consumer on `warehouse.fulfillment.events` (`TaskCompleted`; ADR 0017). Always on; no default; unset is a boot error. | `observed.consumerGroup` (required; warehouse-infra pins `warehouse-planning-observed-capacity`) | `cmd/api/runtime.go` |
| `EVENT_PUBLISHER` | `kafka` | no | Outbox relay sink. Only `kafka` (also the default) is accepted: `log` is a boot error (`EVENT_PUBLISHER=log is not supported: ...`), as is any other value. The variable stays because the chart and warehouse-infra set it. | `config.eventPublisher` (`kafka`) | `cmd/api/runtime.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Pause between relay passes that did not fill a batch of 100. A Go duration (`500ms`, `2s`) or plain seconds (`0.5`). A malformed or non-positive value logs a WARN and uses `1s`. | `config.outboxRelayInterval` (rendered only when set) | `cmd/api/main.go` |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | How many times one outbox row may fail before the relay excludes it from claims ([ADR 0013](/docs/adr/0013-outbox-relay-attempt-cap)). The row stays in `outbox_events` with `attempts` and `last_error` until an operator re-arms it (`UPDATE outbox_events SET attempts = 0 WHERE id = ...`, see the [Runbook](/docs/operations/runbook#outbox-relay)). `0` means unlimited (one poison row then blocks the relay forever). A negative or unparsable value logs a WARN and uses `20`. | `config.outboxMaxAttempts` (`20`) | `cmd/api/main.go` |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | no | How long shutdown waits after flipping `/readyz` to 503 and before closing the listener. `0` disables the wait. A negative or unparsable value logs a WARN and uses `5s`. Only `cmd/api` reads it. | `config.shutdownDrainDelay` (rendered only when set) | `cmd/api/main.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173` | no | Comma-separated, trimmed list of browser origins for the CORS middleware (methods `GET`, `POST`, `PUT`). Standalone `web/` dev on port 5190 needs `http://localhost:5190`. In the cluster, Kong's CORS plugin answers browsers. | `config.corsAllowedOrigins` (rendered only when set) | `internal/adapters/inbound/http/process_capacity_handler.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint (host:port, insecure) for traces and metrics. Export never blocks boot: a missing Collector only drops telemetry. | `otel.endpoint`, rendered while `otel.enabled` | `cmd/api/main.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version` resource attribute. | `image.tag` (else the chart `appVersion`) | `cmd/api/main.go` |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name` resource attribute. | `environment` | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/mcp` (MCP server)

`cmd/mcp` reads no Kafka variable: it never dials Kafka and never starts the
relay. Its plan tools write outbox rows that the `cmd/api` relay drains.

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address of the Streamable HTTP server (`/`, `/mcp` and `GET /healthz`). | `mcp.httpAddr` | `cmd/mcp/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/api`, and also accepts `warning`. | `config.logLevel` | `cmd/mcp/main.go` |
| `DATABASE_URL` | none | **yes** | The same OLTP database as `cmd/api`. Unset is a boot error (`DATABASE_URL is required: ...`); there is no in-memory mode. | as for `cmd/api` | `cmd/mcp/main.go` |
| `MIGRATIONS_DATABASE_URL` | the value of `DATABASE_URL` | no | Direct DSN for the migration step. `cmd/mcp` also migrates on boot, and the advisory lock makes concurrent starts with `cmd/api` safe. | `database.migrationsExistingSecretKey` | `cmd/mcp/main.go` |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | As for `cmd/api`. | `config.migrationsPath` | `cmd/mcp/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/api`. The service name is `warehouse-planning-mcp`. | `otel.endpoint`, rendered while `otel.enabled` | `cmd/mcp/main.go` |
| `SERVICE_VERSION` | `dev` | no | As for `cmd/api`. | `image.tag` | `cmd/mcp/main.go` |
| `ENVIRONMENT` | `local` | no | As for `cmd/api`. | `environment` | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/planning-projector` (analytics writer)

The projector validates its configuration before it does anything else and
exits non-zero when a required variable is missing. It then calls
`telemetry.Setup` like the other three binaries
([ADR 0015](/docs/adr/0015-projector-telemetry-and-chart-otel-env)).

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | Read-write DSN of the separate analytical database (`warehouse_planning_analytics`). It is also used for the analytical migrations, so make it a direct DSN, not PgBouncer. | secret key `ANALYTICS_DATABASE_URL` (`analytics.database.projectorUrl` or `analytics.database.existingSecret`) | `cmd/planning-projector/main.go` |
| `KAFKA_BROKERS` | none | **yes** | Comma-separated broker list. Each entry is trimmed and empty entries are dropped. | `kafka.brokers` | `cmd/planning-projector/main.go` |
| `ANALYTICS_CONSUMER_GROUP` | none | **yes** | Fixed consumer group on `warehouse.warehouse-planning.analytics`. No default; unset is a boot error. A new group id starts from the earliest offset and rebuilds the model from history. | `analytics.projector.consumerGroup` (required while `analytics.enabled`) | `cmd/planning-projector/main.go` |
| `ANALYTICS_MIGRATIONS_PATH` | `analytics/migrations` | no | Directory of the analytical `*.up.sql` files (`/app/analytics/migrations` in the image). | `analytics.migrationsPath` | `cmd/planning-projector/main.go` |
| `ADMIN_ADDR` | `:8091` | no | Admin listener for `/healthz` (liveness) and `/readyz` (drain gate, analytical Postgres ping and Kafka metadata). The chart's container port is fixed at 8091, so change both together. | `analytics.projector.adminAddr` | `cmd/planning-projector/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/mcp`. | `config.logLevel` | `cmd/planning-projector/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/api`. The service name is `warehouse-planning-projector`. | `otel.endpoint`, rendered while `otel.enabled` | `cmd/planning-projector/main.go` |
| `SERVICE_VERSION` | `dev` | no | As for `cmd/api`. | `image.tag` | `cmd/planning-projector/main.go` |
| `ENVIRONMENT` | `local` | no | As for `cmd/api`. | `environment` | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/planning-reports` (analytics reader)

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. The pool forces `default_transaction_read_only=on`. The chart feeds it the reader DSN (secret key `ANALYTICS_READER_DATABASE_URL`, from `analytics.database.reportsUrl`, falling back to `projectorUrl`). | `analytics.database.reportsUrl` / `analytics.database.existingSecret` | `cmd/planning-reports/main.go` |
| `HTTP_ADDR` | `:8092` | no | Listen address of the reports server (`/healthz`, `/readyz` with an analytical Postgres ping, and the reports). | `analytics.reports.httpAddr` | `cmd/planning-reports/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/mcp`. | `config.logLevel` | `cmd/planning-reports/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/api`. The service name is `warehouse-planning-reports`. | `otel.endpoint`, rendered while `otel.enabled` | `cmd/planning-reports/main.go` |
| `SERVICE_VERSION` | `dev` | no | As for `cmd/api`. | `image.tag` | `cmd/planning-reports/main.go` |
| `ENVIRONMENT` | `local` | no | As for `cmd/api`. | `environment` | `internal/adapters/outbound/telemetry/telemetry.go` |

## Fixed in code (no variable)

These limits are constants. Changing one needs a code change, and for the pool
sizes [ADR 0009](/docs/adr/0009-hpa-and-pgxpool-tuning) asks you to review
the HPA ceiling at the same time.

| Setting | Value | Where |
| --- | --- | --- |
| OLTP pool `MaxConns` and `statement_timeout` (`cmd/api`, `cmd/mcp`) | 10 connections, `5s`. A `pool_max_conns` in the DSN is overwritten. | `internal/adapters/outbound/postgres/pool.go` |
| Projector pool | 5 connections, `10s` | `internal/adapters/outbound/analyticsstore/pool.go` |
| Reports pool | 5 connections, `15s`, read-only transactions | `internal/adapters/outbound/analyticsstore/pool.go` |
| Boot retry of migrations and the first ping | 5 attempts, backoff 1, 2, 4 and 8 s between them (15 s in total); the last error is returned | `internal/bootretry/bootretry.go` |
| Outbox relay batch size | 100 rows per pass | `internal/adapters/outbound/outbox/relay.go` |
| Consumer retry backoff | 200 ms doubling to a 5 s cap | `internal/adapters/inbound/kafka/kafka.go` |
| Domain-consumer attempts before dead-lettering | 5 | `internal/adapters/inbound/kafka/deadletter.go` |
| HTTP graceful drain, consumer drain, relay drain | 10 s each | `cmd/api/main.go` |
| OTel metric export interval | 30 s | `internal/adapters/outbound/telemetry/telemetry.go` |
| Report range | default the 30 days ending now, at most 366 days | `internal/analytics/report/range.go` |
