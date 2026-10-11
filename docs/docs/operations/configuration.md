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

The Helm chart (`charts/warehouse-planning`) renders most variables from
dedicated values. The **Chart value** column names the key. "not rendered"
means the chart never sets the variable, so the binary's default applies
unless you add it through `extraEnv` (the `api` component), `mcp.extraEnv`, or
a values override in `warehouse-infra`.

## `cmd/api` (REST, Kafka consumers, outbox relay)

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST server. | `config.httpAddr` | `cmd/api/main.go` |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` or `error`, case-insensitive. Any other value means `info`. Logs are JSON on stdout. | `config.logLevel` | `cmd/api/main.go` |
| `DATABASE_URL` | none | no for the binary, yes for the chart | Postgres DSN of the OLTP database (pgxpool). **Unset means in-memory adapters**: nothing survives a restart. The chart refuses to render without `database.url` or `database.existingSecret`. | `database.url` / `database.existingSecret` + `database.existingSecretKey` | `cmd/api/main.go` |
| `MIGRATIONS_DATABASE_URL` | the value of `DATABASE_URL` | no | DSN used **only** by the golang-migrate step at boot. In the kind cluster `DATABASE_URL` goes through PgBouncer (transaction pooling), which cannot hold golang-migrate's session-scoped `pg_advisory_lock`, so this one is a direct Postgres DSN ([ADR 0010](/docs/adr/0010-migrations-over-direct-connection)). The runtime pool never uses it. | `database.migrationsExistingSecretKey` (rendered with `optional: true`) | `cmd/api/main.go` |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | Directory of the OLTP `*.up.sql` files, relative to the working directory. The image copies them to `/app/migrations`. | `config.migrationsPath` (`migrations`) | `cmd/api/main.go` |
| `KAFKA_BROKERS` | none | only with `EVENT_PUBLISHER=kafka` or `DEMAND_CONSUMER_GROUP` | Comma-separated broker list, split on `,` without trimming (do not put spaces in it). Unset disables the labor and storage consumers (a WARN line, not a failure) and the order-demand consumer. | `kafka.brokers`, rendered only when `kafka.enabled=true` | `cmd/api/main.go`, `cmd/api/demand.go` |
| `LABOR_CAPACITY_CONSUMER_GROUP` | `warehouse-planning-labor-capacity` | no | Consumer group of the labor consumer on `warehouse.workforce.events`. | `config.laborCapacityConsumerGroup` | `cmd/api/main.go` |
| `STORAGE_CAPACITY_CONSUMER_GROUP` | `warehouse-planning-storage-capacity` | no | Consumer group of the facility consumer on `warehouse.facility.events`. | `config.storageCapacityConsumerGroup` | `cmd/api/main.go` |
| `DEMAND_CONSUMER_GROUP` | none | no | Consumer group of the order-demand consumer on `warehouse.order-management.events`. **Unset switches the feature off** ([ADR 0004](/docs/adr/0004-demand-ingestion-from-order-management)). It has no default on purpose, so that a local process never joins the live group. | `demand.consumerGroup` | `cmd/api/demand.go` |
| `DEMAND_SITE_ID` | none | yes when `DEMAND_CONSUMER_GROUP` is set | The one site every consumed order is attributed to, because order-management's events carry no site. If the group is set without it, the binary exits at boot. | `demand.siteId` | `cmd/api/demand.go` |
| `EVENT_PUBLISHER` | `log` | no | Outbox relay sink. `log` writes each drained event to the log. `kafka` writes to Kafka and needs `KAFKA_BROKERS`, otherwise the binary exits at boot. Any other value is a boot error. | `config.eventPublisher` | `cmd/api/main.go` |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Pause between relay passes that did not fill a batch of 100. A Go duration (`500ms`, `2s`) or plain seconds (`0.5`). A malformed or non-positive value logs a WARN and uses `1s`. | `config.outboxRelayInterval` (rendered only when set) | `cmd/api/main.go` |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | no | How long shutdown waits after flipping `/readyz` to 503 and before closing the listener. `0` disables the wait. A negative or unparsable value logs a WARN and uses `5s`. | not rendered | `cmd/api/main.go` |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173` | no | Comma-separated, trimmed list of browser origins for the CORS middleware (methods `GET`, `POST`, `PUT`). Standalone `web/` dev on port 5190 needs `http://localhost:5190`. In the cluster, Kong's CORS plugin answers browsers. | `config.corsAllowedOrigins` (rendered only when set) | `internal/adapters/inbound/http/process_capacity_handler.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint (host:port, insecure) for traces and metrics. Export never blocks boot: a missing Collector only drops telemetry. | not rendered | `cmd/api/main.go`, `internal/adapters/outbound/telemetry/telemetry.go` |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version` resource attribute. | not rendered | `cmd/api/main.go` |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name` resource attribute. | not rendered | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/mcp` (MCP server)

`cmd/mcp` reads no Kafka variable: it never dials Kafka and never starts the
relay. Its plan tools write outbox rows that the `cmd/api` relay drains.

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address of the Streamable HTTP server (`/`, `/mcp` and `GET /healthz`). | `mcp.httpAddr` | `cmd/mcp/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/api`, and also accepts `warning`. | `config.logLevel` | `cmd/mcp/main.go` |
| `DATABASE_URL` | none | no for the binary, yes for the chart | The same OLTP database as `cmd/api`. Unset means in-memory adapters that share nothing with `cmd/api`. | as for `cmd/api` | `cmd/mcp/main.go` |
| `MIGRATIONS_DATABASE_URL` | the value of `DATABASE_URL` | no | Direct DSN for the migration step. `cmd/mcp` also migrates on boot, and the advisory lock makes concurrent starts with `cmd/api` safe. | `database.migrationsExistingSecretKey` | `cmd/mcp/main.go` |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | no | As for `cmd/api`. | `config.migrationsPath` | `cmd/mcp/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/api`. The service name is `warehouse-planning-mcp`. | not rendered | `cmd/mcp/main.go` |
| `SERVICE_VERSION` | `dev` | no | As for `cmd/api`. | not rendered | `cmd/mcp/main.go` |
| `ENVIRONMENT` | `local` | no | As for `cmd/api`. | not rendered | `internal/adapters/outbound/telemetry/telemetry.go` |

## `cmd/planning-projector` (analytics writer)

The projector validates its configuration before it does anything else and
exits non-zero when a required variable is missing. It sets up no OpenTelemetry
at all, so it reads no `OTEL_*`, `SERVICE_VERSION` or `ENVIRONMENT`.

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | Read-write DSN of the separate analytical database (`warehouse_planning_analytics`). It is also used for the analytical migrations, so make it a direct DSN, not PgBouncer. | secret key `ANALYTICS_DATABASE_URL` (`analytics.database.projectorUrl` or `analytics.database.existingSecret`) | `cmd/planning-projector/main.go` |
| `KAFKA_BROKERS` | none | **yes** | Comma-separated broker list. Each entry is trimmed and empty entries are dropped. | `kafka.brokers` | `cmd/planning-projector/main.go` |
| `ANALYTICS_CONSUMER_GROUP` | `warehouse-planning-analytics` | no | Fixed consumer group on `warehouse.warehouse-planning.analytics`. A new group id starts from the earliest offset and rebuilds the model from history. | `analytics.projector.consumerGroup` | `cmd/planning-projector/main.go` |
| `ANALYTICS_MIGRATIONS_PATH` | `analytics/migrations` | no | Directory of the analytical `*.up.sql` files (`/app/analytics/migrations` in the image). | `analytics.migrationsPath` | `cmd/planning-projector/main.go` |
| `ADMIN_ADDR` | `:8091` | no | Admin listener for `/healthz` and `/readyz`. The chart's container port is fixed at 8091, so change both together. | `analytics.projector.adminAddr` | `cmd/planning-projector/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/mcp`. | `config.logLevel` | `cmd/planning-projector/main.go` |

## `cmd/planning-reports` (analytics reader)

| Variable | Default | Required | Meaning | Chart value | Source |
| --- | --- | --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. The pool forces `default_transaction_read_only=on`. The chart feeds it the reader DSN (secret key `ANALYTICS_READER_DATABASE_URL`, from `analytics.database.reportsUrl`, falling back to `projectorUrl`). | `analytics.database.reportsUrl` / `analytics.database.existingSecret` | `cmd/planning-reports/main.go` |
| `HTTP_ADDR` | `:8092` | no | Listen address of the reports server. | `analytics.reports.httpAddr` | `cmd/planning-reports/main.go` |
| `LOG_LEVEL` | `info` | no | As for `cmd/mcp`. | `config.logLevel` | `cmd/planning-reports/main.go` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `cmd/api`. The service name is `warehouse-planning-reports`. | not rendered | `cmd/planning-reports/main.go` |
| `SERVICE_VERSION` | `dev` | no | As for `cmd/api`. | not rendered | `cmd/planning-reports/main.go` |
| `ENVIRONMENT` | `local` | no | As for `cmd/api`. | not rendered | `internal/adapters/outbound/telemetry/telemetry.go` |

## Fixed in code (no variable)

These limits are constants. Changing one needs a code change, and for the pool
sizes [ADR 0009](/docs/adr/0009-hpa-and-pgxpool-tuning) asks you to review
the HPA ceiling at the same time.

| Setting | Value | Where |
| --- | --- | --- |
| OLTP pool `MaxConns` and `statement_timeout` (`cmd/api`, `cmd/mcp`) | 10 connections, `5s`. A `pool_max_conns` in the DSN is overwritten. | `internal/adapters/outbound/postgres/pool.go` |
| Projector pool | 5 connections, `10s` | `internal/adapters/outbound/analyticsstore/pool.go` |
| Reports pool | 5 connections, `15s`, read-only transactions | `internal/adapters/outbound/analyticsstore/pool.go` |
| Boot retry of migrations and the first ping | 5 attempts, backoff 1, 2, 4 and 8 s between them; the last error is returned | `internal/bootretry/bootretry.go` |
| Outbox relay batch size | 100 rows per pass | `internal/adapters/outbound/outbox/relay.go` |
| Consumer retry backoff | 200 ms doubling to a 5 s cap | `internal/adapters/inbound/kafka/kafka.go` |
| Domain-consumer attempts before dead-lettering | 5 | `internal/adapters/inbound/kafka/deadletter.go` |
| HTTP graceful drain, consumer drain, relay drain | 10 s each | `cmd/api/main.go` |
| OTel metric export interval | 30 s | `internal/adapters/outbound/telemetry/telemetry.go` |
| Report range | default the 30 days ending now, at most 366 days | `internal/analytics/report/range.go` |
