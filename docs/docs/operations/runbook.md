---
id: runbook
title: Runbook
sidebar_label: Runbook
---

# Runbook

How the service is deployed and run, and the routine procedures it needs. The
facts come from `charts/warehouse-planning`, the four composition roots under
`cmd/`, and `warehouse-infra` where it supplies the cluster values. For every
environment variable see [Configuration](/docs/operations/configuration). For
symptoms and fixes see [Troubleshooting](/docs/operations/troubleshooting).

## Deployment

One image (`warehouse/warehouse-planning` in kind, `ghcr.io/iqvo/warehouse-planning`
when released) contains all four binaries at `/app/<name>`. The OLTP
migrations are at `/app/migrations` and the analytical ones at
`/app/analytics/migrations`. The default `ENTRYPOINT` is `./api`, and the other
components override `command`. The Helm chart is `warehouse-planning`
(`charts/warehouse-planning`). The `release` job in `.github/workflows/ci.yml`
packages it to `oci://ghcr.io/iqvo` on every merge to `main`. In the kind
cluster, ArgoCD installs it from this repo's `develop` branch with the values in
`warehouse-infra`'s `helm-values/warehouse-planning.yaml`.

### Workloads per component

`<fullname>` is the chart's full name. In the kind cluster that is
`warehouse-planning`, because the release name equals the chart name.

| Component (`app.kubernetes.io/component`) | Deployment | Binary | Enabled by | Container port | Service |
| --- | --- | --- | --- | --- | --- |
| `api` | `<fullname>` | `/app/api` (the entrypoint) | always | `http` 8080 | `<fullname>`, ClusterIP, port 80 to `http` |
| `mcp` | `<fullname>-mcp` | `/app/mcp` | `mcp.enabled` (default `false`) | `http` 8090 | `<fullname>-mcp`, ClusterIP, port 8090 |
| `analytics-projector` | `<fullname>-projector` | `/app/planning-projector` | `analytics.enabled` (default `false`) | `admin` 8091 | none |
| `analytics-reports` | `<fullname>-reports` | `/app/planning-reports` | `analytics.enabled` | `http` 8092 | `<fullname>-reports`, ClusterIP, port 80 to `http` |
| `frontend` | `<fullname>-frontend` | nginx serving `web/` (`capacity_mfe`) | `frontend.enabled` (default `false`) | 8080 | `<fullname>-frontend`, port 80 |

In the kind cluster, `warehouse-infra` turns on `kafka`, `demand` and
`analytics` (in `helm-values/warehouse-planning.yaml`), the frontend (in
`terraform/services.tf` and `frontends.tf`) and the MCP server (in
`terraform/mcp.tf`). The edge routes are:

- Kong on `:8000`: `/api/warehouse-planning/...` goes to the `api` Service with the prefix stripped (the chart's `gatewayApi` HTTPRoute, `stripPath: true`).
- Kong on `:8000`: `/api/warehouse-planning/reports/...` goes to the `<fullname>-reports` Service, rewritten to `/reports/...` (a second route defined in `warehouse-infra`'s `terraform/services.tf`).
- Nginx on `:80`: `/mfes/warehouse-planning/` serves the frontend remote. Kong never serves frontend assets.

REST, MCP and reports are unauthenticated. Access control is the in-cluster
boundary.

### Render guards

The chart fails `helm template` instead of rendering a pod that would crash or
run in a degraded mode:

- no `database.url` and no `database.existingSecret` (`requireDatabase`; the binary would fall back to in-memory adapters);
- `config.eventPublisher=kafka` with `kafka.enabled=false` (`requireKafkaForPublisher`);
- `demand.consumerGroup` set without `demand.siteId`, or without `kafka.enabled` (`requireDemandConfig`);
- `analytics.enabled` without an analytical DSN source or without `kafka.enabled` (`requireAnalyticsConfig`).

Check a values change locally with
`helm lint charts/warehouse-planning --set database.url=postgres://u@example.invalid:5432/db`
and `python3 charts/warehouse-planning/tests/test_service_selectors.py`. The
second check proves that each Service selects exactly one Deployment.

### Probes and what readiness waits for

| Component | startupProbe | livenessProbe | readinessProbe | `terminationGracePeriodSeconds` |
| --- | --- | --- | --- | --- |
| `api` | `GET /healthz`, every 2 s, 30 failures allowed (60 s) | `GET /healthz`, every 10 s after 5 s | `GET /readyz`, every 5 s after 3 s | 40 |
| `mcp` | `GET /healthz` (same timing) | `GET /healthz` | `GET /healthz` | Kubernetes default (30) |
| `analytics-projector` | `GET /healthz` on `admin` | `GET /healthz` | `GET /readyz` | 40 |
| `analytics-reports` | `GET /healthz` | `GET /healthz` | `GET /healthz` | 40 |

What readiness actually covers:

- **`api`**: the listener opens only after the boot sequence: migrations (5 attempts, backoff 1 to 8 s), pool creation and a ping (also retried), then the consumers and the outbox relay start. `/healthz` and `/readyz` therefore cannot answer until Postgres is reachable and migrated. Kafka is **not** waited for: readers and the relay writer dial lazily, so a broker outage never blocks readiness. `/readyz` returns 200 `{"status":"ready"}` from the start and flips to 503 `{"status":"not_ready"}` only as the first step of shutdown.
- **`mcp`**: same boot order, without Kafka. There is no `/readyz`. Readiness uses `/healthz`, which never flips.
- **`analytics-projector`**: the admin server starts after the analytical migrations and a ping. `/readyz` flips to 503 only at shutdown. Consumer lag does not affect it.
- **`analytics-reports`**: the listener starts after a ping of the analytical database. It runs no migrations, so if it starts before the projector has created the tables, its queries fail with 500 `report-store-error` until they exist.

### Graceful shutdown (`cmd/api`)

On SIGTERM, `serveUntilSignal` in `cmd/api/main.go` does this, in order:

1. `/readyz` flips to 503.
2. It waits `SHUTDOWN_DRAIN_DELAY` (default 5 s).
3. It drains in-flight HTTP requests (up to 10 s).
4. It stops the Kafka consumers and waits up to 10 s each.
5. It stops the outbox relay **last** and waits up to 10 s, so that an event committed by the final HTTP request or consumer message is still sent.
6. It closes the pool.

The worst case is about 35 s, which `terminationGracePeriodSeconds: 40` covers.
If you raise `SHUTDOWN_DRAIN_DELAY`, raise the grace period too.

## Migrations

[ADR 0010](/docs/adr/0010-migrations-over-direct-connection) records this
design. Migrations are golang-migrate `*.up.sql` files applied at boot. No
separate Job or init container runs them.

| Database | Applied by | Path | DSN used |
| --- | --- | --- | --- |
| OLTP (one per context, e.g. `warehouse_planning`) | `cmd/api` **and** `cmd/mcp`, each on start. The advisory lock makes concurrent starts safe. | `MIGRATIONS_PATH` (`/app/migrations`) | `MIGRATIONS_DATABASE_URL`, falling back to `DATABASE_URL` |
| Analytical (`warehouse_planning_analytics`) | `cmd/planning-projector` on start | `ANALYTICS_MIGRATIONS_PATH` (`/app/analytics/migrations`) | `ANALYTICS_DATABASE_URL` |

In the kind cluster, `DATABASE_URL` points at PgBouncer in transaction-pooling
mode, which cannot hold golang-migrate's session-scoped `pg_advisory_lock`. The
`<svc>-db` Secret therefore also carries a direct `MIGRATIONS_DATABASE_URL`,
and the chart mounts it with `optional: true`. Where the Secret has no such key,
the binary migrates over `DATABASE_URL`.

OLTP migrations (`internal/adapters/outbound/postgres/migrations`):

| Version | Effect |
| --- | --- |
| `0001_process_capacity` | `process_capacity` and `process_capacity_constraint` (child rows, `ON DELETE CASCADE`) |
| `0002_kafka_read_models` | `processed_events` (consumer idempotency), `location_slot_registration` and `location_slot_tally` (the facility tally) |
| `0003_capacity_plan_and_outbox` | `capacity_plans` and `outbox_events`, with a partial index on unpublished rows |
| `0004_process_paths` | `process_paths` |
| `0005_station_standards_and_cleanup` | `station_standards`; adds `capacity_plans.bottleneck_constraint` and `warnings`; **deletes** the legacy derived `process_capacity` rows (process type `STORAGE` or a 1970 window start) |
| `0006_order_demand` | `order_demand`; adds `capacity_plans.demand_source` |
| `0007_outbox_event_id_per_topic` | replaces the `outbox_events.event_id` unique key with `UNIQUE (event_id, topic)`, so one CloudEvents id can go to both topics |
| `0008_capacity_plan_site_id` | adds `capacity_plans.site_id`, which defaults to `''` for older rows |

Analytical migration (`analytics/migrations`): `0001_plan_facts` creates
`analytics_processed_events` and `plan_facts`.

Every OLTP migration has a `.down.sql` file, but no binary or make target runs
a down migration. A rollback is a manual `migrate` CLI run against the direct
DSN.

## Kafka topics and consumer groups

There is one broker platform-wide. In-cluster it is
`kafka.warehouse-systems.svc.cluster.local:9092`, and from the host it is
`localhost:9092`. Topics are auto-created by the writers
(`AllowAutoTopicCreation`). Every message is a CloudEvents 1.0 event in
structured mode (`content-type: application/cloudevents+json; charset=UTF-8`).

| Direction | Topic | Binary | Consumer group (env, default) | Types |
| --- | --- | --- | --- | --- |
| consume | `warehouse.workforce.events` | `cmd/api` | `LABOR_CAPACITY_CONSUMER_GROUP`, `warehouse-planning-labor-capacity` | `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` |
| consume | `warehouse.facility.events` | `cmd/api` | `STORAGE_CAPACITY_CONSUMER_GROUP`, `warehouse-planning-storage-capacity` | `...facility-layout.locationslot.LocationSlotRegistered`, `...LocationSlotDecommissioned` |
| consume | `warehouse.order-management.events` | `cmd/api` | `DEMAND_CONSUMER_GROUP`, no default (off when unset; `warehouse-planning-demand` in the cluster) | `...order-management.order.OrderAllocated`, `...OrderPartiallyAllocated` |
| produce | `warehouse.warehouse-planning.events` | `cmd/api` relay | n/a | `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`, `BottleneckDetected` |
| produce | `warehouse.warehouse-planning.analytics` | `cmd/api` relay | n/a | the same four types (ADR 0005) |
| consume | `warehouse.warehouse-planning.analytics` | `cmd/planning-projector` | `ANALYTICS_CONSUMER_GROUP`, `warehouse-planning-analytics` | the same four types |
| produce (DLQ) | `warehouse.workforce.events.dlq`, `warehouse.facility.events.dlq`, `warehouse.order-management.events.dlq` | `cmd/api` | n/a | the original message plus `x-dlq-*` headers |
| produce (DLQ) | `warehouse.warehouse-planning.analytics.dlq` | `cmd/planning-projector` | n/a | the original message plus `x-dlq-*` headers |

The `processed_events` claim uses these consumer names: `labor-capacity-consumer`,
`storage-capacity-consumer` and `order-demand-consumer`.

### Outbox relay

`CreateCapacityPlan` and `PublishCapacityPlan` save the aggregate and insert the
already-encoded events into `outbox_events` in one transaction. That is two rows
per event, one for each produced topic, with the same CloudEvents id. `cmd/mcp`
writes the same rows but never relays them. **Only `cmd/api` runs the relay**,
so MCP-created plans reach Kafka only while an `api` pod is up with
`EVENT_PUBLISHER=kafka`.

Each pass claims up to 100 unpublished rows in `id` order with
`FOR UPDATE SKIP LOCKED` and sends them one by one. It marks a row
`published_at = now()` only after the broker acknowledged it (`RequireAll`
acks). At the first failure it records `attempts` and `last_error` on that row
and ends the pass. A full batch is followed at once by another pass. Otherwise
the relay sleeps `OUTBOX_RELAY_INTERVAL`. The Kafka message key is the plan id
(Hash balancer), so one plan's events share a partition. Delivery is
at-least-once, and consumers dedupe on the CloudEvents `id`.

### Dead-letter behaviour

- **Domain consumers (`cmd/api`)**: deterministic problems are logged and skipped, never dead-lettered: not a CloudEvent, an unknown type, a malformed payload, missing fields, or a domain-validation rejection. A transient failure (database begin, claim, upsert or commit) is retried on the same message up to 5 times with backoff, then published to `<topic>.dlq` with the headers `x-dlq-source-topic`, `x-dlq-source-partition`, `x-dlq-source-offset`, `x-dlq-error` and `x-dlq-failed-at`, and committed past. The DLQ publish itself is retried until it succeeds.
- **Projector**: a known type with an unusable payload, or a deterministic store rejection, is dead-lettered at once to `warehouse.warehouse-planning.analytics.dlq` with `x-dlq-source-topic`, `x-dlq-error` and `x-dlq-failed-at`. A transient failure is retried **forever** and never dead-lettered ([ADR 0005](/docs/adr/0005-analytics-read-side)). A non-CloudEvents message is skipped, with a WARN at most once a minute.

Nothing in this repo consumes or replays a DLQ topic. See
[Re-drive a dead-lettered message](#re-drive-a-dead-lettered-message).

## Housekeeping

There is **no sweeper or retention job** in this service. The following tables
only grow:

- `outbox_events`: published rows keep `published_at` set and are never deleted;
- `processed_events` and `analytics_processed_events`: one row per consumed event, forever;
- `order_demand`: one row per order id, with no expiry.

None of these sizes is a correctness problem: every read is indexed, and the
relay only scans `published_at IS NULL` through a partial index. Pruning
published outbox rows or old idempotency claims is a manual, out-of-band
operation. If you delete a `processed_events` row for an event that is later
redelivered, that event is applied again.

## Scaling

- **HPA** (`templates/hpa.yaml`, [ADR 0009](/docs/adr/0009-hpa-and-pgxpool-tuning)): each component has its own `autoscaling.<component>` block, and all are off by default. The CPU target is 70 %, and the maximum is 4 replicas for `api`, 2 for `projector`, 3 for `reports` and 3 for `frontend`. With an HPA enabled, the Deployment omits `replicas`.
- **`api` with more than one replica**: the relay is safe because `SKIP LOCKED` hands each pass a disjoint batch. Strict `id` order then holds within one pass, not across replicas. The three consumers share partitions through their stable group ids, and every message's claim and effect form one transaction.
- **`mcp`**: it has no HPA block on purpose. The Streamable HTTP handler keeps per-process session state, so scale it only with session affinity.
- **Connection budget**: each `api` or `mcp` process opens at most 10 OLTP connections (`pool.go`). Each projector or reports process opens at most 5 analytical connections. Budget the database with replicas × pool size before raising `maxReplicas`.

## Routine procedures

### Seed station standards and verify path capacity

Station throughput is operator-declared ([ADR 0002](/docs/adr/0002-station-capacity-composition)).
Declare it through the API, never with SQL. In-cluster:

```bash
kubectl -n warehouse-systems run seed --rm -i --restart=Never --image=curlimages/curl --command -- \
  curl -s -X PUT -H 'Content-Type: application/json' \
  -d '{"quantity":180,"unit":"PACKAGE","period_seconds":3600}' \
  http://warehouse-planning.warehouse-systems.svc.cluster.local/station-standards/SIM1/PACK
```

Then check `GET /station-standards?location=SIM1` and `GET /storage-capacity?location=SIM1`.

### Re-publish events from the outbox

The relay sends any row whose `published_at` is NULL, and it re-sends the same
bytes and the same CloudEvents id. To re-publish specific rows (for example
after a topic was lost), set `published_at = NULL` for those `outbox_events`
ids in the OLTP database. The `api` relay picks them up on its next pass.
Downstream consumers that dedupe on `id` ignore the copies they already
applied.

### Replay a consumed topic

- **Domain consumers**: reset the group's offsets with `kafka-consumer-groups.sh --reset-offsets` while `api` is scaled to 0. Already-claimed CloudEvents ids are skipped through `processed_events`, so a replay only applies events this service has not seen.
- **Projector (rebuild the analytics model)**: point `ANALYTICS_CONSUMER_GROUP` at a new group id, or reset the existing group to the earliest offset. To rebuild from scratch, also truncate `plan_facts` and `analytics_processed_events`, because the projection is idempotent on the event id.

### Re-drive a dead-lettered message

No tool exists for this. Fix the cause first (database, payload). Then
re-produce the message's original value and key from the `.dlq` topic onto the
source topic. Its CloudEvents id is unchanged, so a message that was already
applied is skipped by the processed-event claim.

### Rotate a database credential

Update the Secret (`database.existingSecret`, or the analytics Secret), then
`kubectl rollout restart` each affected Deployment: `api`, `mcp`, `projector`
and `reports`. The pod template's `checksum/config` annotation covers only the
ConfigMap, so a changed Secret does not roll the pods by itself.

### Analytics reports

`cmd/planning-reports` serves `GET /reports/bottleneck-frequency`,
`/reports/shortage-trend`, `/reports/plan-throughput` and `/reports/freshness`.
All four are declared in `apis/openapi.yaml`. The first three take optional
RFC 3339 `from` (inclusive) and `to` (exclusive) query parameters. Without them
the range is the 30 days ending now, and a range longer than 366 days is a 400
`invalid-report-range`. `/reports/freshness` returns `as_of` (the time of the
newest applied event) and `lag_seconds`. Both are `null` until the projector
has applied an event.
