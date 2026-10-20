{{/*
Expand the name of the chart.
*/}}
{{- define "warehouse-planning.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "warehouse-planning.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "warehouse-planning.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "warehouse-planning.labels" -}}
helm.sh/chart: {{ include "warehouse-planning.chart" . }}
{{ include "warehouse-planning.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "warehouse-planning.selectorLabels" -}}
app.kubernetes.io/name: {{ include "warehouse-planning.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "warehouse-planning.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "warehouse-planning.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "warehouse-planning.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "warehouse-planning.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service.
*/}}
{{- define "warehouse-planning.mcpFullname" -}}
{{- include "warehouse-planning.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote (web/, `capacity_mfe`) is served by its own nginx pod and reached
through warehouse-infra's Nginx web gateway at /mfes/warehouse-planning/. It is
deliberately a separate workload from the API: Kong never routes to it, and the
OLTP Service must never select it (component=frontend vs component=api).
*/}}
{{- define "warehouse-planning.frontendFullname" -}}
{{- include "warehouse-planning.fullname" . }}-frontend
{{- end }}

{{/*
Fully qualified name of the analytics projector deployment (ADR 0005).
*/}}
{{- define "warehouse-planning.projectorFullname" -}}
{{- include "warehouse-planning.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR 0005).
The reports Service is cluster-internal (component=analytics-reports); nothing
in this chart routes external traffic to it.
*/}}
{{- define "warehouse-planning.reportsFullname" -}}
{{- include "warehouse-planning.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytical DSNs: the operator's own
(analytics.database.existingSecret, keys ANALYTICS_DATABASE_URL and
ANALYTICS_READER_DATABASE_URL) or the one this chart creates.
*/}}
{{- define "warehouse-planning.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "warehouse-planning.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
Analytics guard: the projector and reports binaries refuse to boot without
ANALYTICS_DATABASE_URL, and the projector (ADR 0005) refuses to boot without
ANALYTICS_CONSUMER_GROUP. The brokers it reads are kafka.brokers, which the api
Deployment already requires (requireKafka), so only the DSN source and the
group are checked here, and only while analytics is on.
*/}}
{{- define "warehouse-planning.requireAnalyticsConfig" -}}
{{- if not (or .Values.analytics.database.projectorUrl .Values.analytics.database.existingSecret) -}}
{{- fail "analytics.enabled is true but neither analytics.database.projectorUrl nor analytics.database.existingSecret is set — the projector and reports binaries refuse to boot without ANALYTICS_DATABASE_URL." -}}
{{- end -}}
{{- if not .Values.analytics.projector.consumerGroup -}}
{{- fail "analytics.projector.consumerGroup is required (ANALYTICS_CONSUMER_GROUP): the projector always consumes warehouse.warehouse-planning.analytics under a STABLE group id and refuses to boot without one (ADR 0016)." -}}
{{- end -}}
{{- end -}}

{{/*
MANDATORY (ADR 0016: Postgres is the only runtime). cmd/api and cmd/mcp exit at
boot without DATABASE_URL; there is no in-memory mode. Fails the render naming
the values instead of crash-looping.
*/}}
{{- define "warehouse-planning.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "warehouse-planning requires database.url or database.existingSecret to be set (DATABASE_URL): the service runs on real Postgres only and exits at boot without it (ADR 0016)." -}}
{{- end -}}
{{- end -}}

{{/*
MANDATORY (ADR 0016: Kafka is the only runtime). cmd/api exits at boot without
KAFKA_BROKERS and rejects EVENT_PUBLISHER=log; the projector needs the brokers
too. There is no default broker, no log sink and no way to switch Kafka off.
kafka.enabled is kept only so values written before ADR 0016 still render
(warehouse-infra sets it to "true"): it may be true, never false.
*/}}
{{- define "warehouse-planning.requireKafka" -}}
{{- $_ := required "warehouse-planning requires kafka.brokers to be set (KAFKA_BROKERS, comma-separated): the service runs on real Kafka only and exits at boot without it (ADR 0016)." .Values.kafka.brokers -}}
{{- if and (hasKey .Values.kafka "enabled") (or (not .Values.kafka.enabled) (eq (toString .Values.kafka.enabled) "false")) -}}
{{- fail "kafka.enabled is false — Kafka cannot be switched off: the service runs on real Kafka only (ADR 0016). Drop the key or set it to true and set kafka.brokers." -}}
{{- end -}}
{{- if ne (toString .Values.config.eventPublisher) "kafka" -}}
{{- fail (printf "config.eventPublisher is %q — the only publisher is \"kafka\": the log sink was removed (ADR 0016); the binary rejects EVENT_PUBLISHER=log at boot." (toString .Values.config.eventPublisher)) -}}
{{- end -}}
{{- end -}}

{{/*
The three consumers (labor capacity, storage capacity, order demand) always
run, each under a STABLE consumer group id that must come from values (cmd/api
exits at boot without LABOR_CAPACITY_CONSUMER_GROUP,
STORAGE_CAPACITY_CONSUMER_GROUP and DEMAND_CONSUMER_GROUP; the binary has no
default group, which would let a locally run process join the live one). A
stable id is what lets a rescheduled pod resume from its committed offsets.
The order-demand consumer also needs the ONE site its demand is attributed to
(DEMAND_SITE_ID): order-management orders carry no site.
*/}}
{{- define "warehouse-planning.requireConsumerGroups" -}}
{{- range $c := list (dict "section" "config" "key" "laborCapacityConsumerGroup" "env" "LABOR_CAPACITY_CONSUMER_GROUP") (dict "section" "config" "key" "storageCapacityConsumerGroup" "env" "STORAGE_CAPACITY_CONSUMER_GROUP") (dict "section" "demand" "key" "consumerGroup" "env" "DEMAND_CONSUMER_GROUP") -}}
{{- if not (index (index $.Values $c.section) $c.key) -}}
{{- fail (printf "%s.%s is required (%s): the consumer always runs on real Kafka and cmd/api exits at boot without a group. Use a STABLE id (ADR 0016)." $c.section $c.key $c.env) -}}
{{- end -}}
{{- end -}}
{{- if not .Values.demand.siteId -}}
{{- fail "demand.siteId is required (DEMAND_SITE_ID): order-management orders carry no site, so the site the demand is attributed to must be configured; cmd/api exits at boot without it (ADR 0016)." -}}
{{- end -}}
{{- end -}}

{{/*
extraEnv may not bring back a log publisher (ADR 0016): no EVENT_PUBLISHER other
than kafka. Called with the list to check and the name of its value for the
message.
*/}}
{{- define "warehouse-planning.requireNoInMemoryEnv" -}}
{{- $where := .where -}}
{{- range .list -}}
{{- $name := toString .name -}}
{{- $value := toString (default "" .value) -}}
{{- if and (eq $name "EVENT_PUBLISHER") (ne $value "kafka") -}}
{{- fail (printf "%s sets EVENT_PUBLISHER=%s — the only publisher is \"kafka\" (ADR 0016)." $where $value) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The graceful shutdown is sequential (SHUTDOWN_DRAIN_DELAY 5s + HTTP drain 10s +
consumers drain 10s + outbox relay drain 10s). A grace period under 20s would
SIGKILL the pod between "consumers stopped" and "relay stopped", stranding an
uncommitted offset or an in-flight outbox batch.
*/}}
{{- define "warehouse-planning.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 20 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v — want at least 20: the consumers and the outbox relay drain after the HTTP server, and a shorter grace period SIGKILLs them mid-drain (ADR 0016)." .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end -}}

{{/*
Every render-time requirement of the real runtime, in one include. The api
Deployment (always rendered) includes it, so a mis-set release fails
`helm template` / `helm install` whichever component is enabled.
*/}}
{{- define "warehouse-planning.requireRealRuntime" -}}
{{- include "warehouse-planning.requireDatabase" . -}}
{{- include "warehouse-planning.requireKafka" . -}}
{{- include "warehouse-planning.requireConsumerGroups" . -}}
{{- include "warehouse-planning.requireNoInMemoryEnv" (dict "list" .Values.extraEnv "where" "extraEnv") -}}
{{- include "warehouse-planning.requireNoInMemoryEnv" (dict "list" .Values.mcp.extraEnv "where" "mcp.extraEnv") -}}
{{- include "warehouse-planning.requireGracePeriod" . -}}
{{- end -}}


{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "warehouse-planning.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "warehouse-planning.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}
