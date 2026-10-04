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
analytics.enabled needs an analytical DSN source (the projector refuses to boot
without ANALYTICS_DATABASE_URL) and a broker (KAFKA_BROKERS is only rendered
into the api when kafka.enabled; the projector reads kafka.brokers). Fail at
render instead of crash-looping. Events only reach the analytics topic when
config.eventPublisher is "kafka"; that is documented, not enforced, since the
projector is harmless without events.
*/}}
{{- define "warehouse-planning.requireAnalyticsConfig" -}}
{{- if not (or .Values.analytics.database.projectorUrl .Values.analytics.database.existingSecret) -}}
{{- fail "analytics.enabled is true but neither analytics.database.projectorUrl nor analytics.database.existingSecret is set — the projector and reports binaries refuse to boot without ANALYTICS_DATABASE_URL." -}}
{{- end -}}
{{- if not .Values.kafka.enabled -}}
{{- fail "analytics.enabled is true but kafka.enabled is false — the projector consumes warehouse.warehouse-planning.analytics and needs kafka.brokers. Set kafka.enabled=true." -}}
{{- end -}}
{{- end -}}

{{/*
Fails chart rendering with a clear message if no DATABASE_URL source is
configured. The binary would silently fall back to in-memory adapters (state
lost on restart, ProcessPaths never persisted anyway) -- fine for `go run`,
never a deployable state, so surface it as a helm render-time error instead.
*/}}
{{- define "warehouse-planning.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "warehouse-planning requires database.url or database.existingSecret to be set — without DATABASE_URL the binary silently runs on in-memory adapters, which is not a deployable state." -}}
{{- end -}}
{{- end -}}

{{/*
EVENT_PUBLISHER=kafka makes the binary refuse to boot without KAFKA_BROKERS
(cmd/api startOutboxRelay), and KAFKA_BROKERS is only rendered when
kafka.enabled is true -- so that combination would crash-loop. Fail at render.
*/}}
{{- define "warehouse-planning.requireKafkaForPublisher" -}}
{{- if and (eq .Values.config.eventPublisher "kafka") (not .Values.kafka.enabled) -}}
{{- fail "config.eventPublisher is \"kafka\" but kafka.enabled is false — the binary exits at boot (EVENT_PUBLISHER=kafka requires KAFKA_BROKERS). Set kafka.enabled=true and kafka.brokers." -}}
{{- end -}}
{{- end -}}

{{/*
The order-demand consumer (docs/adr/0004) is OFF while demand.consumerGroup is
empty. When it is set the binary needs the ONE site its demand is attributed to
(DEMAND_SITE_ID; a group without a site refuses to boot) and a broker
(KAFKA_BROKERS, only rendered when kafka.enabled is true; without it the
consumer would be silently disabled). Fail at render instead.
*/}}
{{- define "warehouse-planning.requireDemandConfig" -}}
{{- if .Values.demand.consumerGroup -}}
{{- if not .Values.demand.siteId -}}
{{- fail "demand.consumerGroup is set but demand.siteId is not — order-management orders carry no site, so the site demand is attributed to must be configured (the binary refuses to boot without DEMAND_SITE_ID)." -}}
{{- end -}}
{{- if not .Values.kafka.enabled -}}
{{- fail "demand.consumerGroup is set but kafka.enabled is false — the order-demand consumer would be silently disabled (no KAFKA_BROKERS). Set kafka.enabled=true and kafka.brokers." -}}
{{- end -}}
{{- end -}}
{{- end -}}
