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
