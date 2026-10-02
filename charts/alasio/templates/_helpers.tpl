{{/* The chart's name, and the release's full name every object's name begins with. */}}
{{- define "alasio.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "alasio.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* A component's name: "<fullname>-<component>". */}}
{{- define "alasio.componentName" -}}
{{- printf "%s-%s" (include "alasio.fullname" .root) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The labels every object carries. */}}
{{- define "alasio.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "alasio.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: alasio
{{- end -}}

{{/* A component's labels, and the ones that select its pods. */}}
{{- define "alasio.componentLabels" -}}
{{ include "alasio.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "alasio.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alasio.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* An image reference: repository, then the tag (the chart's appVersion when empty), then the digest. */}}
{{- define "alasio.image" -}}
{{- $tag := .image.tag | default .root.Chart.AppVersion -}}
{{- printf "%s:%s" .image.repository $tag -}}
{{- with .image.digest }}@{{ . }}{{ end -}}
{{- end -}}

{{/* The namespaces of sessions and of folder workspaces' bayma. */}}
{{- define "alasio.sessionsNamespace" -}}
{{- .Values.sessions.namespace -}}
{{- end -}}

{{- define "alasio.hostNamespace" -}}
{{- .Values.host.namespace -}}
{{- end -}}

{{/* The Secret alasio reads its database URL and the lake role's password from. */}}
{{- define "alasio.databaseSecret" -}}
{{- if .Values.neon.enabled -}}
{{- printf "%s-database" (include "alasio.fullname" .) -}}
{{- else -}}
{{- required "neon.external.existingSecret is required when neon.enabled is false" .Values.neon.external.existingSecret -}}
{{- end -}}
{{- end -}}

{{/* The object store's S3 endpoint, as pods in the release's namespace reach it. */}}
{{- define "alasio.s3Endpoint" -}}
{{- if .Values.objectStore.bundled.enabled -}}
{{- printf "http://%s:8333" (include "alasio.componentName" (dict "root" . "component" "seaweedfs")) -}}
{{- else -}}
{{- required "objectStore.external.endpoint is required when objectStore.bundled.enabled is false" .Values.objectStore.external.endpoint -}}
{{- end -}}
{{- end -}}

{{/* The pod security context every workload of the release runs with: restricted. */}}
{{- define "alasio.restrictedPod" -}}
runAsNonRoot: true
runAsUser: {{ .uid }}
runAsGroup: {{ .gid }}
fsGroup: {{ .gid }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "alasio.restrictedContainer" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
{{- end -}}

{{/*
OpenTelemetry's standard variables, which alasio, the lake and Neon's collector export
with, from `telemetry`. Nothing when no endpoint is set.
*/}}
{{- define "alasio.otelEnv" -}}
{{- with .Values.telemetry -}}
{{- if .otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .otlpEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ .otlpProtocol | quote }}
{{- if .headersSecret }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  valueFrom:
    secretKeyRef:
      name: {{ .headersSecret }}
      key: {{ .headersKey }}
{{- end }}
{{- if .resourceAttributes }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ .resourceAttributes | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The pull secrets every pod of the release is given. */}}
{{- define "alasio.imagePullSecrets" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
{{- range . }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The host profile's mounts, as volumes and as volume mounts. */}}
{{- define "alasio.hostVolumes" -}}
{{- range .Values.host.mounts }}
- name: {{ .name }}
  hostPath:
    path: {{ .hostPath }}
    {{- with .type }}
    type: {{ . }}
    {{- end }}
{{- end }}
{{- end -}}

{{- define "alasio.hostVolumeMounts" -}}
{{- range .Values.host.mounts }}
- name: {{ .name }}
  mountPath: {{ .mountPath }}
  {{- if .readOnly }}
  readOnly: true
  {{- end }}
{{- end }}
{{- end -}}
