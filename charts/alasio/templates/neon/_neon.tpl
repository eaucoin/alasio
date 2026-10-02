{{/* A Neon component's name: "<fullname>-neon-<component>". */}}
{{- define "alasio.neonName" -}}
{{- include "alasio.componentName" (dict "root" .root "component" (printf "neon-%s" .component)) -}}
{{- end -}}

{{/* The labels of a pod of the data stack (Neon, the object store, the lake), which its NetworkPolicy admits each other by. */}}
{{- define "alasio.stackLabels" -}}
{{ include "alasio.componentLabels" . }}
alasio.dev/stack: neon
{{- end -}}

{{/* What every pod of the data stack has in its spec: pull secrets, placement, and Neon's user. */}}
{{- define "alasio.neonPodSpec" -}}
{{- include "alasio.imagePullSecrets" . }}
securityContext: {{- include "alasio.restrictedPod" (dict "uid" .Values.neon.runAsUser "gid" .Values.neon.runAsGroup) | nindent 2 }}
{{- with .Values.neon.nodeSelector }}
nodeSelector: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.neon.tolerations }}
tolerations: {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* An init container that waits until `url` answers 2xx, with Neon's image's curl. */}}
{{- define "alasio.waitFor" -}}
- name: wait-for-{{ .name }}
  image: {{ include "alasio.image" (dict "image" .root.Values.neon.image "root" .root) }}
  imagePullPolicy: IfNotPresent
  command: [/bin/sh, -c, 'until curl -fsS -o /dev/null --max-time 5 "$0"; do sleep 2; done', {{ .url | quote }}]
  securityContext: {{- include "alasio.restrictedContainer" . | nindent 4 }}
  resources:
    requests: { cpu: 10m, memory: 16Mi }
    limits: { memory: 64Mi }
{{- end -}}

{{/* A volume claim template or claim spec of `storage` ({ size, storageClassName }). */}}
{{- define "alasio.claimSpec" -}}
accessModes: [ReadWriteOnce]
{{- with .storageClassName }}
storageClassName: {{ . }}
{{- end }}
resources:
  requests:
    storage: {{ .size }}
{{- end -}}

{{/* Until the bundled object store's buckets exist, nothing that stores in them starts. */}}
{{- define "alasio.waitForObjectStore" -}}
{{- if .Values.objectStore.bundled.enabled }}
{{ include "alasio.waitFor" (dict "root" . "name" "object-store" "url" (printf "http://%s:8333/healthz" (include "alasio.componentName" (dict "root" . "component" "seaweedfs")))) }}
{{- end }}
{{- end -}}
