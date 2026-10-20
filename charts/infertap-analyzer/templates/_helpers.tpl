{{/*
Names and labels, plus the three refusals that make a wrong install fail at `helm install` rather
than at the first pod log.
*/}}

{{- define "infertap-analyzer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The ServiceAccount the pod runs as. `create: false` with a name means "use the one my platform
team already manages"; `create: false` with no name falls back to `default`, which is the old
behaviour and is stated here rather than left implicit.
*/}}
{{- define "infertap-analyzer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "infertap-analyzer.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "infertap-analyzer.fullname" -}}
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

{{- define "infertap-analyzer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "infertap-analyzer.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "infertap-analyzer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "infertap-analyzer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
The image reference.

**`image.registry` has no default and this refuses without one** (distribution P2). The mirror is
step one of the install, and a chart that silently reaches our registry has made us part of a
customer's runtime — which is the dependency P2 exists to forbid. Failing here costs one error
message; failing at their first `kubectl get pods` costs an afternoon and a support ticket.
*/}}
{{- define "infertap-analyzer.image" -}}
{{- if not .Values.image.registry -}}
{{- fail "\n\nimage.registry is required and has no default.\n\nThe analyzer is pulled from YOUR registry, never ours: mirror the release once\n(`oras copy oci.infertap.ai/analyzer:1.0.0 <your-registry>/analyzer:1.0.0`) and set\n`image.registry` to it. A chart with our registry baked in would put us in your\nruntime path, which is exactly what this refusal prevents." -}}
{{- end -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository $tag -}}
{{- end -}}

{{/*
The deployment facts that have no plausible default. Each is checked here so `helm install`
refuses, rather than the daemon refusing on its own after the pod has scheduled and pulled.
*/}}
{{- define "infertap-analyzer.validate" -}}
{{- if not .Values.fleet.id -}}
{{- fail "\n\nfleet.id is required.\n\nOne analyzer serves one fleet, and the id is what every figure it emits is keyed by.\nThere is no default because a wrong one would install cleanly and measure the wrong estate." -}}
{{- end -}}
{{- if not .Values.bucket.url -}}
{{- fail "\n\nbucket.url is required.\n\nThe analyzer reads the tap's sealed segments from your S3-API object store. We never\nship one (\"no object store\" is not a supported configuration), so this names yours." -}}
{{- end -}}
{{- end -}}

{{/*
The bucket credentials, as environment variables.

**Defined once because two containers need them and a copy is a place to drift.** The preflight
initContainer and the analyzer itself must reach the same bucket with the same credential, or the
gate is testing something the service will not do. Environment rather than arguments throughout:
an argument list is visible to every process on the node via `ps`, a process environment is not.
*/}}
{{- define "infertap-analyzer.bucketEnv" -}}
{{- if .Values.bucket.existingSecret }}
- name: INFERTAP_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.bucket.existingSecret }}
      key: {{ .Values.bucket.accessKeyIdKey }}
- name: INFERTAP_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.bucket.existingSecret }}
      key: {{ .Values.bucket.secretAccessKeyKey }}
{{- end }}
{{- end -}}
