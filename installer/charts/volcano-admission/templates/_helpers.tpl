{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "volcano-admission.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "volcano-admission.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified fe name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "volcano-admission.volcano_admission.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{- define "volcano-admission.volcano_admission.fullname_registry" -}}
{{- printf "registry-%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}
