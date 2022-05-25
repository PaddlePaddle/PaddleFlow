{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "init-cluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "init-cluster.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified fe name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "init-cluster.init_cluster.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{- define "init-cluster.init_cluster.fullname_registry" -}}
{{- printf "registry-%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}
