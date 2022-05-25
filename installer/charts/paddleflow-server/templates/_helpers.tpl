{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "paddleflow-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "paddleflow-server.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{/*
Create a default fully qualified fe name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "paddleflow-server.paddleflow_server.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}

{{- define "paddleflow-server.paddleflow_server.fullname_registry" -}}
{{- printf "registry-%s" .Release.Name | trunc 63 | trimSuffix "-" | replace "_" "-" -}}
{{- end -}}
