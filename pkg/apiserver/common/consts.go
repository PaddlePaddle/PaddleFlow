/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

const (
	SeparatorComma = ","

	PrefixSchedule   = "schedule-"
	PrefixRun        = "run-"
	PrefixPipeline   = "ppl-"
	PrefixCache      = "cch-"
	PrefixGrant      = "grant"
	PrefixQueue      = "queue"
	PrefixCluster    = "cluster"
	PrefixFlavour    = "flavour"
	PrefixConnection = "conn"

	ResourceTypeSchedule      = "schedule"
	ResourceTypeRun           = "run"
	ResourceTypeRunCache      = "run_cache"
	ResourceTypeArtifactEvent = "artifact_event"
	ResourceTypeUser          = "user"
	ResourceTypeQueue         = "queue"
	ResourceTypeFs            = "fs"
	ResourceTypeImage         = "image"
	ResourceTypePipeline      = "pipeline"
	ResourceTypeCluster       = "cluster"
	ResourceTypeJob           = "job"

	HeaderKeyRequestID     = "x-pf-request-id"
	HeaderKeyUserName      = "x-pf-user-name"
	HeaderKeyAuthorization = "x-pf-authorization"
	HeaderClientIDKey      = "x-pf-client-id"

	ResponseCode      = "code"
	ResponseMessage   = "message"
	ResponseRequestID = "requestID"

	CFSSuccessMessage     = "create file system success"
	ClaimsSuccessMessage  = "create pv and pvc success"
	RegisterClientMessage = "register client success"
	HeartBeatMessage      = "heat beat client success"

	BeginFilePosition = "begin"
	EndFilePosition   = "end"

	LogPageSizeMax     = 100
	LogPageSizeDefault = 100
	LogPageNoDefault   = 1

	Pod = "pod"

	StsMaxSeqData = 1000
)
