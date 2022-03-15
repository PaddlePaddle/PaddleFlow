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
	SysParamNamePFRunID    = "PF_RUN_ID"
	SysParamNamePFFsID     = "PF_FS_ID"
	SysParamNamePFJobID    = "PF_JOB_ID"
	SysParamNamePFStepName = "PF_STEP_NAME"
	SysParamNamePFFsName   = "PF_FS_NAME"
	SysParamNamePFUserID   = "PF_USER_ID"
	SysParamNamePFUserName = "PF_USER_NAME"

	WfExtraInfoKeySource   = "Source" // pipelineID or yamlPath
	WfExtraInfoKeyUserName = "UserName"
	WfExtraInfoKeyFsName   = "FsName"
	WfExtraInfoKeyFsID     = "FsID"

	ParamTypeString = "string"
	ParamTypeFloat  = "float"
	ParamTypePath   = "path"

	WfParallelismDefault = 10
	WfParallelismMaximum = 20

	FieldParameters       = "parameters"
	FieldCommand          = "command"
	FieldEnv              = "env"
	FieldInputArtifacts   = "inputArtifacts"
	FieldOutputArtifacts  = "outputArtifacts"

	CacheStrategyConservative = "conservative"
	CacheStrategyAggressive   = "aggressive"
	CacheExpiredTimeNever     = "-1"
)