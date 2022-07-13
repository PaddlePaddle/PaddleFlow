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

type NodeType string
type ViewType string

const (
	// 如果有增加新的系统变量，记得需要同步更新 SysParamNameList
	SysParamNamePFRunID        = "PF_RUN_ID"
	SysParamNamePFStepName     = "PF_STEP_NAME"
	SysParamNamePFUserName     = "PF_USER_NAME"
	SysParamNamePFLoopArgument = "PF_LOOP_ARGUMENT"
	SysParamNamePFMountPath    = "PF_MOUNT_PATH"

	PF_PARENT = "PF_PARENT"

	WfExtraInfoKeySource   = "Source" // pipelineID or yamlPath
	WfExtraInfoKeyUserName = "UserName"
	WfExtraInfoKeyFsName   = "FsName"
	WfExtraInfoKeyFsID     = "FsID"

	ParamTypeString = "string"
	ParamTypeFloat  = "float"
	ParamTypePath   = "path"
	ParamTypeInt    = "int"
	ParamTypeList   = "list"

	WfParallelismDefault = 10
	WfParallelismMaximum = 20

	FieldParameters      = "parameters"
	FieldCommand         = "command"
	FieldEnv             = "env"
	FieldInputArtifacts  = "inputArtifacts"
	FieldOutputArtifacts = "outputArtifacts"
	FieldCondition       = "condition"
	FieldLoopArguemt     = "loop_argument"

	CacheStrategyConservative = "conservative"
	CacheStrategyAggressive   = "aggressive"
	CacheExpiredTimeNever     = "-1"

	NodeTypeEntrypoint  NodeType = "entrypoints"
	NodeTypePostProcess NodeType = "postProcess"

	ViewTypeEntrypoint  ViewType = "entrypoints"
	ViewTypePostProcess ViewType = "postProcess"

	RegExpUpstreamTpl          = `^\{\{(\s)*[a-zA-Z0-9-_]+\.[a-zA-Z0-9_]+(\s)*\}\}$`  // {{xx-xx.xx_xx}}
	RegExpCurTpl               = `^\{\{(\s)*([a-zA-Z0-9_]+)(\s)*\}\}$`                // {{xx_xx}}
	RegExpIncludingUpstreamTpl = `\{\{(\s)*([a-zA-Z0-9-_]+\.[a-zA-Z0-9_]+)(\s)*\}\}`  // 包含 {{xx-xx.xx_xx}}
	RegExpIncludingTpl         = `\{\{(\s)*([a-zA-Z0-9-_]*\.?[a-zA-Z0-9_]+)(\s)*\}\}` // 包含 {{xx-xx.xx_xx}} 或 {xx_xx}
	RegExpIncludingCurTpl      = `\{\{(\s)*([a-zA-Z0-9_]+)(\s)*\}\}`                  // 包含 {{xx_xx}}

	// condition 字段中引用的 artifact 支持最大空间， 单位为 byte
	ConditionArtifactMaxSize = 1024 // 1KB

	// loop_argument 字段中引用的 artifact 支持最大空间， 单位为 byte
	LoopArgumentArtifactMaxSize = 1024 * 1024 // 1MB

	// dagID 中随机码的位数
	DagIDRandCodeNum = 16

	// artifact 挂载路径的父目录
	ArtMountDir = "/tmp"
)

var SysParamNameList []string = []string{
	SysParamNamePFRunID,
	SysParamNamePFStepName,
	SysParamNamePFUserName,
	SysParamNamePFLoopArgument,
	SysParamNamePFMountPath,
}
