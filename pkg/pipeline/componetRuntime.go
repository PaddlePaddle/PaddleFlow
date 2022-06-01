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

package pipeline

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type RuntimeStatus = schema.JobStatus

var (
	StatusRuntimeInit        RuntimeStatus = schema.StatusJobInit
	StatusRunttimePending    RuntimeStatus = schema.StatusJobPending
	StatusRuntimeRunning     RuntimeStatus = schema.StatusJobRunning
	StatusRuntimeFailed      RuntimeStatus = schema.StatusJobFailed
	StatusRuntimeSucceeded   RuntimeStatus = schema.StatusJobSucceeded
	StatusRuntimeTerminating RuntimeStatus = schema.StatusJobTerminating
	StatusRuntimeTerminated  RuntimeStatus = schema.StatusJobTerminated
	StatusRuntimeCancelled   RuntimeStatus = schema.StatusJobCancelled
	StatusRuntimeSkipped     RuntimeStatus = schema.StatusJobSkipped
)

// 管理并发信息
type parallelismManager struct {
	ch chan struct{}
}

func NewParallelismManager(parallelism int) *parallelismManager {
	return &parallelismManager{
		ch: make(chan struct{}, parallelism),
	}
}

func (pm *parallelismManager) increase() {
	pm.ch <- struct{}{}
}

func (pm *parallelismManager) decrease() {
	<-pm.ch
}

// Run 的相关配置，其信息来源有以下几种:
// 1. workflowSource
// 2. 用户创建 Run 的请求体中 除 1 外的其余信息
// 3. Apiserver 或者 Parser 动态生成的信息，如 runID 等
type runConfig struct {
	// 1. workflowSource 中的信息
	*schema.WorkflowSource

	// 2. 来自于请求体中的信息
	fsID     string
	fsName   string
	userName string

	// pipelineID or yamlPath or md5sum of yamlRaw
	pplSource string

	// 3. 由 ApiServer 或者 Parser 动态生成的信息
	runID  string
	logger *logrus.Entry
	*parallelismManager

	// 用于与 APIServer 同步信息
	callbacks WorkflowCallbacks
}

func NewRunConfig(workflowSource *schema.WorkflowSource, fsID, fsName, userName, runID string, logger *logrus.Entry,
	callbacks WorkflowCallbacks, pplSource string) *runConfig {
	return &runConfig{
		WorkflowSource: workflowSource,

		fsID:      fsID,
		fsName:    fsName,
		userName:  userName,
		pplSource: pplSource,

		runID:              runID,
		logger:             logger,
		callbacks:          callbacks,
		parallelismManager: NewParallelismManager(workflowSource.Parallelism),
	}
}

// stepRuntime 和 DagRuntime 的基类
type componentRuntime struct {
	component schema.Component

	// 节点名字
	componentName string

	//表明 节点的第几次运行， 从 0 开始计算
	seq int

	// 类似根目录，由其所有祖先组件名加上自身名字组成，名字与名字之间以"." 分隔
	fullName string

	// runtime 在数据库的主键值，方便在存库是使用，应该由 apiserver 的回调函数返回，不应该自行设置
	pk int64

	// 用于监听终止信号的上下文
	ctx context.Context

	// 监听子节点事件的 channel
	receiveEventChildren <-chan WorkflowEvent

	// 将 event 同步至父节点的 channel
	sendEventToParent chan<- WorkflowEvent

	status RuntimeStatus

	// 是否处于终态
	done bool

	// 节点是否已经别调度执行
	started bool

	// run 级别的相关配置
	*runConfig
}

func NewComponentRuntime(name string, component schema.Component, seq int, ctx context.Context,
	eventChannel chan<- WorkflowEvent, config *runConfig) *componentRuntime {

	return &componentRuntime{
		componentName:        name,
		component:            component,
		seq:                  seq,
		ctx:                  ctx,
		sendEventToParent:    eventChannel,
		receiveEventChildren: make(<-chan WorkflowEvent),
		runConfig:            config,
	}
}

// 判断当前节点是否被 disabled
func (crt *componentRuntime) isDisabled() bool {
	for _, fullName := range crt.GetDisabled() {
		if fullName == crt.fullName {
			return true
		}
	}
	return false
}

func (crt *componentRuntime) isCancelled() bool {
	return crt.status == StatusRuntimeCancelled
}

func (crt *componentRuntime) isFailed() bool {
	return crt.status == StatusRuntimeFailed
}

func (crt *componentRuntime) isTerminated() bool {
	return crt.status == StatusRuntimeTerminated
}

// 判断当次运行是否已经处于终态
func (crt *componentRuntime) isDone() bool {
	return crt.done
}

// 用于判断是否已经调用过节点的 Start() 函数
func (crt *componentRuntime) isStarted() bool {
	return crt.started
}

// 更新节点状态
func (crt *componentRuntime) updateStatus(status RuntimeStatus) error {
	if crt.done {
		err := fmt.Errorf("cannot update the status of runtime[%s] for node[%s]，because the status of it is [%s]",
			crt.componentName, crt.fullName, crt.status)
		crt.logger.Errorln(err.Error())
		return err
	}

	crt.status = status

	if crt.status == StatusRuntimeCancelled || crt.status == StatusRuntimeFailed || crt.status == StatusRuntimeSucceeded || crt.status == StatusRuntimeSkipped {
		crt.done = true
	}
	return nil
}

// 获取 artifact 的路径
func (crt *componentRuntime) getArtifactPath(artName string) (string, error) {
	path, err := crt.GetInputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	path, err = crt.GetOutputArtifactPath(artName)
	if err == nil {
		return path, err
	}

	err = fmt.Errorf("the Component[%s] doesn't has an artifact named [%s]", crt.fullName, artName)
	return "", err
}

// 获取 输入artifact的存储路径
func (crt *componentRuntime) GetInputArtifactPath(artName string) (string, error) {
	path, ok := crt.component.GetArtifacts().Input[artName]
	if !ok {
		err := fmt.Errorf("the Component[%s] doesn't has an input artifact named [%s]", crt.fullName, artName)
		return "", err
	}

	return path, nil
}

// 获取输出artifact的存储路径
func (crt *componentRuntime) GetOutputArtifactPath(artName string) (string, error) {
	path, ok := crt.component.GetArtifacts().Output[artName]
	if !ok {
		err := fmt.Errorf("the Component[%s] doesn't has an output artifact named [%s]", crt.fullName, artName)
		return "", err
	}

	return path, nil
}

// 获取制定Artifact的内容
func (crt *componentRuntime) GetArtifactContent(artName string) (string, error) {
	path, err := crt.getArtifactPath(artName)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact[%s] of component[%s]: %v",
			artName, crt.fullName, err.Error())
		return "", err
	}

	fsHandler, err := handler.NewFsHandlerWithServer(crt.fsID, crt.logger)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact[%s] of component[%s]: %v",
			artName, crt.fullName, err.Error())
		return "", err
	}

	// 这里不在对 path 是否存在，以及其是否为一个文件做校验，因为如果不符合要求，fsHandler.ReadFsFile 会报错
	content, err := fsHandler.ReadFsFile(path)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact[%s] of component[%s]: %v",
			artName, crt.fullName, err.Error())
		return "", err
	}

	contentString := string(content)
	return contentString, nil

}

// 替换节点中，command，env，condition，loop_argument 中 parameter 或者 artifact 模版
func (crt *componentRuntime) resolveParameterAndArtifactTemplate() error {
	return nil
}

func (crt *componentRuntime) CalculateCondition() bool {
	//TODO:
	// 1、获取 condition 中parameter/ artifact 的模板
	// 2、进行参数替换
	// 3、计算Condition值
	return false
}
