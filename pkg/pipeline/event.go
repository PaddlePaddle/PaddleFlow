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

type WfEventType string
type WfEventValue string

const (
	// 事件类型
	WfEventNoraml WfEventType = "Normal"   // 正常事件类型，包括状态更新等
	WfEventWarn   WfEventType = "Warnning" // 警告事件类型，不影响业务运行，但需要关注
	WfEventError  WfEventType = "Error"    // 异常事件类型，影响业务运行，例如其他模块函数返回异常

	// 事件值
	WfEventFailureOptionsTriggered WfEventValue = "FailureOpitons"
	WfEventJobUpdate               WfEventValue = "JobUpdate"
	WfEventRunUpdate               WfEventValue = "RunUpdate"
	WfEventDagUpdate               WfEventValue = "DagUpdate"
	WfEventJobSubmitErr            WfEventValue = "JobSubmitErr"
	WfEventJobWatchErr             WfEventValue = "JobWatchErr"
	WfEventJobStopErr              WfEventValue = "JobStopErr"
)

type WorkflowEvent struct {
	Type    WfEventType
	Event   WfEventValue
	Message string
	Extra   map[string]interface{}
}

// 实例化
func NewWorkflowEvent(e WfEventValue, msg string, extra map[string]interface{}) *WorkflowEvent {
	wfe := &WorkflowEvent{
		Type:    WfEventNoraml,
		Event:   e,
		Message: msg,
		Extra:   extra,
	}
	return wfe
}

// 是否Job更新事件
func (wfe *WorkflowEvent) isJobUpdate() bool {
	return wfe.Event == WfEventJobUpdate
}

// 是否Run更新事件
func (wfe *WorkflowEvent) isRunUpdate() bool {
	return wfe.Event == WfEventRunUpdate
}

// 是否Job Watch异常事件
func (wfe *WorkflowEvent) isJobWatchErr() bool {
	return wfe.Event == WfEventJobWatchErr
}

// 是否Job Submit异常事件
func (wfe *WorkflowEvent) isJobSubmitErr() bool {
	return wfe.Event == WfEventJobSubmitErr
}

// 是否Job Stop异常事件
func (wfe *WorkflowEvent) isJobStopErr() bool {
	return wfe.Event == WfEventJobStopErr
}

// 是否是 FailureOptionsTriggered 事件
func (wfe *WorkflowEvent) isFailureOptionsTriggered() bool {
	return wfe.Event == WfEventFailureOptionsTriggered
}

// 获取Job更新信息
func (wfe *WorkflowEvent) getJobUpdate() (map[string]interface{}, bool) {
	return wfe.Extra, wfe.isJobUpdate()
}

// 获取Run更新信息
func (wfe *WorkflowEvent) getRunUpdate() (map[string]interface{}, bool) {
	return wfe.Extra, wfe.isRunUpdate()
}
