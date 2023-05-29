// Due to kubernetes version conflict, AITrainingJob define referenced from baidu cce.
/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package v1

import (
	autoscalingv2beta2 "k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ReplicaName string

// The type of Replica.
type ReplicaType string

const (
	ReplicaMaster ReplicaType = "master"
	ReplicaWorker ReplicaType = "worker"
	ReplicaNone   ReplicaType = ""
)

type FrameworkType string

const (
	FrameworkTypePaddle  FrameworkType = "paddle"
	FrameworkTypeHorovod FrameworkType = "horovod"
)

// +k8s:deepcopy-gen=true
// ReplicaSpec is a description of the job replica.
type ReplicaSpec struct {
	ReplicaType ReplicaType `json:"replicaType,omitempty"`
	MinReplicas *int32      `json:"minReplicas,omitempty"`
	MaxReplicas *int32      `json:"maxReplicas,omitempty"`
	Replicas    *int32      `json:"replicas,omitempty"`

	Template corev1.PodTemplateSpec `json:"template,omitempty"`

	FailPolicy     EndingPolicy `json:"failPolicy,omitempty"`
	CompletePolicy EndingPolicy `json:"completePolicy,omitempty"`

	RestartLimit        *int32                `json:"restartLimit,omitempty"`
	RestartTimeout      *int32                `json:"restartTimeout,omitempty"`
	RestartPolicy       RestartPolicy         `json:"restartPolicy,omitempty"`
	RestartScope        RestartScope          `json:"restartScope,omitempty"`
	FaultTolerantPolicy []FaultTolerantPolicy `json:"faultTolerantPolicy"`
	ScaleInPods         []string              `json:"scaleInpods,omitempty"`
	ScaleTimeout        *int32                `json:"scaleTimeout,omitempty"`
	ScaleCancle         bool                  `json:"scaleCancle,omitempty"`
}

// +k8s:deepcopy-gen=true
// FaultTolerantPolicy is a description of the job FaultTolerant Policy.
type FaultTolerantPolicy struct {
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
	RestartScope  RestartScope  `json:"restartScope,omitempty"`
	// 格式: "1-100", "1,2,3,4,5,6", "1"
	ExitCodes        ExitCodes        `json:"exitCodes,omitempty"`
	ExceptionalEvent ExceptionalEvent `json:"exceptionalEvent,omitempty"`
}

type ExitCodes string
type ExceptionalEvent string
type RestartPolicy string
type RestartScope string

const (
	RestartPolicyAlways     RestartPolicy = "Always"
	RestartPolicyOnFailure  RestartPolicy = "OnFailure"
	RestartPolicyOnNodeFail RestartPolicy = "OnNodeFail"
	RestartPolicyNever      RestartPolicy = "Never"
	// `ExitCode` policy means that user should add exit code by themselves,
	// The job operator will check these exit codes to
	// determine the behavior when an error occurs:
	// - 1-127: permanent error, do not restart.
	// - 128-255: retryable error, will restart the pod.
	RestartPolicyExitCode               RestartPolicy = "ExitCode"
	RestartPolicyOnNodeFailWithExitCode RestartPolicy = "OnNodeFailWithExitCode"
	RestartScopeAll                     RestartScope  = "All"
	RestartScopeReplica                 RestartScope  = "Replica"
	RestartScopePod                     RestartScope  = "Pod"
)

type ReplicaStatus struct {
	// The number of pending pods.
	Pending int32 `json:"pending,omitempty"`
	// The number of scheduled pods.
	Scheduled int32 `json:"scheduled,omitempty"`
	// The number of actively running pods.
	Active int32 `json:"active,omitempty"`
	// The number of pods which reached phase Succeeded.
	Succeeded int32 `json:"succeeded,omitempty"`
	// The number of restarting pods.
	Restarting int32 `json:"restarting,omitempty"`
	// The number of pods which reached phase Failed.
	Failed int32 `json:"failed,omitempty"`
}

type EndingPolicy string

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=aitrainingjob

// AITrainingJob is a specification for a AITrainingJob resource
type AITrainingJob struct {
	metav1.TypeMeta `json:",inline"`
	// ObjectMeta is metadata that all persisted resources must have, which includes all objects
	// users must create.
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec defines the specification of the desired behavior of the TrainingJob.
	Spec TrainingJobSpec `json:"spec,omitempty"`
	// Status is the most recently observed status of the TrainingJob.
	Status TrainingJobStatus `json:"status,omitempty"`
}

// TrainingJobSpec is the spec for a AITrainingJob resource
type TrainingJobSpec struct {
	// Specify the framework, eg: tensorflow / paddlepaddle
	FrameworkType FrameworkType `json:"frameworkType,omitempty"`
	// Identify whether fault tolerance is required, 是否开启弹性
	ElasticFaultTolerant bool `json:"faultTolerant,omitempty"`
	// 动态弹性的判断指标
	Metrics []autoscalingv2beta2.MetricSpec `json:"metrics,omitempty"`
	// Specify the job priority
	Priority string `json:"priority,omitempty"`
	// Specify the Kubernetes scheduler name
	SchedulerName string `json:"schedulerName,omitempty"`
	// Time limit for the job (in seconds)
	TimeLimit *int64 `json:"timeLimit,omitempty"`
	// Define the policy for cleaning up pods after the trainingjob completes
	CleanPodPolicy *CleanPodPolicy `json:"cleanPodPolicy,omitempty"`
	// Define the policy for fail
	FailPolicy EndingPolicy `json:"failPolicy,omitempty"`
	// Define the policy for complete
	CompletePolicy EndingPolicy `json:"completePolicy,omitempty"`
	// Specify the TrainingJob configuration
	ReplicaSpecs map[ReplicaName]*ReplicaSpec `json:"replicaSpecs"`
	Plugin       map[string][]string          `json:"plugin,,omitempty"`
}

type CleanPodPolicy string

const (
	// Delete all pods/services, when job is finished
	CleanPodPolicyAll CleanPodPolicy = "All"
	// Delete nothing
	CleanPodPolicyNone CleanPodPolicy = "None"
)

// +k8s:deepcopy-gen=true
// TrainingJobStatus is the status for a AITrainingJob resource
type TrainingJobStatus struct {
	// The phase of a job is a simple, high-level summary of where the Job is in its lifecycle.
	Phase TrainingJobPhase `json:"phase"`
	// An array of current job conditions
	Conditions []TrainingJobCondition `json:"conditions"`
	// detail status of echo replica resource
	ReplicaStatuses map[ReplicaName]*ReplicaStatus `json:"replicaStatuses"`
	// The times of pods restart.
	RestartCountes map[ReplicaName]int32 `json:"RestartCount,omitempty"`
	// ReplicaName need to restart
	RestartReplicaName ReplicaName `json:"restartReplicaName,omitempty"`
	// RestartScope means the scope of restart
	RestartScope RestartScope `json:"restartScope,omitempty"`
	// Represents the time when the job was acknowledged by the job controller
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// Represents the time when the job start running.
	StartRunningTime *metav1.Time `json:"startRunningTime,omitempty"`
	// Represents the time when the job was completed
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// Represents the last time when the job was reconciled.
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`
	// scale skip pod index
	ScaleInPods map[ReplicaName][]int `json:"scaleInPods,omitempty"`
	// ReplicaName need to scale
	ScaleReplicaName ReplicaName `json:"scaleReplicaName,omitempty"`
	// scaling pods
	ScalingPods []string `json:"scalingPods,omitempty"`
	// scale pod index
	ScaleIndex   []*int32     `json:"scaleindex,omitempty"`
	ScaleHistory []*ScaleInfo `json:"scaleHistory,omitempty"`
	ScaleInfo    *ScaleInfo   `json:"scaleinfo,omitempty"`
	// targetWorker for job
	TargetWorkers map[ReplicaName][]string `json:"targetWorker"`
	// Current job replicas
	CurrentReplicas map[ReplicaName]*int32 `json:"currentReplicas"`
	// LabelSelector for hpa
	LabelSelector string `json:"labelSelector,omitempty"`
}

type ScaleInfo struct {
	Operation string                 `json:"operation"`
	Number    *int32                 `json:"num,omitempty"`
	Pods      []string               `json:"pods,omitempty"`
	Roletype  string                 `json:"roletype"`
	Version   *int32                 `json:"version"`
	Status    TrainingJobScaleStatus `json:"Status,omitempty"`
	Timeout   *metav1.Time           `json:"timeout"`
}

// trainingJob scale status
type TrainingJobScaleStatus string

// trainingJob scaletype scalein or scaleout
type TrainingJobScaleType string

// TrainingJobPhase is the phase of AITrainingJob
type TrainingJobPhase string

const (
	// None means the job has been accepted by the system
	TrainingJobPhaseNone TrainingJobPhase = ""
	// Pending means one or more of the pods/services has not been scheduled.
	TrainingJobPhasePending = "Pending"
	// Creating means all pods/services of this job have been successfully scheduled,
	// but one or more of the pods/services has not been launched.
	TrainingJobPhaseCreating = "Creating"
	// Running means all pods/services have been launched.
	TrainingJobPhaseRunning = "Running"
	// Succeed means all pods of this job reached phase success.
	TrainingJobPhaseSucceeded = "Succeed"
	// Failed means one or more pods of this job reached phase failed.
	TrainingJobPhaseFailed = "Failed"
	// Timeout means the job runs over the preset maximum run time
	TrainingJobPhaseTimeout = "Timeout"
	// TODO: Restarting means the job is restarting
	TrainingJobPhaseRestarting = "Restarting"
	// Terminating means the job have been terminated, but resources are not yet fully released
	TrainingJobPhaseTerminating = "Terminating"
	// Preempted means the job have been preempted, and resources were fully released
	TrainingJobPhasePreempted = "Preempted"
	// NodeFail means the node is failed
	TrainingJobPhaseNodeFail = "NodeFail"
	// trainingjob phase scaling
	TrainingJobPhaseScaling = "Scaling"
	// trainingjob scale in
	TrainingJobScaleIn TrainingJobScaleType = "scalein"
	// trainingjob scale out
	TrainingJobScaleOut TrainingJobScaleType = "scaleout"
	// TrainingJobScaleStatus
	TrainingJobScaleSucceed TrainingJobScaleStatus = "Succeed"
	TrainingJobScaleScaling TrainingJobScaleStatus = "Scaling"
	TrainingJobScaleCancled TrainingJobScaleStatus = "Cancled"
	TrainingJobScaleFailed  TrainingJobScaleStatus = "Failed"
	TrainingJobScaleTimeout TrainingJobScaleStatus = "Timeout"
)

type TrainingJobRestartEvent string

const (
	TrainingJobPodForceDeleteEvent TrainingJobRestartEvent = "PodForceDeleted"
)

// +k8s:deepcopy-gen=true
// TrainingJobCondition describes the state of the job at a certain point.
type TrainingJobCondition struct {
	// Type is the type of the condition.
	Type TrainingJobPhase `json:"type"`
	// Status is the status of the condition.
	// Can be True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// Unique, one-word, CamelCase reason for the condition's last transition
	Reason string `json:"reason,omitempty"`
	// Human-readable message indicating details about last transition.
	Message string `json:"message,omitempty"`
	// Last time we probed the condition.
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}
