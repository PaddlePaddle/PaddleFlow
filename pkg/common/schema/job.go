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

package schema

import (
	"fmt"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	sparkoperatorv1beta2 "paddleflow/pkg/apis/spark-operator/sparkoperator.k8s.io/v1beta2"
)

type JobType string
type ActionOnJob string
type JobStatus string

const (
	EnvJobType      = "PF_JOB_TYPE"
	EnvJobQueueName = "PF_JOB_QUEUE_NAME"
	EnvJobNamespace = "PF_JOB_NAMESPACE"
	EnvJobUserName  = "PF_USER_NAME"
	EnvJobFsID      = "PF_FS_ID"
	EnvJobPVCName   = "PF_JOB_PVC_NAME"
	EnvJobPriority  = "PF_JOB_PRIORITY"
	EnvJobMode      = "PF_JOB_MODE"
	// EnvJobYamlPath Additional configuration for a specific job
	EnvJobYamlPath = "PF_JOB_YAML_PATH"

	// EnvJobModePS env
	EnvJobModePS          = "PS"
	EnvJobPSPort          = "PF_JOB_PS_PORT"
	EnvJobPServerReplicas = "PF_JOB_PSERVER_REPLICAS"
	EnvJobPServerFlavour  = "PF_JOB_PSERVER_FLAVOUR"
	EnvJobPServerCommand  = "PF_JOB_PSERVER_COMMAND"
	EnvJobWorkerReplicas  = "PF_JOB_WORKER_REPLICAS"
	EnvJobWorkerFlavour   = "PF_JOB_WORKER_FLAVOUR"
	EnvJobWorkerCommand   = "PF_JOB_WORKER_COMMAND"

	// EnvJobModeCollective env
	EnvJobModeCollective = "Collective"
	EnvJobReplicas       = "PF_JOB_REPLICAS"
	EnvJobFlavour        = "PF_JOB_FLAVOUR"

	// EnvJobModePod env reuse EnvJobReplicas and EnvJobFlavour
	EnvJobModePod = "Pod"

	// spark job env
	EnvJobSparkMainFile    = "PF_JOB_SPARK_MAIN_FILE"
	EnvJobSparkMainClass   = "PF_JOB_SPARK_MAIN_CLASS"
	EnvJobSparkArguments   = "PF_JOB_SPARK_ARGUMENTS"
	EnvJobDriverFlavour    = "PF_JOB_DRIVER_FLAVOUR"
	EnvJobExecutorReplicas = "PF_JOB_EXECUTOR_REPLICAS"
	EnvJobExecutorFlavour  = "PF_JOB_EXECUTOR_FLAVOUR"

	TypeVcJob    JobType = "vcjob"
	TypeSparkJob JobType = "spark"

	StatusJobPending     JobStatus = "pending"
	StatusJobRunning     JobStatus = "running"
	StatusJobFailed      JobStatus = "failed"
	StatusJobSucceeded   JobStatus = "succeeded"
	StatusJobTerminating JobStatus = "terminating"
	StatusJobTerminated  JobStatus = "terminated"
	StatusJobCancelled   JobStatus = "cancelled"
	StatusJobCached      JobStatus = "cached" // 表示这个步骤使用cache，跳过运行

	// job priority
	EnvJobVeryLowPriority  = "VERY_LOW"
	EnvJobLowPriority      = "LOW"
	EnvJobNormalPriority   = "NORMAL"
	EnvJobHighPriority     = "HIGH"
	EnvJobVeryHighPriority = "VERY_HIGH"

	// priority class
	PriorityClassVeryLow  = "very-low"
	PriorityClassLow      = "low"
	PriorityClassNormal   = "normal"
	PriorityClassHigh     = "high"
	PriorityClassVeryHigh = "very-high"

	JobOwnerLabel = "owner"
	JobOwnerValue = "paddleflow"
	JobIDLabel    = "paddleflow-job-id"

	VolcanoJobNameLabel = "volcano.sh/job-name"

	JobPrefix            = "job"
	DefaultSchedulerName = "volcano"
	DefaultFSMountPath   = "/home/paddleflow/storage/mnt"
)

const (
	Update    ActionOnJob = "update"
	Delete    ActionOnJob = "delete"
	Terminate ActionOnJob = "terminate"
)

func GetSparkJobStatus(state sparkoperatorv1beta2.ApplicationStateType) (JobStatus, error) {
	status := JobStatus("")
	switch state {
	case sparkoperatorv1beta2.NewState, sparkoperatorv1beta2.SubmittedState:
		status = StatusJobPending
	case sparkoperatorv1beta2.RunningState, sparkoperatorv1beta2.SucceedingState, sparkoperatorv1beta2.FailingState,
		sparkoperatorv1beta2.InvalidatingState, sparkoperatorv1beta2.PendingRerunState:
		status = StatusJobRunning
	case sparkoperatorv1beta2.CompletedState:
		status = StatusJobSucceeded
	case sparkoperatorv1beta2.FailedState, sparkoperatorv1beta2.FailedSubmissionState, sparkoperatorv1beta2.UnknownState:
		status = StatusJobFailed
	}

	if status == "" {
		return status, fmt.Errorf("unexpected spark application status [%s]\n", state)
	}
	return status, nil
}

func GetVCJobStatus(phase batchv1alpha1.JobPhase) (JobStatus, error) {
	status := JobStatus("")
	switch phase {
	case batchv1alpha1.Pending:
		status = StatusJobPending
	case batchv1alpha1.Running, batchv1alpha1.Restarting, batchv1alpha1.Completing:
		status = StatusJobRunning
	case batchv1alpha1.Terminating, batchv1alpha1.Aborting:
		status = StatusJobTerminating
	case batchv1alpha1.Completed:
		status = StatusJobSucceeded
	case batchv1alpha1.Aborted:
		status = StatusJobTerminated
	case batchv1alpha1.Failed, batchv1alpha1.Terminated:
		status = StatusJobFailed
	}

	if status == "" {
		return status, fmt.Errorf("unexpected vcjob status [%s]\n", phase)
	}
	return status, nil
}
