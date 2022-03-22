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
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type JobType string
type ActionOnJob string
type JobStatus string

const (
	EnvJobType        = "PF_JOB_TYPE"
	EnvJobQueueName   = "PF_JOB_QUEUE_NAME"
	EnvJobQueueID     = "PF_JOB_QUEUE_ID"
	EnvJobClusterName = "PF_JOB_CLUSTER_NAME"
	EnvJobClusterID   = "PF_JOB_CLUSTER_ID"
	EnvJobNamespace   = "PF_JOB_NAMESPACE"
	EnvJobUserName    = "PF_USER_NAME"
	EnvJobFsID        = "PF_FS_ID"
	EnvJobPVCName     = "PF_JOB_PVC_NAME"
	EnvJobPriority    = "PF_JOB_PRIORITY"
	EnvJobMode        = "PF_JOB_MODE"
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

	TypeVcJob     JobType = "vcjob"
	TypeSparkJob  JobType = "spark"
	TypePaddleJob JobType = "paddlejob"
	TypePodJob    JobType = "pod"

	StatusJobInit        JobStatus = "init"
	StatusJobPending     JobStatus = "pending"
	StatusJobRunning     JobStatus = "running"
	StatusJobFailed      JobStatus = "failed"
	StatusJobSucceeded   JobStatus = "succeeded"
	StatusJobTerminating JobStatus = "terminating"
	StatusJobTerminated  JobStatus = "terminated"
	StatusJobCancelled   JobStatus = "cancelled"
	StatusJobSkipped   JobStatus = "skipped"

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

	VolcanoJobNameLabel  = "volcano.sh/job-name"
	SparkAPPJobNameLabel = "sparkoperator.k8s.io/app-name"

	JobPrefix            = "job"
	DefaultSchedulerName = "volcano"
	DefaultFSMountPath   = "/home/paddleflow/storage/mnt"
)

const (
	Update    ActionOnJob = "update"
	Delete    ActionOnJob = "delete"
	Terminate ActionOnJob = "terminate"
)

func IsImmutableJobStatus(status JobStatus) bool {
	switch status {
	case StatusJobSucceeded, StatusJobFailed, StatusJobTerminated:
		return true
	default:
		return false
	}
}

type PFJobConf interface {
	GetName() string
	GetEnv() map[string]string
	GetCommand() string
	GetImage() string

	GetPriority() string
	SetPriority(string)

	GetQueueName() string
	GetQueueID() string
	GetClusterName() string
	GetClusterID() string
	GetUserName() string
	GetFS() string
	GetYamlPath() string
	GetNamespace() string
	GetJobMode() string

	GetFlavour() string
	GetPSFlavour() string
	GetWorkerFlavour() string

	SetQueueID(string)
	SetClusterID(string)
	SetNamespace(string)
	SetEnv(string, string)
	Type() JobType
}

type Conf struct {
	Name    string            `json:"name"`
	Env     map[string]string `json:"env"`
	Command string            `json:"command"`
	Image   string            `json:"image"`
}

func (c *Conf) GetName() string {
	return c.Name
}

func (c *Conf) GetEnv() map[string]string {
	return c.Env
}

func (c *Conf) GetCommand() string {
	return c.Command
}

func (c *Conf) GetWorkerCommand() string {
	return c.Env[EnvJobWorkerCommand]
}

func (c *Conf) GetPSCommand() string {
	return c.Env[EnvJobPServerCommand]
}

func (c *Conf) GetImage() string {
	return c.Image
}

func (c *Conf) GetPriority() string {
	return c.Env[EnvJobPriority]
}

func (c *Conf) SetPriority(pc string) {
	c.Env[EnvJobPriority] = pc
}

func (c *Conf) GetQueueName() string {
	return c.Env[EnvJobQueueName]
}

func (c *Conf) GetClusterName() string {
	return c.Env[EnvJobClusterName]
}

func (c *Conf) GetUserName() string {
	return c.Env[EnvJobUserName]
}

func (c *Conf) GetFS() string {
	return c.Env[EnvJobFsID]
}

func (c *Conf) GetYamlPath() string {
	return c.Env[EnvJobYamlPath]
}

func (c *Conf) GetNamespace() string {
	return c.Env[EnvJobNamespace]
}

func (c *Conf) SetNamespace(ns string) {
	c.Env[EnvJobNamespace] = ns
}

func (c *Conf) Type() JobType {
	return JobType(c.Env[EnvJobType])
}

func (c *Conf) GetJobMode() string {
	return c.Env[EnvJobMode]
}

func (c *Conf) GetJobReplicas() string {
	return c.Env[EnvJobReplicas]
}

func (c *Conf) GetWorkerReplicas() string {
	return c.Env[EnvJobWorkerReplicas]
}

func (c *Conf) GetPSReplicas() string {
	return c.Env[EnvJobPServerReplicas]
}

func (c *Conf) GetJobExecutorReplicas() string {
	return c.Env[EnvJobExecutorReplicas]
}

func (c *Conf) GetFlavour() string {
	return c.Env[EnvJobFlavour]
}

func (c *Conf) GetPSFlavour() string {
	return c.Env[EnvJobPServerFlavour]
}

func (c *Conf) GetWorkerFlavour() string {
	return c.Env[EnvJobWorkerFlavour]
}

func (c *Conf) SetFlavour(flavourKey string) {
	c.Env[EnvJobFlavour] = flavourKey
}

func (c *Conf) SetPSFlavour(flavourKey string) {
	c.Env[EnvJobPServerFlavour] = flavourKey
}

func (c *Conf) SetWorkerFlavour(flavourKey string) {
	c.Env[EnvJobWorkerFlavour] = flavourKey
}

func (c *Conf) SetEnv(name, value string) {
	c.Env[name] = value
}

func (c *Conf) GetQueueID() string {
	return c.Env[EnvJobQueueID]
}

func (c *Conf) SetQueueID(id string) {
	c.Env[EnvJobQueueID] = id
}

func (c *Conf) GetClusterID() string {
	return c.Env[EnvJobClusterID]
}

func (c *Conf) SetClusterID(id string) {
	c.Env[EnvJobClusterID] = id
}

// Scan for gorm
func (s *Conf) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("Conf scan failed")
	}
	err := json.Unmarshal(b, s)
	if err != nil {
		return err
	}
	return nil
}

// Value for gorm
func (s Conf) Value() (driver.Value, error) {
	value, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return value, nil
}
