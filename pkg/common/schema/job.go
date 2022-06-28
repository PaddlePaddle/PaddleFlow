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

type JobType string
type ActionType string
type JobStatus string
type TaskStatus string
type Framework string
type MemberRole string

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
	EnvJobYamlPath  = "PF_JOB_YAML_PATH"
	EnvIsCustomYaml = "PF_IS_CUSTOM_YAML"

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

	// TODO move to framework
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
	StatusJobSkipped     JobStatus = "skipped"

	StatusTaskPending   TaskStatus = "pending"
	StatusTaskRunning   TaskStatus = "running"
	StatusTaskSucceeded TaskStatus = "succeeded"
	StatusTaskFailed    TaskStatus = "failed"

	RoleMaster   MemberRole = "master"
	RoleWorker   MemberRole = "worker"
	RoleDriver   MemberRole = "driver"
	RoleExecutor MemberRole = "executor"
	RolePServer  MemberRole = "pserver"
	RolePWorker  MemberRole = "pworker"

	TypeSingle      JobType = "single"
	TypeDistributed JobType = "distributed"
	TypeWorkflow    JobType = "workflow"

	FrameworkSpark      Framework = "spark"
	FrameworkMPI        Framework = "mpi"
	FrameworkTF         Framework = "tensorflow"
	FrameworkPytorch    Framework = "pytorch"
	FrameworkPaddle     Framework = "paddle"
	FrameworkStandalone Framework = "standalone"

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
	JobTTLSeconds = "padleflow/job-ttl-seconds"

	VolcanoJobNameLabel  = "volcano.sh/job-name"
	QueueLabelKey        = "volcano.sh/queue-name"
	SparkAPPJobNameLabel = "sparkoperator.k8s.io/app-name"

	JobPrefix            = "job"
	DefaultSchedulerName = "volcano"
	DefaultFSMountPath   = "/home/paddleflow/storage/mnt"

	// EnvPaddleParaJob defines env for Paddle Para Job
	EnvPaddleParaJob            = "PF_PADDLE_PARA_JOB"
	EnvPaddleParaPriority       = "PF_PADDLE_PARA_PRIORITY"
	EnvPaddleParaConfigHostFile = "PF_PADDLE_PARA_CONFIG_FILE"
	// PaddleParaVolumeName defines config for Paddle Para Pod
	PaddleParaVolumeName            = "paddle-para-conf-volume"
	PaddleParaAnnotationKeyJobName  = "paddle-para/job-name"
	PaddleParaAnnotationKeyPriority = "paddle-para/priority"
	PaddleParaEnvJobName            = "FLAGS_job_name"
	PaddleParaEnvGPUConfigFile      = "GPU_CONFIG_FILE"
	PaddleParaGPUConfigFilePath     = "/opt/paddle/para/gpu_config.json"
)

const (
	Create    ActionType = "create"
	Update    ActionType = "update"
	Delete    ActionType = "delete"
	Terminate ActionType = "terminate"
)

func IsImmutableJobStatus(status JobStatus) bool {
	switch status {
	case StatusJobSucceeded, StatusJobFailed, StatusJobTerminated, StatusJobSkipped, StatusJobCancelled:
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

	// Deprecated
	GetFS() string
	SetFS(string)

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
	SetLabels(string, string)
	SetAnnotations(string, string)

	Type() JobType
}

type Conf struct {
	Name string `json:"name"`
	// 存储资源
	FileSystem      FileSystem   `json:"fileSystem,omitempty"`
	ExtraFileSystem []FileSystem `json:"extraFileSystem,omitempty"`
	// 计算资源
	Flavour  Flavour `json:"flavour,omitempty"`
	Priority string  `json:"priority"`
	QueueID  string  `json:"queueID"`
	// 运行时需要的参数
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Env         map[string]string `json:"env,omitempty"`
	Command     string            `json:"command,omitempty"`
	Image       string            `json:"image"`
	Port        int               `json:"port,omitempty"`
	Args        []string          `json:"args,omitempty"`
}

// FileSystem indicate PaddleFlow
type FileSystem struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name"`
	MountPath string `json:"mountPath,omitempty"`
	SubPath   string `json:"subPath,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
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
	c.preCheckEnv()
	return c.Env[EnvJobWorkerCommand]
}

func (c *Conf) GetPSCommand() string {
	c.preCheckEnv()
	return c.Env[EnvJobPServerCommand]
}

func (c *Conf) GetImage() string {
	return c.Image
}

func (c *Conf) GetPriority() string {
	c.preCheckEnv()
	return c.Env[EnvJobPriority]
}

func (c *Conf) SetPriority(pc string) {
	c.preCheckEnv()
	c.Env[EnvJobPriority] = pc
}

func (c *Conf) GetQueueName() string {
	c.preCheckEnv()
	return c.Env[EnvJobQueueName]
}

// SetQueueName set queue name
func (c *Conf) SetQueueName(queueName string) {
	c.preCheckEnv()
	c.Env[EnvJobQueueName] = queueName
}

func (c *Conf) GetClusterName() string {
	c.preCheckEnv()
	return c.Env[EnvJobClusterName]
}

func (c *Conf) GetUserName() string {
	c.preCheckEnv()
	return c.Env[EnvJobUserName]
}

func (c *Conf) SetUserName(userName string) {
	c.preCheckEnv()
	c.Env[EnvJobUserName] = userName
}

// Deprecated
func (c *Conf) GetFS() string {
	c.preCheckEnv()
	return c.Env[EnvJobFsID]
}

// SetFS sets the filesystem id
func (c *Conf) SetFS(fsID string) {
	c.preCheckEnv()
	c.Env[EnvJobFsID] = fsID
}

func (c *Conf) GetYamlPath() string {
	c.preCheckEnv()
	return c.Env[EnvJobYamlPath]
}

func (c *Conf) GetNamespace() string {
	c.preCheckEnv()
	return c.Env[EnvJobNamespace]
}

func (c *Conf) SetNamespace(ns string) {
	c.preCheckEnv()
	c.Env[EnvJobNamespace] = ns
}

func (c *Conf) Type() JobType {
	c.preCheckEnv()
	return JobType(c.Env[EnvJobType])
}

func (c *Conf) GetJobMode() string {
	c.preCheckEnv()
	return c.Env[EnvJobMode]
}

func (c *Conf) GetJobReplicas() string {
	c.preCheckEnv()
	return c.Env[EnvJobReplicas]
}

func (c *Conf) GetWorkerReplicas() string {
	c.preCheckEnv()
	return c.Env[EnvJobWorkerReplicas]
}

func (c *Conf) GetPSReplicas() string {
	c.preCheckEnv()
	return c.Env[EnvJobPServerReplicas]
}

func (c *Conf) GetJobExecutorReplicas() string {
	c.preCheckEnv()
	return c.Env[EnvJobExecutorReplicas]
}

func (c *Conf) GetFlavour() string {
	c.preCheckEnv()
	return c.Env[EnvJobFlavour]
}

func (c *Conf) GetPSFlavour() string {
	c.preCheckEnv()
	return c.Env[EnvJobPServerFlavour]
}

func (c *Conf) GetWorkerFlavour() string {
	c.preCheckEnv()
	return c.Env[EnvJobWorkerFlavour]
}

func (c *Conf) SetFlavour(flavourKey string) {
	c.preCheckEnv()
	c.Env[EnvJobFlavour] = flavourKey
}

func (c *Conf) SetPSFlavour(flavourKey string) {
	c.preCheckEnv()
	c.Env[EnvJobPServerFlavour] = flavourKey
}

func (c *Conf) SetWorkerFlavour(flavourKey string) {
	c.preCheckEnv()
	c.Env[EnvJobWorkerFlavour] = flavourKey
}

func (c *Conf) SetEnv(name, value string) {
	c.preCheckEnv()
	c.Env[name] = value
}

func (c *Conf) GetQueueID() string {
	return c.QueueID
}

func (c *Conf) SetQueueID(id string) {
	c.QueueID = id
}

func (c *Conf) GetClusterID() string {
	c.preCheckEnv()
	return c.Env[EnvJobClusterID]
}

func (c *Conf) SetClusterID(id string) {
	c.preCheckEnv()
	c.Env[EnvJobClusterID] = id
}

func (c *Conf) SetLabels(k, v string) {
	c.preCheck()
	c.Labels[k] = v
}

func (c *Conf) SetAnnotations(k, v string) {
	c.preCheck()
	c.Annotations[k] = v
}

func (c *Conf) preCheck() {
	if c.Labels == nil {
		c.Labels = make(map[string]string)
	}
	if c.Annotations == nil {
		c.Annotations = make(map[string]string)
	}
}

func (c *Conf) preCheckEnv() {
	if c.Env == nil {
		c.Env = make(map[string]string)
	}
}

// GetAllFileSystem combine FileSystem and ExtraFileSystem to a slice
func (c *Conf) GetAllFileSystem() []FileSystem {
	var fileSystems []FileSystem
	if c.FileSystem.Name != "" {
		fileSystems = append([]FileSystem{}, c.FileSystem)
	}
	fileSystems = append(fileSystems, c.ExtraFileSystem...)
	return fileSystems
}

/**
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
}*/
