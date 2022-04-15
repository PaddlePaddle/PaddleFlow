package api

type CreateJobRequest struct {
	JobType            string `json:"jobType"`
	CommonSpec         `json:",inline"`
	JobSpec            `json:",inline"`
	DistributedJobSpec `json:",inline"`
}

type CreateJobResponse struct {
	ID string `json:"id"`
}

type JobInfo struct {
	CommonSpec         `json:",inline"`
	JobSpec            `json:",inline"`
	DistributedJobSpec `json:",inline"`
	Status             string                 `json:"status"`
	AcceptTime         string                 `json:"acceptTime"`
	StartTime          string                 `json:"startTime"`
	FinishTime         string                 `json:"finishTime"`
	User               string                 `json:"userName"`
	Runtime            RuntimeInfo            `json:"runtime,omitempty"`
	DistributedRuntime DistributedRuntimeInfo `json:"distributedRuntime,omitempty"`
	WorkflowRuntime    WorkflowRuntimeInfo    `json:"workflowRuntime,omitempty"`
}

type JobSpec struct {
	ExtraFileSystem []FileSystem      `json:"extraFileSystem,omitempty"`
	Image            string            `json:"image"`
	Env              map[string]string `json:"env,omitempty"`
	Command          string            `json:"command,omitempty"`
	Args             []string          `json:"args,omitempty"`
	Port             int               `json:"port,omitempty"`
	Flavour          `json:"flavour,omitempty"`
	FileSystem       `json:"fileSystem,omitempty"`
}

type DistributedJobSpec struct {
	Framework string       `json:"framework,omitempty"`
	Members   []MemberSpec `json:"members,omitempty"`
}

type CommonSpec struct {
	ID                string            `json:"id,omitempty"`
	Name              string            `json:"name,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	ExtensionTemplate string            `json:"extensionTemplate,omitempty"`
	SchedulingPolicy  `json:"schedulingPolicy"`
}

type MemberSpec struct {
	Replicas int    `json:"replicas,omitempty"`
	Role     string `json:"role,omitempty"`
	JobSpec  `json:",inline"`
}

type SchedulingPolicy struct {
	Queue    string `json:"queue"`
	Priority string `json:"priority"`
}

type Flavour struct {
	Name            string            `json:"name"`
	Cpu             string            `json:"cpu,omitempty"`
	Memory          string            `json:"memory,omitempty"`
	ScalarResources map[string]string `json:"scalarResources,omitempty"`
}

type FileSystem struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath,omitempty"`
	SubPath   string `json:"subPath,omitempty"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type RuntimeInfo struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	ID        string `json:"id,omitempty"`
	Status    string `json:"status,omitempty"`
}

type DistributedRuntimeInfo struct {
	Name      string        `json:"name,omitempty"`
	Namespace string        `json:"namespace,omitempty"`
	ID        string        `json:"id,omitempty"`
	Status    string        `json:"status,omitempty"`
	Runtimes  []RuntimeInfo `json:"runtimes,omitempty"`
}

type WorkflowRuntimeInfo struct {
	Name      string                   `json:"name,omitempty"`
	Namespace string                   `json:"namespace,omitempty"`
	ID        string                   `json:"id,omitempty"`
	Status    string                   `json:"status,omitempty"`
	Nodes     []DistributedRuntimeInfo `json:"nodes,omitempty"`
}

type ListJobOptions struct {
	Queue     string            `json:"queue,omitempty"`
	Status    string            `json:"status,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	StartTime string            `json:"startTime,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Marker    string            `json:"marker,omitempty"`
	MaxKeys   int               `json:"maxKeys,omitempty"`
}

type ListJobResponse struct {
	JobList     []JobInfo `json:"jobList"`
	Marker      string    `json:"marker"`
	NextMarker  string    `json:"nextMarker"`
	IsTruncated bool      `json:"isTruncated"`
}

type LoginResponse struct {
	Authorization string `json:"authorization"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
