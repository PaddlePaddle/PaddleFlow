package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
// var app appsv1.Deployment{}
// RayClusterSpec defines the desired state of RayCluster
type RayClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// HeadGroupSpecs are the spec for the head pod
	HeadGroupSpec HeadGroupSpec `json:"headGroupSpec"`
	// WorkerGroupSpecs are the specs for the worker pods
	WorkerGroupSpecs []WorkerGroupSpec `json:"workerGroupSpecs,omitempty"`
	// RayVersion is the version of ray being used. this affects the command used to start ray
	RayVersion string `json:"rayVersion,omitempty"`
	// EnableInTreeAutoscaling indicates whether operator should create in tree autoscaling configs
	EnableInTreeAutoscaling *bool `json:"enableInTreeAutoscaling,omitempty"`
	// AutoscalerOptions specifies optional configuration for the Ray autoscaler.
	AutoscalerOptions *AutoscalerOptions `json:"autoscalerOptions,omitempty"`
}

// HeadGroupSpec are the spec for the head pod
type HeadGroupSpec struct {
	// ServiceType is Kubernetes service type of the head service. it will be used by the workers to connect to the head pod
	ServiceType v1.ServiceType `json:"serviceType"`
	// EnableIngress indicates whether operator should create ingress object for head service or not.
	EnableIngress *bool `json:"enableIngress,omitempty"`
	// HeadGroupSpec.Replicas is deprecated and ignored; there can only be one head pod per Ray cluster.
	Replicas *int32 `json:"replicas,omitempty"`
	// RayStartParams are the params of the start command: node-manager-port, object-store-memory, ...
	RayStartParams map[string]string `json:"rayStartParams"`
	// Template is the eaxct pod template used in K8s depoyments, statefulsets, etc.
	Template v1.PodTemplateSpec `json:"template"`
}

// WorkerGroupSpec are the specs for the worker pods
type WorkerGroupSpec struct {
	// we can have multiple worker groups, we distinguish them by name
	GroupName string `json:"groupName"`
	// Replicas Number of desired pods in this pod group. This is a pointer to distinguish between explicit
	// zero and not specified. Defaults to 1.
	Replicas *int32 `json:"replicas"`
	// MinReplicas defaults to 1
	MinReplicas *int32 `json:"minReplicas"`
	// MaxReplicas defaults to maxInt32
	MaxReplicas *int32 `json:"maxReplicas"`
	// RayStartParams are the params of the start command: address, object-store-memory, ...
	RayStartParams map[string]string `json:"rayStartParams"`
	// Template a pod template for the worker
	Template v1.PodTemplateSpec `json:"template"`
	// ScaleStrategy defines which pods to remove
	ScaleStrategy ScaleStrategy `json:"scaleStrategy,omitempty"`
}

// ScaleStrategy to remove workers
type ScaleStrategy struct {
	// WorkersToDelete workers to be deleted
	WorkersToDelete []string `json:"workersToDelete,omitempty"`
}

// AutoscalerOptions specifies optional configuration for the Ray autoscaler.
type AutoscalerOptions struct {
	// Resources specifies optional resource request and limit overrides for the autoscaler container.
	// Default values: 500m CPU request and limit. 512Mi memory request and limit.
	Resources *v1.ResourceRequirements `json:"resources,omitempty"`
	// Image optionally overrides the autoscaler's container image. This override is for provided for autoscaler testing and development.
	Image *string `json:"image,omitempty"`
	// ImagePullPolicy optionally overrides the autoscaler container's image pull policy. This override is for provided for autoscaler testing and development.
	ImagePullPolicy *v1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// Optional list of environment variables to set in the autoscaler container.
	Env []v1.EnvVar `json:"env,omitempty"`
	// Optional list of sources to populate environment variables in the autoscaler container.
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`
	// IdleTimeoutSeconds is the number of seconds to wait before scaling down a worker pod which is not using Ray resources.
	// Defaults to 60 (one minute).
	IdleTimeoutSeconds *int32 `json:"idleTimeoutSeconds,omitempty"`
	// UpscalingMode is "Conservative", "Default", or "Aggressive."
	// Conservative: Upscaling is rate-limited; the number of pending worker pods is at most the size of the Ray cluster.
	// Default: Upscaling is not rate-limited.
	// Aggressive: An alias for Default; upscaling is not rate-limited.
	UpscalingMode *UpscalingMode `json:"upscalingMode,omitempty"`
}

// +kubebuilder:validation:Enum=Default;Aggressive;Conservative
type UpscalingMode string

// The overall state of the Ray cluster.
type ClusterState string

const (
	Ready     ClusterState = "ready"
	Unhealthy ClusterState = "unhealthy"
	Failed    ClusterState = "failed"
)

// RayClusterStatus defines the observed state of RayCluster
type RayClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// Status reflects the status of the cluster
	State ClusterState `json:"state,omitempty"`
	// AvailableWorkerReplicas indicates how many replicas are available in the cluster
	AvailableWorkerReplicas int32 `json:"availableWorkerReplicas,omitempty"`
	// DesiredWorkerReplicas indicates overall desired replicas claimed by the user at the cluster level.
	DesiredWorkerReplicas int32 `json:"desiredWorkerReplicas,omitempty"`
	// MinWorkerReplicas indicates sum of minimum replicas of each node group.
	MinWorkerReplicas int32 `json:"minWorkerReplicas,omitempty"`
	// MaxWorkerReplicas indicates sum of maximum replicas of each node group.
	MaxWorkerReplicas int32 `json:"maxWorkerReplicas,omitempty"`
	// LastUpdateTime indicates last update timestamp for this cluster status.
	// +nullable
	LastUpdateTime *metav1.Time `json:"lastUpdateTime,omitempty"`
	// Service Endpoints
	Endpoints map[string]string `json:"endpoints,omitempty"`
}

// RayNodeType  the type of a ray node: head/worker
type RayNodeType string

const (
	// HeadNode means that this pod will be ray cluster head
	HeadNode RayNodeType = "head"
	// WorkerNode means that this pod will be ray cluster worker
	WorkerNode RayNodeType = "worker"
)

// RayCluster is the Schema for the RayClusters API
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+genclient
type RayCluster struct {
	// Standard object metadata.
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the RayCluster.
	Spec   RayClusterSpec   `json:"spec,omitempty"`
	Status RayClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RayClusterList contains a list of RayCluster
type RayClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RayCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RayCluster{}, &RayClusterList{})
}

type EventReason string

const (
	RayConfigError EventReason = "RayConfigError"
)
