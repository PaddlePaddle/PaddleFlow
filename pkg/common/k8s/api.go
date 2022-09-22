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

package k8s

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	commomschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

var (
	PodGVK       = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	VCJobGVK     = schema.GroupVersionKind{Group: "batch.volcano.sh", Version: "v1alpha1", Kind: "Job"}
	PodGroupGVK  = schema.GroupVersionKind{Group: "scheduling.volcano.sh", Version: "v1beta1", Kind: "PodGroup"}
	VCQueueGVK   = schema.GroupVersionKind{Group: "scheduling.volcano.sh", Version: "v1beta1", Kind: "Queue"}
	EQuotaGVK    = schema.GroupVersionKind{Group: "scheduling.volcano.sh", Version: "v1beta1", Kind: "ElasticResourceQuota"}
	SparkAppGVK  = schema.GroupVersionKind{Group: "sparkoperator.k8s.io", Version: "v1beta2", Kind: "SparkApplication"}
	PaddleJobGVK = schema.GroupVersionKind{Group: "batch.paddlepaddle.org", Version: "v1", Kind: "PaddleJob"}
	// PyTorchJobGVK TFJobGVK defines GVK for kubeflow jobs
	PyTorchJobGVK = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "PyTorchJob"}
	TFJobGVK      = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "TFJob"}
	MPIJobGVK     = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "MPIJob"}
	MXNetJobGVK   = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "MXJob"}
	XGBoostJobGVK = schema.GroupVersionKind{Group: "kubeflow.org", Version: "v1", Kind: "XGBoostJob"}
	RayJobGVK     = schema.GroupVersionKind{Group: "ray.io", Version: "v1alpha1", Kind: "RayJob"}

	// ArgoWorkflowGVK defines GVK for argo Workflow
	ArgoWorkflowGVK = schema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Workflow"}

	// GVKJobStatusMap contains GroupVersionKind and convertStatus function to sync job status
	GVKJobStatusMap = map[schema.GroupVersionKind]GetStatusFunc{
		VCJobGVK:        VCJobStatus,
		SparkAppGVK:     SparkAppStatus,
		PaddleJobGVK:    PaddleJobStatus,
		PodGVK:          SingleJobStatus,
		ArgoWorkflowGVK: ArgoWorkflowStatus,
		PyTorchJobGVK:   PytorchJobStatus,
		TFJobGVK:        TFJobStatus,
		MXNetJobGVK:     MXNetJobStatus,
		MPIJobGVK:       MPIJobStatus,
		RayJobGVK:       RayJobStatus,
	}
	// GVKToQuotaType GroupVersionKind lists for PaddleFlow QuotaType
	GVKToQuotaType = []schema.GroupVersionKind{
		VCQueueGVK,
		EQuotaGVK,
	}
)

func GetJobFrameworkVersion(jobType commomschema.JobType, framework commomschema.Framework) commomschema.FrameworkVersion {
	if jobType == commomschema.TypeWorkflow {
		return commomschema.NewFrameworkVersion(ArgoWorkflowGVK.Kind, ArgoWorkflowGVK.GroupVersion().String())
	}
	var gvk schema.GroupVersionKind
	switch framework {
	case commomschema.FrameworkStandalone:
		gvk = PodGVK
	case commomschema.FrameworkTF:
		gvk = TFJobGVK
	case commomschema.FrameworkPytorch:
		gvk = PyTorchJobGVK
	case commomschema.FrameworkSpark:
		gvk = SparkAppGVK
	case commomschema.FrameworkPaddle:
		gvk = PaddleJobGVK
	case commomschema.FrameworkMXNet:
		gvk = MXNetJobGVK
	case commomschema.FrameworkMPI:
		gvk = MPIJobGVK
	}
	return commomschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
}

func GetJobTypeAndFramework(gvk schema.GroupVersionKind) (commomschema.JobType, commomschema.Framework) {
	switch gvk {
	case PodGVK:
		return commomschema.TypeSingle, commomschema.FrameworkStandalone
	case SparkAppGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkSpark
	case PaddleJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkPaddle
	case PyTorchJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkPytorch
	case TFJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkTF
	case MXNetJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkMXNet
	case MPIJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkMPI
	case RayJobGVK:
		return commomschema.TypeDistributed, commomschema.FrameworkRay
	default:
		log.Errorf("GroupVersionKind %s is not support", gvk)
		return "", ""
	}
}

type StatusInfo struct {
	OriginStatus string
	Status       commomschema.JobStatus
	Message      string
}

type GetStatusFunc func(interface{}) (StatusInfo, error)

// DynamicClientOption for kubernetes dynamic client
type DynamicClientOption struct {
	DynamicClient   dynamic.Interface
	DynamicFactory  dynamicinformer.DynamicSharedInformerFactory
	DiscoveryClient discovery.DiscoveryInterface
	Config          *rest.Config
	ClusterInfo     *commomschema.Cluster
	// GVKToGVR contains GroupVersionKind map to GroupVersionResource
	GVKToGVR sync.Map
}

func CreateDynamicClientOpt(config *rest.Config, cluster *commomschema.Cluster) (*DynamicClientOption, error) {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Errorf("init dynamic client failed. error:%s", err)
		return nil, err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		log.Errorf("create discovery client failed: %v", err)
		return nil, err
	}
	if cluster == nil {
		log.Errorf("cluster info is nil")
		return nil, fmt.Errorf("cluster info is nil")
	}
	return &DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  factory,
		DiscoveryClient: discoveryClient,
		Config:          config,
		ClusterInfo:     cluster,
	}, nil
}

func (dc *DynamicClientOption) GetGVR(gvk schema.GroupVersionKind) (meta.RESTMapping, error) {
	gvr, ok := dc.GVKToGVR.Load(gvk.String())
	if ok {
		return gvr.(meta.RESTMapping), nil
	}
	return dc.findGVR(&gvk)
}

func (dc *DynamicClientOption) findGVR(gvk *schema.GroupVersionKind) (meta.RESTMapping, error) {
	// DiscoveryClient queries API server about the resources
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc.DiscoveryClient))
	// Find GVR
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		log.Warningf("find GVR with restMapping failed: %v", err)
		return meta.RESTMapping{}, err
	}
	// Store GVR
	log.Debugf("The GVR of GVK[%s] is [%s]", gvk.String(), mapping.Resource.String())
	dc.GVKToGVR.Store(gvk.String(), *mapping)
	return *mapping, nil
}

type PodStatusMessage struct {
	Phase             v1.PodPhase              `json:"phase,omitempty"`
	Message           string                   `json:"message,omitempty"`
	Reason            string                   `json:"reason,omitempty"`
	ContainerMessages []ContainerStatusMessage `json:"containerMessages,omitempty"`
}

func (ps *PodStatusMessage) String() string {
	msg := fmt.Sprintf("pod phase is %s", ps.Phase)
	if len(ps.Reason) != 0 {
		msg += fmt.Sprintf(" with reason %s", ps.Reason)
	}
	if len(ps.Message) != 0 {
		msg += fmt.Sprintf(", detail message: %s", ps.Message)
	}
	// Container status message
	if len(ps.ContainerMessages) != 0 {
		msg += ". Containers status:"
	}
	for _, cs := range ps.ContainerMessages {
		msg += fmt.Sprintf(" %s;", cs.String())
	}
	return msg
}

type ContainerStatusMessage struct {
	Name            string                       `json:"name,omitempty"`
	ContainerID     string                       `json:"containerID,omitempty"`
	RestartCount    int32                        `json:"restartCount,omitempty"`
	WaitingState    *v1.ContainerStateWaiting    `json:"waitingState,omitempty"`
	TerminatedState *v1.ContainerStateTerminated `json:"terminatedState,omitempty"`
}

func (cs *ContainerStatusMessage) String() string {
	msg := fmt.Sprintf("container %s with restart count %d", cs.Name, cs.RestartCount)
	if len(cs.ContainerID) != 0 {
		msg += fmt.Sprintf(", id is %s", cs.ContainerID)
	}
	if cs.WaitingState != nil {
		msg += fmt.Sprintf(", wating with reason %s", cs.WaitingState.Reason)
		if len(cs.WaitingState.Message) != 0 {
			msg += fmt.Sprintf(", message: %s", cs.WaitingState.Message)
		}
	}
	if cs.TerminatedState != nil {
		msg += fmt.Sprintf(", terminated with exitCode %d, reason is %s", cs.TerminatedState.ExitCode, cs.TerminatedState.Reason)
		if len(cs.TerminatedState.Message) != 0 {
			msg += fmt.Sprintf(", message: %s", cs.TerminatedState.Message)
		}
	}
	return msg
}

// GetTaskMessage construct message from pod status
func GetTaskMessage(podStatus *v1.PodStatus) string {
	if podStatus == nil {
		return ""
	}
	statusMessage := PodStatusMessage{
		Phase:             podStatus.Phase,
		Reason:            podStatus.Reason,
		Message:           podStatus.Message,
		ContainerMessages: []ContainerStatusMessage{},
	}
	for _, initCS := range podStatus.InitContainerStatuses {
		statusMessage.ContainerMessages = append(statusMessage.ContainerMessages, ContainerStatusMessage{
			Name:            initCS.Name,
			ContainerID:     initCS.ContainerID,
			RestartCount:    initCS.RestartCount,
			WaitingState:    initCS.State.Waiting,
			TerminatedState: initCS.State.Terminated,
		})
	}
	for _, cs := range podStatus.ContainerStatuses {
		statusMessage.ContainerMessages = append(statusMessage.ContainerMessages, ContainerStatusMessage{
			Name:            cs.Name,
			ContainerID:     cs.ContainerID,
			RestartCount:    cs.RestartCount,
			WaitingState:    cs.State.Waiting,
			TerminatedState: cs.State.Terminated,
		})
	}
	return statusMessage.String()
}

func GetJobGVK(jobType commomschema.JobType, framework commomschema.Framework) (schema.GroupVersionKind, error) {
	var gvk schema.GroupVersionKind
	var err error
	switch jobType {
	case commomschema.TypeSingle:
		gvk = PodGVK
	case commomschema.TypeDistributed:
		gvk, err = getDistributedJobGVK(framework)
	case commomschema.TypeWorkflow:
		gvk = ArgoWorkflowGVK
	default:
		err = fmt.Errorf("job type %s is not supported", jobType)
	}
	return gvk, err
}

func getDistributedJobGVK(framework commomschema.Framework) (schema.GroupVersionKind, error) {
	var gvk schema.GroupVersionKind
	var err error
	switch framework {
	case commomschema.FrameworkPaddle:
		gvk = PaddleJobGVK
	case commomschema.FrameworkSpark:
		gvk = SparkAppGVK
	case commomschema.FrameworkPytorch:
		gvk = PyTorchJobGVK
	case commomschema.FrameworkTF:
		gvk = TFJobGVK
	case commomschema.FrameworkMXNet:
		gvk = MXNetJobGVK
	case commomschema.FrameworkRay:
		gvk = RayJobGVK
	case commomschema.FrameworkMPI:
		err = fmt.Errorf("framework %s is not implemented", framework)
	default:
		err = fmt.Errorf("framework %s is not supported", framework)
	}
	return gvk, err
}
