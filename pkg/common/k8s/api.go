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
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"

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
	GVKJobStatusMap = map[schema.GroupVersionKind]bool{
		SparkAppGVK:     true,
		PaddleJobGVK:    true,
		PodGVK:          true,
		ArgoWorkflowGVK: true,
		PyTorchJobGVK:   true,
		TFJobGVK:        true,
		MXNetJobGVK:     true,
		MPIJobGVK:       true,
		RayJobGVK:       true,
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
	case commomschema.FrameworkRay:
		gvk = RayJobGVK
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
