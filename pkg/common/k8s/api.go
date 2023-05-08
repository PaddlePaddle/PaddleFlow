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
	"k8s.io/apimachinery/pkg/runtime/schema"

	commomschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

var (
	PodGVK       = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
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
	RayJobGVK     = schema.GroupVersionKind{Group: "ray.io", Version: "v1alpha1", Kind: "RayJob"}
	// ArgoWorkflowGVK defines GVK for argo Workflow
	ArgoWorkflowGVK = schema.GroupVersionKind{Group: "argoproj.io", Version: "v1alpha1", Kind: "Workflow"}

	// PodGVR TODO:// add gvr to process and get rid of all gvks in future
	PodGVR          = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	SparkAppGVR     = schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	PaddleJobGVR    = schema.GroupVersionResource{Group: "batch.paddlepaddle.org", Version: "v1", Resource: "paddlejobs"}
	PyTorchJobGVR   = schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "pytorchjobs"}
	TFJobGVR        = schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "tfjobs"}
	MPIJobGVR       = schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "mpijobs"}
	MXNetJobGVR     = schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "mxjobs"}
	XGBoostJobGVR   = schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "xgboostjobs"}
	RayJobGVR       = schema.GroupVersionResource{Group: "ray.io", Version: "v1alpha1", Resource: "rayjobs"}
	ArgoWorkflowGVR = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "workflows"}
)

func GetJobGVR(kindGroupVersion commomschema.KindGroupVersion) schema.GroupVersionResource {
	switch kindGroupVersion {
	case commomschema.StandaloneKindGroupVersion:
		return PodGVR
		// TODO://reopen
		// case commomschema.FrameworkTF:
		// 	return TFJobGVR
		// case commomschema.FrameworkPytorch:
		// 	return PyTorchJobGVR
		// case commomschema.FrameworkSpark:
		// 	return SparkAppGVR
		// case commomschema.FrameworkPaddle:
		// 	return PaddleJobGVR
		// case commomschema.FrameworkMXNet:
		// 	return MXNetJobGVR
		// case commomschema.FrameworkMPI:
		// 	return MPIJobGVR
		// case commomschema.FrameworkRay:
		// 	return RayJobGVR
	}
	// default return pod gvr
	return PodGVR
}
