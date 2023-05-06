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

package schema

import (
	"fmt"
	"strings"
)

type KindGroupVersion struct {
	Kind       string `json:"kind"`
	Group      string `json:"group"`
	APIVersion string `json:"apiVersion"`
}

func NewKindGroupVersion(kind, group, version string) KindGroupVersion {
	return KindGroupVersion{
		Kind:       kind,
		Group:      group,
		APIVersion: version,
	}
}

func (kv KindGroupVersion) String() string {
	return fmt.Sprintf("kind: %s, groupVersion: %s/%s", kv.Kind, kv.Group, kv.APIVersion)
}

func ToKindGroupVersion(clusterType string, framework Framework, annotations map[string]string) KindGroupVersion {
	// TODO: get KindGroupVersion for different cluster
	// get KindGroupVersion from job annotations
	kind := annotations[JobKindAnnotation]
	groupVersion := annotations[JobGroupVersionAnnotation]
	if kind != "" && groupVersion != "" {
		group, version, err := FromGroupVersion(groupVersion)
		if err == nil {
			return NewKindGroupVersion(kind, group, version)
		}
	}
	// get KindGroupVersion from default
	kv, ok := frameworkKindGroupVersionMap[framework]
	if !ok {
		return KindGroupVersion{}
	}
	return kv
}

func FromGroupVersion(groupVersion string) (string, string, error) {
	group := ""
	version := ""
	var err error
	items := strings.Split(groupVersion, "/")
	switch len(items) {
	case 1:
		version = items[0]
	case 2:
		version = items[1]
		group = items[0]
	default:
		err = fmt.Errorf("unexpected GroupVersion string: %v", groupVersion)
	}
	return group, version, err
}

var (
	// StandaloneKindGroupVersion kind group version for single job
	StandaloneKindGroupVersion = KindGroupVersion{Kind: "Pod", Group: "", APIVersion: "v1"}
	PaddleKindGroupVersion     = KindGroupVersion{Kind: "PaddleJob", Group: "batch.paddlepaddle.org", APIVersion: "v1"}
	PyTorchKindGroupVersion    = KindGroupVersion{Kind: "PyTorchJob", Group: "kubeflow.org", APIVersion: "v1"}
	TFKindGroupVersion         = KindGroupVersion{Kind: "TFJob", Group: "kubeflow.org", APIVersion: "v1"}
	MPIKindGroupVersion        = KindGroupVersion{Kind: "MPIJob", Group: "kubeflow.org", APIVersion: "v1"}
	MXNetKindGroupVersion      = KindGroupVersion{Kind: "MXJob", Group: "kubeflow.org", APIVersion: "v1"}
	SparkKindGroupVersion      = KindGroupVersion{Kind: "SparkApplication", Group: "sparkoperator.k8s.io", APIVersion: "v1beta2"}
	RayKindGroupVersion        = KindGroupVersion{Kind: "RayJob", Group: "ray.io", APIVersion: "v1alpha1"}
	// WorkflowKindGroupVersion kind group version for argo workflow job
	WorkflowKindGroupVersion = KindGroupVersion{Kind: "Workflow", Group: "argoproj.io", APIVersion: "v1alpha1"}

	frameworkKindGroupVersionMap = map[Framework]KindGroupVersion{
		FrameworkStandalone: StandaloneKindGroupVersion,
		FrameworkPaddle:     PaddleKindGroupVersion,
		FrameworkPytorch:    PyTorchKindGroupVersion,
		FrameworkTF:         TFKindGroupVersion,
		FrameworkMPI:        MPIKindGroupVersion,
		FrameworkMXNet:      MXNetKindGroupVersion,
		FrameworkSpark:      SparkKindGroupVersion,
		FrameworkRay:        RayKindGroupVersion,
	}

	// ElasticQueueKindGroupVersion kind group version for elastic queue
	ElasticQueueKindGroupVersion = KindGroupVersion{Kind: "ElasticResourceQuota", Group: "scheduling.volcano.sh", APIVersion: "v1beta1"}
	VCQueueKindGroupVersion      = KindGroupVersion{Kind: "Queue", Group: "scheduling.volcano.sh", APIVersion: "v1beta1"}
)
