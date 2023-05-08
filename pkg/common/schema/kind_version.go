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
	return fmt.Sprintf("kind: %s, groupVersion: %s", kv.Kind, kv.GroupVersion())
}

func (kv KindGroupVersion) GroupVersion() string {
	return fmt.Sprintf("%s/%s", kv.Group, kv.APIVersion)
}

func ToKindGroupVersion(clusterType string, framework Framework, annotations map[string]string) (KindGroupVersion, error) {
	// TODO: get KindGroupVersion for different cluster
	// 1. get KindGroupVersion from default
	kindGV := frameworkKindGroupVersionMap[framework]
	// 2. get KindGroupVersion from job annotations if set
	kindGroupVersionStr := annotations[JobKindGroupVersionAnnotation]
	if kindGroupVersionStr != "" {
		kv, err := parseKindGroupVersion(kindGroupVersionStr)
		if err == nil {
			kindGV = kv
		} else {
			return KindGroupVersion{}, err
		}
	}
	// 3. check KindGroupVersion
	_, find := JobKindGroupVersionMap[kindGV]
	if !find {
		return KindGroupVersion{}, fmt.Errorf("the KindGroupVersion %s is not supported", kindGV)
	}
	return kindGV, nil
}

// xFromGroupVersion convert kind group version str to struct, str format is {kind}.{group}/{version}
func parseKindGroupVersion(kindGVStr string) (KindGroupVersion, error) {
	kindGV := KindGroupVersion{}
	kindVersion := strings.Split(kindGVStr, "/")
	if len(kindVersion) != 2 {
		return kindGV, fmt.Errorf("the KindGroupVersion %s is invalid, "+
			"and it's format must be {kind}.{group}/{version}", kindGVStr)
	}
	// get kind and group
	kindGroups := strings.Split(kindVersion[0], ".")
	if len(kindGroups) < 2 {
		return kindGV, fmt.Errorf("the KindGroupVersion %s is invalid, "+
			"and it's format must be {kind}.{group}/{version}", kindGVStr)
	}
	kindGV.Kind = kindGroups[0]
	kindGV.Group = strings.TrimPrefix(kindVersion[0], kindGV.Kind+".")
	kindGV.APIVersion = kindVersion[1]
	return kindGV, nil
}

var (
	// StandaloneKindGroupVersion kind group version for single job
	StandaloneKindGroupVersion = KindGroupVersion{Kind: "Pod", Group: "", APIVersion: "v1"}
	PaddleKindGroupVersion     = KindGroupVersion{Kind: "PaddleJob", Group: "batch.paddlepaddle.org", APIVersion: "v1"}
	KFPaddleKindGroupVersion   = KindGroupVersion{Kind: "PaddleJob", Group: "kubeflow.org", APIVersion: "v1"}
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
	JobKindGroupVersionMap = map[KindGroupVersion]bool{
		StandaloneKindGroupVersion: true,
		SparkKindGroupVersion:      true,
		PaddleKindGroupVersion:     true,
		KFPaddleKindGroupVersion:   true,
		PyTorchKindGroupVersion:    true,
		TFKindGroupVersion:         true,
		MXNetKindGroupVersion:      true,
		MPIKindGroupVersion:        true,
		RayKindGroupVersion:        true,
		WorkflowKindGroupVersion:   true,
	}

	// ElasticQueueKindGroupVersion kind group version for elastic queue
	ElasticQueueKindGroupVersion = KindGroupVersion{Kind: "ElasticResourceQuota", Group: "scheduling.volcano.sh", APIVersion: "v1beta1"}
	VCQueueKindGroupVersion      = KindGroupVersion{Kind: "Queue", Group: "scheduling.volcano.sh", APIVersion: "v1beta1"}
)

func GetJobType(kindVersion KindGroupVersion) JobType {
	switch kindVersion {
	case StandaloneKindGroupVersion:
		return TypeSingle
	case WorkflowKindGroupVersion:
		return TypeWorkflow
	default:
		return TypeDistributed
	}
}

func GetJobFramework(gvk KindGroupVersion) Framework {
	switch gvk {
	case StandaloneKindGroupVersion:
		return FrameworkStandalone
	case WorkflowKindGroupVersion:
		return ""
	default:
		return distributedJobFramework(gvk)
	}
}

func distributedJobFramework(gvk KindGroupVersion) Framework {
	switch gvk {
	case SparkKindGroupVersion:
		return FrameworkSpark
	case PaddleKindGroupVersion, KFPaddleKindGroupVersion:
		return FrameworkPaddle
	case PyTorchKindGroupVersion:
		return FrameworkPytorch
	case TFKindGroupVersion:
		return FrameworkTF
	case MXNetKindGroupVersion:
		return FrameworkMXNet
	case MPIKindGroupVersion:
		return FrameworkMPI
	case RayKindGroupVersion:
		return FrameworkRay
	default:
		return ""
	}
}
