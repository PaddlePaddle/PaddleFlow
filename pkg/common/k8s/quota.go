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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func SubQuota(r *schema.Resource, pod *v1.Pod) error {
	for _, container := range pod.Spec.Containers {
		containerQuota := NewResource(container.Resources.Requests)
		r.Sub(containerQuota)
	}
	return nil
}

// CalcPodResources calculate pod minimum resource
func CalcPodResources(pod *v1.Pod) *schema.ResourceInfo {
	podRes := schema.EmptyResourceInfo()
	for _, c := range pod.Spec.Containers {
		res := NewResourceInfo(c.Resources.Requests)
		*podRes = podRes.Add(*res)
	}
	return podRes
}

// NewResourceInfo create a new resource object from resource list
func NewResourceInfo(rl v1.ResourceList) *schema.ResourceInfo {
	r := schema.EmptyResourceInfo()

	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.CPU = rQuant.String()
		case v1.ResourceMemory:
			r.Mem = rQuant.String()
		default:
			if IsScalarResourceName(rName) {
				r.SetScalar(schema.ResourceName(rName), rQuant.String())
			}
		}
	}
	return r
}

// NewResource create a new resource object from resource list
func NewResource(rl v1.ResourceList) *schema.Resource {
	r := schema.EmptyResource()

	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.MilliCPU += float64(rQuant.MilliValue())
		case v1.ResourceMemory:
			r.Memory += float64(rQuant.Value())
		case v1.ResourceEphemeralStorage:
			r.Storage += float64(rQuant.Value())
		default:
			// NOTE: When converting this back to k8s resource, we need record the format as well as / 1000
			if IsScalarResourceName(rName) {
				r.AddScalar(schema.ResourceName(rName), float64(rQuant.Value()))
			}
		}
	}
	return r
}

func NewKubeResourceList(r *schema.ResourceInfo) v1.ResourceList {
	resourceList := v1.ResourceList{}
	resourceList[v1.ResourceCPU] = resource.MustParse(r.CPU)
	resourceList[v1.ResourceMemory] = resource.MustParse(r.Mem)
	for k, v := range r.ScalarResources {
		resourceList[v1.ResourceName(k)] = resource.MustParse(v)
	}
	return resourceList
}

// NewResourceList create a new resource object from resource list
func NewResourceList(r *schema.Resource) v1.ResourceList {
	resourceList := v1.ResourceList{}
	cpuQuantity := resource.NewMilliQuantity(int64(r.MilliCPU), resource.BinarySI)
	memoryQuantity := resource.NewQuantity(int64(r.Memory), resource.BinarySI)
	storageQuantity := resource.NewQuantity(int64(r.Storage), resource.BinarySI)

	resourceList[v1.ResourceCPU] = *cpuQuantity
	resourceList[v1.ResourceMemory] = *memoryQuantity
	resourceList[v1.ResourceStorage] = *storageQuantity

	for resourceName, RQuant := range r.ScalarResources {
		quantity := resource.NewQuantity(int64(RQuant), resource.BinarySI)
		resourceList[v1.ResourceName(resourceName)] = *quantity
	}
	return resourceList
}
