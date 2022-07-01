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

	pfResources "github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func SubQuota(r *pfResources.Resource, pod *v1.Pod) error {
	for _, container := range pod.Spec.Containers {
		containerQuota := NewResource(container.Resources.Requests)
		r.Sub(containerQuota)
	}
	return nil
}

// CalcPodResources calculate pod minimum resource
func CalcPodResources(pod *v1.Pod) *pfResources.Resource {
	podRes := pfResources.EmptyResource()
	if pod == nil {
		return podRes
	}
	for _, c := range pod.Spec.Containers {
		res := NewResource(c.Resources.Requests)
		podRes.Add(res)
	}
	return podRes
}

func NewResource(rl v1.ResourceList) *pfResources.Resource {
	r := pfResources.EmptyResource()
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.SetResources(pfResources.ResourceCPU, rQuant.MilliValue())
		case v1.ResourceMemory:
			r.SetResources(pfResources.ResourceMemory, rQuant.Value())
		case v1.ResourceEphemeralStorage:
			r.SetResources(pfResources.ResourceStorage, rQuant.Value())
		default:
			if IsScalarResourceName(rName) {
				r.SetResources(string(rName), rQuant.Value())
			}
		}
	}
	return r
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
func NewResourceList(r *pfResources.Resource) v1.ResourceList {
	resourceList := v1.ResourceList{}
	for resourceName, RQuant := range r.Resources {
		rName := v1.ResourceName("")
		quantity := &resource.Quantity{}
		switch resourceName {
		case pfResources.ResourceCPU:
			quantity = resource.NewMilliQuantity(int64(RQuant), resource.DecimalSI)
			rName = v1.ResourceCPU
		case pfResources.ResourceMemory:
			quantity = resource.NewQuantity(int64(RQuant), resource.BinarySI)
			rName = v1.ResourceMemory
		default:
			quantity = resource.NewQuantity(int64(RQuant), resource.BinarySI)
			rName = v1.ResourceName(resourceName)
		}
		resourceList[rName] = *quantity
	}
	return resourceList
}
