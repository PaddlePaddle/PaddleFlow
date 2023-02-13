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
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pfResources "github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
)

const (
	DefaultCpuRequest = 1
	DefaultMemRequest = 1073741824 // 1Gi=1024*1024*1024B=1073741824B

	MaxGPUIndex       = 16
	GPUIndexResources = "gpuDeviceIDX"
)

var (
	GPUCorePodKey = "GPU_CORE_POD"
	GPUMemPodKey  = "GPU_MEM_POD"
	GPUIdxKey     = "GPU_IDX"

	GPURNamePrefix = "/gpu"
	GPUXName       = "cgpu"
	GPUXCoreName   = fmt.Sprintf("%s_core", GPUXName)
)

func init() {
	gpuCorePod := os.Getenv("PF_GPU_CORE_POD")
	if gpuCorePod != "" {
		GPUCorePodKey = gpuCorePod
	}
	gpuMemPod := os.Getenv("PF_GPU_MEM_POD")
	if gpuMemPod != "" {
		GPUMemPodKey = gpuMemPod
	}
	gpuIDX := os.Getenv("PF_GPU_IDX")
	if gpuIDX != "" {
		GPUIdxKey = gpuIDX
	}
	gpuNamePrefix := os.Getenv("PF_GPU_NAME_PREFIX")
	if gpuNamePrefix != "" {
		GPURNamePrefix = gpuNamePrefix
	}
	fmt.Printf("CORE_POD key: %s, IDX key: %s, GPU Name prefix: %s\n", GPUCorePodKey, GPUIdxKey, GPURNamePrefix)
}

func SharedGPUIDX(pod *v1.Pod) int64 {
	log.Debugf("pod %s/%s shared gpu idx: %v", pod.Namespace, pod.Name, pod.Annotations)
	var deviceIDX int64 = -1
	annotations := pod.Annotations
	if annotations == nil {
		return deviceIDX
	}

	corePodStr, findCore := annotations[GPUCorePodKey]
	idxStr, find := annotations[GPUIdxKey]
	if !findCore || !find {
		return deviceIDX
	}
	corePodNum, err := strconv.Atoi(corePodStr)
	if err != nil {
		return deviceIDX
	}

	idxs := strings.Split(idxStr, ",")
	log.Debugf("split idxs: %v", idxs)
	if corePodNum/50 == len(idxs) {
		// is shared gpu
		deviceIDX = 0
		var deviceID int
		for _, idx := range idxs {
			deviceID, err = strconv.Atoi(idx)
			if err != nil {
				log.Warnf("convert str[%s] to int failed, err: %v", idx, err)
				return -1
			}
			deviceIDX += GPUDeviceIDX(deviceID)
		}
	}
	return deviceIDX
}

func IsGPUX(rName string) bool {
	return strings.HasSuffix(rName, GPUXName) || strings.HasPrefix(rName, GPURNamePrefix)
}

func getGPUX(rName string, rValue int64) (int64, bool) {
	if strings.HasPrefix(rName, GPURNamePrefix) {
		return rValue, true
	}
	if strings.HasSuffix(rName, GPUXName) {
		return rValue / 100, true
	}
	return 0, false
}

func SubWithGPUX(total *pfResources.Resource, rr map[string]int64) map[string]interface{} {
	if total == nil || rr == nil {
		return make(map[string]interface{})
	}
	log.Debugf("total: %v， used: %v", total, rr)
	var isShared = false
	var sharedDevices []int
	gpuIDX, find := rr[GPUIndexResources]
	if find {
		isShared = true
		sharedDevices = GPUSharedDevices(gpuIDX)
	}
	log.Debugf("gpuIDX: %v， shared devices: %v", gpuIDX, sharedDevices)

	// get gpu count and name
	var gpuTotalCount int64
	var gpuxName string
	scalarResources := total.ScalarResources("")
	for rName, rValue := range scalarResources {
		value, ok := getGPUX(rName, int64(rValue))
		if ok {
			gpuxName = rName
			gpuTotalCount = value
			break
		}
	}
	// get used gpu and gpu core
	var gpux, gpuxCore int64
	for rName, rValue := range rr {
		// check used shared gpu
		if strings.HasSuffix(rName, GPUXName) {
			gpux = rValue
		}
		if strings.HasSuffix(rName, GPUXCoreName) {
			gpuxCore = rValue
		}
		// check used gpu
		if strings.HasPrefix(rName, GPURNamePrefix) {
			log.Debugf("used %s , value: %v", rName, rValue)
			gpuTotalCount -= rValue
		}
	}
	log.Debugf("gpuIDX: %v， used gpu %d, and used gpu core %d", gpuIDX, gpux, gpuxCore)

	// calculate idle gpu
	var idlegpux, idleSharedGPUX int64
	gpuxResource := map[string]interface{}{}
	if isShared {
		totalShared := int64(len(sharedDevices))
		totalUsed := gpuxCore/50 - gpux
		sharedUsed := 2*gpux - gpuxCore/50

		idlegpux = gpuTotalCount - totalShared - totalUsed
		idleSharedGPUX = totalShared*2 - sharedUsed

		gpuxResource = map[string]interface{}{
			"100": idlegpux,
			"50":  idleSharedGPUX,
		}
		log.Debugf("%s, idle gpu %d, idle shared gpu %d", gpuxName, idlegpux, idleSharedGPUX)
	} else {
		idlegpux = gpuTotalCount - gpux
		gpuxResource["100"] = idlegpux
	}

	cpu := resource.NewMilliQuantity(int64(total.CPU())-rr[pfResources.ResCPU], resource.DecimalSI)
	memory := resource.NewQuantity(int64(total.Memory())-rr[pfResources.ResMemory], resource.BinarySI)
	return map[string]interface{}{
		pfResources.ResCPU:    cpu.String(),
		pfResources.ResMemory: memory.String(),
		gpuxName:              gpuxResource,
	}
}

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
			r.SetResources(pfResources.ResCPU, rQuant.MilliValue())
		case v1.ResourceMemory:
			r.SetResources(pfResources.ResMemory, rQuant.Value())
		case v1.ResourceEphemeralStorage:
			r.SetResources(pfResources.ResStorage, rQuant.Value())
		default:
			if IsScalarResourceName(rName) {
				r.SetResources(string(rName), rQuant.Value())
			}
		}
	}
	return r
}

// NewResourceList create a new resource object from resource list
func NewResourceList(r *pfResources.Resource) v1.ResourceList {
	resourceList := v1.ResourceList{}
	if r == nil {
		return resourceList
	}
	for resourceName, RQuant := range r.Resource() {
		rName := v1.ResourceName("")
		var quantity *resource.Quantity
		switch resourceName {
		case pfResources.ResCPU:
			quantity = resource.NewMilliQuantity(int64(RQuant), resource.DecimalSI)
			rName = v1.ResourceCPU
		case pfResources.ResMemory:
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

func NewMinResourceList() v1.ResourceList {
	resourceList := v1.ResourceList{}
	resourceList[v1.ResourceCPU] = *resource.NewQuantity(DefaultCpuRequest, resource.DecimalSI)
	resourceList[v1.ResourceMemory] = *resource.NewQuantity(DefaultMemRequest, resource.BinarySI)
	return resourceList
}

// GPUDeviceIDX process virtual gpu
func GPUDeviceIDX(idx int) int64 {
	if idx < 0 || idx > MaxGPUIndex {
		return 0
	}
	return 1 << (idx * 2)
}

// GPUSharedDevices get shared gpu device index
func GPUSharedDevices(deviceIDX int64) []int {
	var result []int
	for idx := 0; idx <= MaxGPUIndex; idx++ {
		value := deviceIDX & 0b11
		if value > 0 {
			result = append(result, idx)
		}
		deviceIDX = deviceIDX >> 2
	}
	return result
}
