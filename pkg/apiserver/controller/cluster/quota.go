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

package cluster

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/schema"
)

type Quota struct {
	Cpu                int64                      `json:"cpu"`
	RawMemory          *resource.Quantity         `json:"-"`
	Memory             string                     `json:"memory"`
	RawStorage         *resource.Quantity         `json:"-"`
	Storage            string                     `json:"storage"`
	RawScalarResources map[string]int64           `json:"-"`
	ScalarResources    schema.ScalarResourcesType `json:"scalarResources,omitempty"`
}
type NodeQuotaInfo struct {
	NodeName    string `json:"nodeName"`
	Schedulable bool   `json:"schedulable"`
	Total       Quota  `json:"total"`
	Idle        Quota  `json:"idle"`
}

type ClusterQuotaSummary struct {
	TotalQuota Quota `json:"total"`
	IdleQuota  Quota `json:"idle"`
}

func quantityToMB(quantity *resource.Quantity) string {
	floatVal := float64(quantity.Value()) / 1024.0 / 1024.0
	return fmt.Sprintf("%.2fMi", floatVal)
}

func newQuota(resourceList corev1.ResourceList) Quota {
	quota := Quota{}

	if resourceList == nil {
		quota.RawMemory = resource.NewQuantity(0, resource.BinarySI)
		quota.RawStorage = resource.NewQuantity(0, resource.BinarySI)
		quota.Cpu = 0
	} else {
		quota.RawMemory = resourceList.Memory()
		quota.RawStorage = resourceList.StorageEphemeral()
		quota.Cpu = resourceList.Cpu().Value()
	}

	if quota.RawMemory != nil {
		quota.Memory = quantityToMB(quota.RawMemory)
	}
	if quota.RawStorage != nil {
		quota.Storage = quantityToMB(quota.RawStorage)
	}

	quota.initScalarResources(resourceList)

	return quota
}

func (q *Quota) initScalarResources(resourceList corev1.ResourceList) {
	q.RawScalarResources = map[string]int64{}
	scalarResourceArray := config.GlobalServerConfig.Job.ScalarResourceArray

	for _, scalarResource := range scalarResourceArray {
		resourceName := corev1.ResourceName(scalarResource)

		if quantity, found := resourceList[resourceName]; found {
			value, success := quantity.AsInt64()
			if !success {
				log.Errorf("quantity.AsInt64 error! scalarResourceName: %s", scalarResource)
				continue
			}
			q.RawScalarResources[scalarResource] = value
		}
	}
}

func (q *Quota) updateScalarResources(other Quota, fn func(int64, int64) int64) {
	for resourceName, otherValue := range other.RawScalarResources {
		value, found := q.RawScalarResources[resourceName]
		if found {
			q.RawScalarResources[resourceName] = fn(value, otherValue)
		} else {
			q.RawScalarResources[resourceName] = otherValue
		}
	}
}

func (q *Quota) sub(other Quota) {
	q.Cpu -= other.Cpu

	if other.RawMemory != nil {
		q.RawMemory.Sub(*other.RawMemory)
	}
	if other.RawStorage != nil {
		q.RawStorage.Sub(*other.RawStorage)
	}

	q.updateScalarResources(other, func(x, y int64) int64 {
		return x - y
	})
}

func (q *Quota) add(other Quota) {
	q.Cpu += other.Cpu

	if other.RawMemory != nil {
		q.RawMemory.Add(*other.RawMemory)
	}
	if other.RawStorage != nil {
		q.RawStorage.Add(*other.RawStorage)
	}

	q.updateScalarResources(other, func(x, y int64) int64 {
		return x + y
	})
}

// 生成human readable的数据，RawMemory -> memory, RawStorage -> storage
func (q *Quota) quantityToString() {
	q.ScalarResources = schema.ScalarResourcesType{}
	if q.RawMemory != nil {
		q.Memory = quantityToMB(q.RawMemory)
	}
	if q.RawStorage != nil {
		q.Storage = quantityToMB(q.RawStorage)
	}
	if q.RawScalarResources != nil {
		for resourceName, value := range q.RawScalarResources {
			q.ScalarResources[corev1.ResourceName(resourceName)] = fmt.Sprintf("%d", value)
		}
	}
}

// TODO，对于共享GPU，额外返回各个GPU卡的剩余cgpu、cgpu_memory等信息
func GetNodeQuotaList(clientSet *kubernetes.Clientset) (ClusterQuotaSummary, []NodeQuotaInfo) {
	result := []NodeQuotaInfo{}
	summary := ClusterQuotaSummary{
		TotalQuota: newQuota(nil),
		IdleQuota:  newQuota(nil),
	}
	nodes, _ := clientSet.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	log.Infof("GetNodeQuotaList nodes Items len: %d", len(nodes.Items))

	for _, node := range nodes.Items {
		nodeSchedulable := !node.Spec.Unschedulable
		// 过滤掉不能调度的节点
		if !nodeSchedulable {
			continue
		}
		totalQuota := newQuota(node.Status.Allocatable)
		idleQuota := newQuota(node.Status.Allocatable)
		nodeName := node.ObjectMeta.Name

		fieldSelector := "status.phase!=Succeeded,status.phase!=Failed," +
			"status.phase!=Unknown,spec.nodeName=" + nodeName

		pods, _ := clientSet.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
			FieldSelector: fieldSelector,
		})
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				containerQuota := newQuota(container.Resources.Requests)
				idleQuota.sub(containerQuota)
			}
		}

		totalQuota.quantityToString()
		idleQuota.quantityToString()
		nodeQuota := NodeQuotaInfo{
			NodeName:    nodeName,
			Schedulable: nodeSchedulable,
			Total:       totalQuota,
			Idle:        idleQuota,
		}
		result = append(result, nodeQuota)
		summary.TotalQuota.add(totalQuota)
		summary.IdleQuota.add(idleQuota)
	}
	summary.TotalQuota.quantityToString()
	summary.IdleQuota.quantityToString()

	return summary, result
}
