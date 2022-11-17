/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package kubeutil

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

var (
	PhaseList    = []v1.PodPhase{v1.PodPending, v1.PodRunning, v1.PodSucceeded, v1.PodFailed, v1.PodUnknown}
	NodeCondList = []v1.NodeCondition{
		{
			Message: "kubelet is posting ready status",
			Reason:  "KubeletReady",
			Status:  "True",
			Type:    "Ready",
		},
		{
			Message: "kubelet is posting not ready status",
			Reason:  "KubeletNotReady",
			Status:  "False",
			Type:    "NotReady",
		},
	}
)

func BuildResourceList(cpu string, memory string) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourceMemory: resource.MustParse(memory),
	}
}

func BuildFakePod(name, namespace, nodeName string, podPhase v1.PodPhase, req v1.ResourceList) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         types.UID(fmt.Sprintf("%s-%s", namespace, name)),
			Name:        name,
			Namespace:   namespace,
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: req,
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: podPhase,
		},
	}
}

func BuildFakeNode(name string, unsched bool, alloc v1.ResourceList, nodeCond v1.NodeCondition, labels map[string]string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: map[string]string{},
		},
		Spec: v1.NodeSpec{
			Unschedulable: unsched,
		},
		Status: v1.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
			Conditions: []v1.NodeCondition{
				nodeCond,
			},
		},
	}
}

func CreatePods(fakeClient kubernetes.Interface, podCount int, nsList []string, nodeList []string,
	phaseList []v1.PodPhase, reqList []v1.ResourceList) error {
	nsLen := len(nsList)
	nodeLen := len(nodeList)
	phaseLen := len(phaseList)
	reqLen := len(reqList)

	var name, namespace string
	var err error
	rand.Seed(time.Now().Unix())
	for idx := 0; idx < podCount; idx++ {
		name = uuid.GenerateIDWithLength("test-pod", 16)
		namespace = nsList[rand.Intn(nsLen)]
		p := BuildFakePod(
			name,
			namespace,
			nodeList[rand.Intn(nodeLen)],
			phaseList[rand.Intn(phaseLen)],
			reqList[rand.Intn(reqLen)])
		_, err = fakeClient.CoreV1().Pods(namespace).Create(context.TODO(), p, metav1.CreateOptions{})
		if err != nil {
			break
		}
	}
	return err
}

func CreateNodes(fakeClient kubernetes.Interface, nodeCount int, allocList []v1.ResourceList,
	condList []v1.NodeCondition, labelList []map[string]string) error {
	reqLen := len(allocList)
	labelLen := len(labelList)
	condLen := len(condList)
	schedList := []bool{true, false}

	var name string
	var err error
	rand.Seed(time.Now().Unix())
	for idx := 0; idx < nodeCount; idx++ {
		name = fmt.Sprintf("instance-%d", idx)
		node := BuildFakeNode(
			name,
			schedList[rand.Intn(2)],
			allocList[rand.Intn(reqLen)],
			condList[rand.Intn(condLen)],
			labelList[rand.Intn(labelLen)])
		_, err = fakeClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
		if err != nil {
			break
		}
	}
	return nil
}
