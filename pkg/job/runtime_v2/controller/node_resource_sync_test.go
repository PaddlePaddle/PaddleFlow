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

package controller

import (
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/util/kubeutil"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func newFakeNodeResourceCtrl() *NodeResourceSync {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	ctrl := &NodeResourceSync{}
	opt := client.NewFakeKubeRuntimeClient(server)
	err := ctrl.Initialize(opt)
	if err != nil {
		log.Errorf("initialize controller failed: %v", err)
	}
	return ctrl
}

func initNodeData(client kubernetes.Interface, nodeCount int) error {
	var reqList = []corev1.ResourceList{
		kubeutil.BuildResourceList("56", "256Gi"),
		kubeutil.BuildResourceList("48", "128Gi"),
		kubeutil.BuildResourceList("64", "512Gi"),
		kubeutil.BuildResourceList("96", "768Gi"),
	}
	var labelList = []map[string]string{
		{
			schema.PFNodeLabels: "cpu-1",
		},
		{
			"xxx/resource-pool": "cpu-2",
		},
		{
			"xxx/resource-pool": "xpu-3",
		},
	}
	return kubeutil.CreateNodes(client, nodeCount, reqList, kubeutil.NodeCondList, labelList)
}

func initPodData(client kubernetes.Interface, podCount int) error {
	var namespaceList = []string{"default", "test-1", "test-b", "test-c", "test-a"}
	var nodeNameList = []string{"instance-0", "instance-1", "instance-a", "instance-b"}
	var reqList = []corev1.ResourceList{
		kubeutil.BuildResourceList("0", "0Gi"),
		kubeutil.BuildResourceList("1", "2Gi"),
		kubeutil.BuildResourceList("2", "4Gi"),
		kubeutil.BuildResourceList("1", "5Gi"),
		kubeutil.BuildResourceList("1", "8Gi"),
	}
	return kubeutil.CreatePods(client, podCount, namespaceList, nodeNameList, kubeutil.PhaseList, reqList)
}

func TestNodeResourceSync_Run(t *testing.T) {
	testCases := []struct {
		caseName   string
		nodeCount  int
		podPerNode int
		err        error
	}{
		{
			caseName:   "1k node 1, 20 pod per node",
			nodeCount:  1000,
			podPerNode: 20,
			err:        nil,
		},
		{
			caseName:   "1k node 2, 22 pod per node",
			nodeCount:  1000,
			podPerNode: 22,
			err:        nil,
		},
		{
			caseName:   "1k node 3, 23 pod per node",
			nodeCount:  1000,
			podPerNode: 23,
			err:        nil,
		},
		{
			caseName:   "1k node 4, 23 pod per node",
			nodeCount:  1000,
			podPerNode: 23,
			err:        nil,
		},
		{
			caseName:   "1k node 5, 25 pod per node",
			nodeCount:  1000,
			podPerNode: 25,
			err:        nil,
		},
		{
			caseName:   "1k node 6, 26 pod per node",
			nodeCount:  1000,
			podPerNode: 26,
			err:        nil,
		},
		{
			caseName:   "1k node 6, 27 pod per node",
			nodeCount:  1000,
			podPerNode: 27,
			err:        nil,
		},
		{
			caseName:   "2k node, 20 pod per node",
			nodeCount:  2000,
			podPerNode: 20,
			err:        nil,
		},
		{
			caseName:   "1k node, 30 pod per node",
			nodeCount:  1000,
			podPerNode: 30,
			err:        nil,
		},
		{
			caseName:   "1k node, 50 pod per node",
			nodeCount:  1000,
			podPerNode: 50,
			err:        nil,
		},
	}

	err := driver.InitCache("DEBUG")
	assert.Equal(t, nil, err)

	for _, test := range testCases {
		test := test
		t.Run(test.caseName, func(t *testing.T) {
			t.Parallel()
			ctrl := newFakeNodeResourceCtrl()
			//  init data
			client := ctrl.runtimeClient.(*client.KubeRuntimeClient)
			err = initNodeData(client.Client, test.nodeCount)
			assert.Equal(t, nil, err)
			err = initPodData(client.Client, test.nodeCount*test.podPerNode)
			assert.Equal(t, nil, err)
			// Run worker
			stopCh := make(chan struct{})
			ctrl.Run(stopCh)
			// wait
			time.Sleep(100 * time.Millisecond)
			for ctrl.taskQueue.Len() != 0 {
				time.Sleep(100 * time.Millisecond)
			}
			close(stopCh)
		})
	}

}
