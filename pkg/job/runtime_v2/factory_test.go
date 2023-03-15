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

package runtime_v2

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestCreateRuntime(t *testing.T) {
	k3sClusterInfo := model.ClusterInfo{ClusterType: schema.K3SType}
	k8sClusterInfo := model.ClusterInfo{ClusterType: schema.KubernetesType}
	k3sSvc, err := CreateRuntime(k3sClusterInfo)
	assert.NotNil(t, err)
	assert.Nil(t, k3sSvc)
	k8sSvc, err := CreateRuntime(k8sClusterInfo)
	assert.NotNil(t, err)
	assert.Nil(t, k8sSvc)
	// new k8s runtime server
	newK8sRuntime := newK8sRuntimeSvc()
	newK3sRuntime := newK3sRuntimeSvc()
	var PFRuntimeMap sync.Map
	PFRuntimeMap.Store("k8s", newK8sRuntime)
	PFRuntimeMap.Store("k3s", newK3sRuntime)
	var runtimeSvc RuntimeService
	runtimeSvc = newK8sRuntime.(RuntimeService)
	if runtimeS, ok := PFRuntimeMap.Load("k8s"); ok {
		runtimeSvc = runtimeS.(RuntimeService)
	}
	assert.Panics(t, func() {
		_ = runtimeSvc.(*K3SRuntimeService)
	})
}

func newK8sRuntimeSvc() RuntimeService {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	return &KubeRuntime{cluster: schema.Cluster{Type: schema.KubernetesType, ID: "k8s"}, kubeClient: client.NewFakeKubeRuntimeClient(server)}
}
func newK3sRuntimeSvc() RuntimeService {
	var server2 = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server2.Close()
	return &K3SRuntimeService{cluster: &schema.Cluster{Type: schema.K3SType, ID: "k3s"}, client: client.NewFakeK3SRuntimeClient(server2)}
}
