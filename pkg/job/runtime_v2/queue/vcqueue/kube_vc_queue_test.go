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

package vcqueue

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestKubeRuntimeVCQueue(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)

	maxRes, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "20",
		resources.ResMemory: "20Gi",
	})
	assert.Equal(t, nil, err)

	q := model.Queue{
		Model: model.Model{
			ID: "test_queue_id",
		},
		Name:         "test_queue_name",
		Namespace:    "default",
		QuotaType:    schema.TypeVolcanoCapabilityQuota,
		MaxResources: maxRes,
	}
	queueInfo := api.NewQueueInfo(q)

	vcQueue := New(kubeRuntimeClient)
	// create vc queue
	err = vcQueue.Create(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
	// update vc queue
	q.MaxResources.SetResources(resources.ResCPU, 50*1000)
	queueInfo = api.NewQueueInfo(q)
	err = vcQueue.Update(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
	// delete vc queue
	err = vcQueue.Delete(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
}

// TestStatusToVcQueueStatus test statusToVCQueueState
func TestStatusToVcQueueStatus(t *testing.T) {
	testCases := []struct {
		name    string
		status  string
		vcState v1beta1.QueueState
	}{
		{
			name:    "status open",
			status:  schema.StatusQueueOpen,
			vcState: v1beta1.QueueStateOpen,
		},
		{
			name:    "status closing",
			status:  schema.StatusQueueClosing,
			vcState: v1beta1.QueueStateClosing,
		},
		{
			name:    "status closed",
			status:  schema.StatusQueueClosed,
			vcState: v1beta1.QueueStateClosed,
		},
		{
			name:    "status unhkonw",
			status:  schema.StatusQueueUnavailable,
			vcState: v1beta1.QueueStateUnknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := statusToVCQueueState(tc.status)
			assert.Equal(t, state, tc.vcState)
		})
	}
}
