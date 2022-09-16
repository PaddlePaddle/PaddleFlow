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

package elasticqueue

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestKubeRuntimeElasticQuota(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)

	q := model.Queue{
		Model: model.Model{
			ID: "test_queue_id",
		},
		Name:      "test_queue_name",
		Namespace: "default",
		QuotaType: schema.TypeElasticQuota,
		MaxResources: &resources.Resource{
			Resources: map[string]resources.Quantity{
				"cpu": 20 * 1000,
				"mem": 20 * 1024 * 1024 * 1024,
			},
		},
		MinResources: &resources.Resource{
			Resources: map[string]resources.Quantity{
				"cpu": 10 * 1000,
				"mem": 10 * 1024 * 1024 * 1024,
			},
		},
	}
	queueInfo := api.NewQueueInfo(q)
	eQuota := New(kubeRuntimeClient)
	// create elastic quota
	err := eQuota.Create(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
	// update elastic quota
	q.MaxResources = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"cpu": 30 * 1000,
			"mem": 30 * 1024 * 1024 * 1024,
		},
	}
	queueInfo = api.NewQueueInfo(q)
	err = eQuota.Update(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
	// delete elastic quota
	err = eQuota.Delete(context.TODO(), queueInfo)
	assert.Equal(t, nil, err)
}
