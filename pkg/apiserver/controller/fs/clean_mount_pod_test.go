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

package fs

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func Test_expiredMountedPodsSingleCluster(t *testing.T) {
	mockCluster := model.ClusterInfo{
		ClusterType: schema.KubernetesType,
		Name:        "mockClusterID",
		Model: model.Model{
			ID: mockClusterID,
		},
	}
	cluster := schema.Cluster{
		ID:   mockCluster.ID,
		Name: mockCluster.Name,
		Type: mockCluster.ClusterType,
	}
	mockRuntime := runtime.NewKubeRuntime(cluster)

	mounted := baseMountPod()
	expired := baseMountPod()
	expired.Annotations = map[string]string{schema.AnnotationKeyMTime: "1603772423"}
	notMounted := baseMountPod()
	notMounted.Annotations = map[string]string{schema.AnnotationKeyMTime: time.Now().Format(model.TimeFormat)}
	podList := k8sCore.PodList{
		Items: []k8sCore.Pod{*mounted, *expired, *notMounted},
	}
	pRuntime := gomonkey.ApplyFunc(runtime.GetOrCreateRuntime, func(clusterInfo model.ClusterInfo) (runtime.RuntimeService, error) {
		return mockRuntime, nil
	})
	defer pRuntime.Reset()
	pListPod := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "ListPods",
		func(_ *runtime.KubeRuntime, namespace string, listOptions k8sMeta.ListOptions) (*k8sCore.PodList, error) {
			return &podList, nil
		})
	defer pListPod.Reset()

	rt, podMap, err := expiredMountedPodsSingleCluster(mockCluster, 2*time.Hour)
	assert.Nil(t, err)
	assert.Equal(t, mockRuntime, rt)
	assert.Equal(t, 1, len(podMap))

	rt, podMap, err = expiredMountedPodsSingleCluster(mockCluster, 0)
	assert.Nil(t, err)
	assert.Equal(t, mockRuntime, rt)
	assert.Equal(t, 2, len(podMap))

	mockCluster.ClusterType = schema.LocalType
	rt, podMap, err = expiredMountedPodsSingleCluster(mockCluster, 0)
	assert.Nil(t, err)
	assert.Equal(t, nil, rt)
	assert.Equal(t, 0, len(podMap))
}
