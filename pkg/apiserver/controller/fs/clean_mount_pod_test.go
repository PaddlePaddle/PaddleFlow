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
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
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
	localCluster := model.ClusterInfo{
		ClusterType: schema.LocalType,
		Name:        "mockClusterLocal",
		Model: model.Model{
			ID: "local",
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
	expired.Annotations = map[string]string{schema.AnnotationKeyMTime: "2006-01-02 15:04:05"}
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

	type args struct {
		cluster           model.ClusterInfo
		expireDuration    time.Duration
		runtimeExpected   runtime.RuntimeService
		lensOfPodsToClean int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "2h expire with 2 pods to clean",
			args: args{
				cluster:           mockCluster,
				expireDuration:    2 * time.Hour,
				runtimeExpected:   mockRuntime,
				lensOfPodsToClean: 2,
			},
		},
		{
			name: "0s expire with 1 pod to clean",
			args: args{
				cluster:           mockCluster,
				expireDuration:    0,
				runtimeExpected:   mockRuntime,
				lensOfPodsToClean: 1,
			},
		},
		{
			name: "local cluster to skip",
			args: args{
				cluster:           localCluster,
				expireDuration:    0,
				runtimeExpected:   (*runtime.KubeRuntime)(nil),
				lensOfPodsToClean: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, podMap, err := expiredMountedPodsSingleCluster(tt.args.cluster, tt.args.expireDuration)
			assert.Nil(t, err)
			assert.Equal(t, tt.args.runtimeExpected, rt)
			assert.Equal(t, tt.args.lensOfPodsToClean, len(podMap))
		})
	}
}
