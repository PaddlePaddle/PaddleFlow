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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func Test_cleanMountPod(t *testing.T) {
	driver.InitMockDB()
	fs1, fs2 := "fs-root-fs1", "fs-root-fs2"
	nodename1, nodename2 := "mock.nodename.1", "mock.nodename.2"
	fs1mp := mountPodWithCacheID(fs1, nodename1)
	fs2mp := mountPodWithCacheID(fs2, nodename1)
	fs1mpAnoterNode := mountPodWithCacheID(fs1, nodename2)
	fs1mpAnoterNode.Name = "pfs-another.nodename_mock-pfs-" + fs1 + "-default-pv"
	mountpods := []k8sCore.Pod{fs1mp, fs2mp, fs1mpAnoterNode}
	mockCluster := model.ClusterInfo{
		ClusterType: schema.KubernetesType,
		Name:        mockClusterName,
		Model: model.Model{
			ID: mockClusterID,
		},
	}
	cluster := schema.Cluster{
		ID:   mockClusterID,
		Name: mockClusterName,
		Type: schema.KubernetesType,
	}
	err := storage.Cluster.CreateCluster(&mockCluster)
	assert.Nil(t, err)
	mockRuntime := runtime.NewKubeRuntime(cluster)
	p1 := gomonkey.ApplyFunc(expiredMountedPodsSingleCluster,
		func(cluster model.ClusterInfo, expireDuration time.Duration) (*runtime.KubeRuntime, []k8sCore.Pod, error) {
			return mockRuntime.(*runtime.KubeRuntime), mountpods, nil
		})
	defer p1.Reset()
	p2 := gomonkey.ApplyFunc(deleteMountPods,
		func(podMap map[*runtime.KubeRuntime][]k8sCore.Pod) error {
			return nil
		})
	defer p2.Reset()

	fsCache1 := model.FSCache{
		FsID:      fs1,
		NodeName:  nodename1,
		ClusterID: mockClusterID,
	}
	fsCache2 := model.FSCache{
		FsID:      fs2,
		NodeName:  nodename1,
		ClusterID: mockClusterID,
	}
	fsCache3 := model.FSCache{
		FsID:      fs1,
		NodeName:  nodename2,
		ClusterID: mockClusterID,
	}
	err = storage.FsCache.Add(&fsCache1)
	assert.Nil(t, err)
	err = storage.FsCache.Add(&fsCache2)
	assert.Nil(t, err)
	err = storage.FsCache.Add(&fsCache3)
	assert.Nil(t, err)

	l, err := storage.FsCache.List(fs1, "")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(l))
	l, err = storage.FsCache.List(fs2, "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(l))

	err = cleanMountPod(0)
	assert.Nil(t, err)
	l, err = storage.FsCache.List(fs1, "")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(l))
	l, err = storage.FsCache.List(fs2, "")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(l))
}

func Test_expiredMountedPodsSingleCluster(t *testing.T) {
	mockCluster := model.ClusterInfo{
		ClusterType: schema.KubernetesType,
		Name:        mockClusterName,
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

	mounted := mountPodWithCacheID(mockFSID, mockNodename)
	mounted.Annotations[schema.AnnotationKeyMountPrefix+"mounted"] = "/what/does/the/fox/say"
	expired := mountPodWithCacheID(mockFSID, mockNodename2)
	expired.Annotations = map[string]string{schema.AnnotationKeyMTime: "2006-01-02 15:04:05"}
	notMounted := mountPodWithCacheID(mockFSID2, mockNodename)
	notMounted.Annotations = map[string]string{schema.AnnotationKeyMTime: time.Now().Add(-6 * time.Hour).Format(model.TimeFormat)}
	podList := k8sCore.PodList{
		Items: []k8sCore.Pod{mounted, expired, notMounted},
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
			name: "12h expire with 1 pod to clean",
			args: args{
				cluster:           mockCluster,
				expireDuration:    12 * time.Hour,
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

func TestExpireTime(t *testing.T) {
	t1 := time.Now().Add(-6 * time.Hour)
	t2 := time.Now()
	expireDuration := 2 * time.Hour
	expireTime := t1.Add(expireDuration)
	fmt.Printf("-6 time vs now time: %s vs %s, %s", t1.Format(model.TimeFormat), t2.Format(model.TimeFormat), expireTime.Format(model.TimeFormat))

	assert.True(t, expireTime.Before(time.Now()))
}
