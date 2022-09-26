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
	"encoding/json"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/agiledragon/gomonkey/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	testTargetPath = "/var/lib/kubelet/pods/abc/volumes/kubernetes.io~csi/pfs-fs-root-test-default-pv/mount"
	mockClusterID  = "cluster-mock"
	mockFSID       = "fs-root-mock"
	mockNodename   = "nodename_mock"
)

func Test_getClusterRuntimeMap(t *testing.T) {
	driver.InitMockDB()
	mockList := []model.ClusterInfo{
		{
			Model:         model.Model{ID: "cluster-kube1"},
			Name:          "kube1",
			NamespaceList: []string{"default", "paddleflow", "kube-system"},
			ClusterType:   schema.KubernetesType,
		},
		{
			Model:         model.Model{ID: "cluster-kube2"},
			Name:          "kube2",
			NamespaceList: []string{"paddleflow"},
			ClusterType:   schema.KubernetesType,
		},
		{
			Model:       model.Model{ID: "cluster-local"},
			Name:        "local",
			ClusterType: schema.LocalType,
		},
	}
	for _, cluster := range mockList {
		err := storage.Cluster.CreateCluster(&cluster)
		assert.Nil(t, err)
	}

	patch := gomonkey.ApplyFunc(runtime.CreateRuntime, func(clusterInfo model.ClusterInfo) (runtime.RuntimeService, error) {
		cluster := schema.Cluster{
			Name: clusterInfo.Name,
			ID:   clusterInfo.ID,
			Type: clusterInfo.ClusterType,
			ClientOpt: schema.ClientOptions{
				Master: clusterInfo.Endpoint,
				Config: clusterInfo.Credential,
				QPS:    1000,
				Burst:  1000,
			},
		}
		return runtime.NewKubeRuntime(cluster), nil
	})
	defer patch.Reset()

	crm, err := getClusterRuntimeMap()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(crm))
	for clusterID, rt := range crm {
		rt.Name()
		assert.Equal(t, true, strings.HasPrefix(clusterID, "cluster-kube"))
		assert.Equal(t, true, strings.HasPrefix(rt.Name(), "kube"))
	}
}

func buildCacheStats() model.CacheStats {
	return model.CacheStats{
		FsID:     mockFSID,
		CacheDir: "/var/cache",
		NodeName: mockNodename,
		UsedSize: 100,
	}
}

func buildFSCache() model.FSCache {
	return model.FSCache{
		FsID:      mockFSID,
		CacheDir:  "/var/cache",
		NodeName:  mockNodename,
		UsedSize:  100,
		ClusterID: mockClusterID,
	}
}

func Test_addOrUpdateFSCache(t *testing.T) {
	driver.InitMockDB()
	createCache := buildFSCache()
	updateCache := buildFSCache()
	updateCache.UsedSize = 666

	tests := []struct {
		name    string
		cache   model.FSCache
		wantErr bool
	}{
		{
			name:    "create-cache",
			cache:   createCache,
			wantErr: false,
		},
		{
			name:    "update-cache",
			cache:   updateCache,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := addOrUpdateFSCache(&tt.cache)
			assert.Nil(t, err)
			listCache, err := storage.FsCache.List(tt.cache.FsID, "")
			assert.Nil(t, err)
			assert.Equal(t, 1, len(listCache))
			assert.Equal(t, tt.cache.ClusterID, listCache[0].ClusterID)
			assert.Equal(t, tt.cache.NodeName, listCache[0].NodeName)
			assert.Equal(t, tt.cache.UsedSize, listCache[0].UsedSize)
			assert.Equal(t, tt.cache.CacheDir, listCache[0].CacheDir)
		})
	}
}

func mountPodWithCacheStats() *k8sCore.Pod {
	stats := buildCacheStats()
	statsStr, _ := json.Marshal(stats)

	pod := baseMountPod()
	pod.Annotations[schema.AnnotationKeyCache] = string(statsStr)
	return pod
}

func baseMountPod() *k8sCore.Pod {
	return &k8sCore.Pod{
		ObjectMeta: k8sMeta.ObjectMeta{
			Name:      "pfs-nodename_mock-pfs-fs-root-mock-default-pv",
			Namespace: schema.MountPodNamespace,
			Labels:    map[string]string{csiconfig.PodTypeKey: csiconfig.PodMount},
			Annotations: map[string]string{
				schema.AnnotationKeyMountPrefix + utils.GetPodUIDFromTargetPath(testTargetPath): testTargetPath,
				schema.AnnotationKeyMTime: time.Now().Format(model.TimeFormat),
			},
		},
		Status: k8sCore.PodStatus{
			Phase: k8sCore.PodRunning,
			Conditions: []k8sCore.PodCondition{{
				Type:   k8sCore.PodReady,
				Status: k8sCore.ConditionTrue,
			}, {
				Type:   k8sCore.ContainersReady,
				Status: k8sCore.ConditionTrue,
			}},
		},
	}
}

func Test_syncCacheFromMountPod(t *testing.T) {
	driver.InitMockDB()
	tests := []struct {
		name    string
		pod     *k8sCore.Pod
		wantErr bool
		wantLen int
	}{
		{
			name:    "mount-pod",
			pod:     baseMountPod(),
			wantErr: false,
			wantLen: 0,
		},
		{
			name:    "mount-pod-cache",
			pod:     mountPodWithCacheStats(),
			wantErr: false,
			wantLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := syncCacheFromMountPod(tt.pod, mockClusterID); (err != nil) != tt.wantErr {
				t.Errorf("PodMount() error = %v, wantErr %v", err, tt.wantErr)
			}
			listCache, err := storage.FsCache.List(mockFSID, "")
			assert.Nil(t, err)
			assert.Equal(t, tt.wantLen, len(listCache))
		})
	}
}
