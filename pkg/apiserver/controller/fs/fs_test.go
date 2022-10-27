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
	log "github.com/sirupsen/logrus"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	testTargetPath  = "/var/lib/kubelet/pods/abc/volumes/kubernetes.io~csi/pfs-fs-root-test-default-pv/mount"
	mockClusterID   = "cluster-mock"
	mockClusterName = "cluster-name-mock"
	mockFSID        = "fs-root-mock"
	mockFSID2       = "fs-root-mock2"
	mockNodename    = "nodename_mock"
	mockNodename2   = "nodename_mock2"
	mockCacheDir    = "/var/cache"
	mockFSName      = "mock"
	mockRootName    = "root"
)

func mountPodWithCacheID(fsID, nodename string) k8sCore.Pod {
	return k8sCore.Pod{
		ObjectMeta: k8sMeta.ObjectMeta{
			Name:      "pfs-" + nodename + "-pfs-" + fsID + "-default-pv",
			Namespace: schema.MountPodNamespace,
			Labels: map[string]string{
				csiconfig.PodTypeKey:   csiconfig.PodMount,
				schema.LabelKeyFsID:    fsID,
				schema.LabelKeyCacheID: model.CacheID(mockClusterID, nodename, "", fsID),
			},
			Annotations: map[string]string{
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

func Test_checkFsMountedSingleCluster(t *testing.T) {
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

	notMountedFs1 := mountPodWithCacheID(mockFSID, mockNodename)
	mounted := mountPodWithCacheID(mockFSID, mockNodename2)
	mounted.Annotations[schema.AnnotationKeyMountPrefix+"dogcatrabbit"] = "/talking/to/the/moon"
	notMountedFs2 := mountPodWithCacheID(mockFSID2, mockNodename)
	podListFs1 := k8sCore.PodList{
		Items: []k8sCore.Pod{notMountedFs1, mounted},
	}
	podListFs2 := k8sCore.PodList{
		Items: []k8sCore.Pod{notMountedFs2},
	}
	pRuntime := gomonkey.ApplyFunc(runtime.GetOrCreateRuntime, func(clusterInfo model.ClusterInfo) (runtime.RuntimeService, error) {
		return mockRuntime, nil
	})
	defer pRuntime.Reset()
	pListPod := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "ListPods",
		func(_ *runtime.KubeRuntime, namespace string, listOptions k8sMeta.ListOptions) (*k8sCore.PodList, error) {
			log.Warnf("elsie list po listLabel: %s", listOptions.LabelSelector)
			if strings.Contains(listOptions.LabelSelector, mockFSID) {
				log.Warnf("elsie list po fs1")
				return &podListFs1, nil
			} else if strings.Contains(listOptions.LabelSelector, mockFSID2) {
				log.Warnf("elsie list po fs2")
				return &podListFs2, nil
			} else {
				return nil, nil
			}
		})
	defer pListPod.Reset()

	type args struct {
		cluster           model.ClusterInfo
		fsID              string
		expectMounted     bool
		lensOfPodsToClean int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "fs1 mounted",
			args: args{
				cluster:           mockCluster,
				fsID:              mockFSID,
				expectMounted:     true,
				lensOfPodsToClean: 0,
			},
		},
		{
			name: "fs2 not mounted",
			args: args{
				cluster:           mockCluster,
				fsID:              mockFSID2,
				expectMounted:     false,
				lensOfPodsToClean: 1,
			},
		},
		{
			name: "local cluster",
			args: args{
				cluster:           localCluster,
				fsID:              mockFSID,
				expectMounted:     false,
				lensOfPodsToClean: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mounted, _, podList, err := checkFsMountedSingleCluster(tt.args.cluster, tt.args.fsID)
			assert.Nil(t, err)
			assert.Equal(t, tt.args.expectMounted, mounted)
			assert.Equal(t, tt.args.lensOfPodsToClean, len(podList))
		})
	}
}
