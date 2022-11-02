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
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
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
			if strings.Contains(listOptions.LabelSelector, mockFSID2) {
				return &podListFs2, nil
			} else {
				return &podListFs1, nil
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

func mockRuntime(t *testing.T) runtime.RuntimeService {
	driver.InitMockDB()
	mockCluster := model.ClusterInfo{
		ClusterType: schema.KubernetesType,
		Name:        mockClusterName,
		Model: model.Model{
			ID: mockClusterID,
		},
	}
	err := storage.Cluster.CreateCluster(&mockCluster)
	assert.Nil(t, err)
	cluster := schema.Cluster{
		ID:   mockCluster.ID,
		Name: mockCluster.Name,
		Type: mockCluster.ClusterType,
	}
	return runtime.NewKubeRuntime(cluster)
}

func activeNamespace(name string) k8sCore.Namespace {
	return k8sCore.Namespace{
		ObjectMeta: k8sMeta.ObjectMeta{
			Name: name,
		},
		Status: k8sCore.NamespaceStatus{Phase: k8sCore.NamespaceActive},
	}
}

func Test_getClusterNamespaceMap(t *testing.T) {
	mockRuntime := mockRuntime(t)
	pRuntime := gomonkey.ApplyFunc(runtime.GetOrCreateRuntime, func(clusterInfo model.ClusterInfo) (runtime.RuntimeService, error) {
		return mockRuntime, nil
	})
	defer pRuntime.Reset()
	pListNs := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "ListNamespaces",
		func(_ *runtime.KubeRuntime, listOptions k8sMeta.ListOptions) (*k8sCore.NamespaceList, error) {
			defaultNS := activeNamespace("default")
			custome := activeNamespace("paddleflow")
			ns := &k8sCore.NamespaceList{Items: []k8sCore.Namespace{defaultNS, custome}}
			return ns, nil
		})
	defer pListNs.Reset()

	cnm, err := getClusterNamespaceMap()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(cnm))
	for _, nsList := range cnm {
		assert.Equal(t, 2, len(nsList))
	}

	pListNs.Reset()
	pListNs = gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "ListNamespaces",
		func(_ *runtime.KubeRuntime, listOptions k8sMeta.ListOptions) (*k8sCore.NamespaceList, error) {
			return nil, fmt.Errorf("ListNamespaces")
		})
	_, err = getClusterNamespaceMap()
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "ListNamespaces"))

	pRuntime.Reset()
	pRuntime = gomonkey.ApplyFunc(runtime.GetOrCreateRuntime, func(clusterInfo model.ClusterInfo) (runtime.RuntimeService, error) {
		return nil, fmt.Errorf("GetOrCreateRuntime")
	})
	_, err = getClusterNamespaceMap()
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "GetOrCreateRuntime"))
}

func Test_patchAndDeletePvcPv(t *testing.T) {
	mockRuntime := mockRuntime(t)

	p1 := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "DeletePersistentVolumeClaim",
		func(_ *runtime.KubeRuntime, namespace string, name string, deleteOptions k8sMeta.DeleteOptions) error {
			return nil
		})
	defer p1.Reset()
	p2 := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "DeletePersistentVolume",
		func(_ *runtime.KubeRuntime, name string, deleteOptions k8sMeta.DeleteOptions) error {
			return nil
		})
	defer p2.Reset()
	p4 := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "PatchPVCFinalizerNull",
		func(_ *runtime.KubeRuntime, namespace, name string) error {
			return nil
		})
	defer p4.Reset()
	pvc := &k8sCore.PersistentVolumeClaim{}
	pvc.Finalizers = []string{"meow"}
	pPvc := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "GetPersistentVolumeClaims",
		func(_ *runtime.KubeRuntime, namespace, name string, getOptions k8sMeta.GetOptions) (*k8sCore.PersistentVolumeClaim, error) {
			return pvc, nil
		})
	defer pPvc.Reset()
	k8sRuntime := mockRuntime.(*runtime.KubeRuntime)
	err := patchAndDeletePvcPv(k8sRuntime, "ns", mockFSID)
	assert.Nil(t, err)

	p2.Reset()
	p2 = gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "DeletePersistentVolume",
		func(_ *runtime.KubeRuntime, name string, deleteOptions k8sMeta.DeleteOptions) error {
			return fmt.Errorf("DeletePersistentVolume")
		})
	err = patchAndDeletePvcPv(k8sRuntime, "ns", mockFSID)
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "DeletePersistentVolume"))

	p1.Reset()
	p1 = gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "DeletePersistentVolumeClaim",
		func(_ *runtime.KubeRuntime, namespace string, name string, deleteOptions k8sMeta.DeleteOptions) error {
			return fmt.Errorf("DeletePersistentVolumeClaim")
		})
	err = patchAndDeletePvcPv(k8sRuntime, "ns", mockFSID)
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "DeletePersistentVolumeClaim"))

	p4.Reset()
	p4 = gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "PatchPVCFinalizerNull",
		func(_ *runtime.KubeRuntime, namespace, name string) error {
			return fmt.Errorf("PatchPVCFinalizerNull")
		})
	err = patchAndDeletePvcPv(k8sRuntime, "ns", mockFSID)
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "PatchPVCFinalizerNull"))

	pPvc.Reset()
	pPvc = gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "GetPersistentVolumeClaims",
		func(_ *runtime.KubeRuntime, namespace, name string, getOptions k8sMeta.GetOptions) (*k8sCore.PersistentVolumeClaim, error) {
			return pvc, fmt.Errorf("GetPersistentVolumeClaims")
		})
	err = patchAndDeletePvcPv(k8sRuntime, "ns", mockFSID)
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "GetPersistentVolumeClaims"))

}

func Test_cleanFsResources(t *testing.T) {
	driver.InitMockDB()
	fsCache1 := model.FSCache{
		FsID:      mockFSID,
		NodeName:  mockNodename,
		ClusterID: mockClusterID,
	}
	fsCache2 := model.FSCache{
		FsID:      mockFSID2,
		NodeName:  mockNodename,
		ClusterID: mockClusterID,
	}
	fsCache3 := model.FSCache{
		FsID:      mockFSID,
		NodeName:  mockNodename2,
		ClusterID: mockClusterID,
	}
	err := storage.FsCache.Add(&fsCache1)
	assert.Nil(t, err)
	err = storage.FsCache.Add(&fsCache2)
	assert.Nil(t, err)
	err = storage.FsCache.Add(&fsCache3)
	assert.Nil(t, err)

	mockCluster := model.ClusterInfo{
		ClusterType: schema.KubernetesType,
		Name:        mockClusterName,
		Model: model.Model{
			ID: mockClusterID,
		},
	}
	err = storage.Cluster.CreateCluster(&mockCluster)
	assert.Nil(t, err)
	cluster := schema.Cluster{
		ID:   mockCluster.ID,
		Name: mockCluster.Name,
		Type: mockCluster.ClusterType,
	}
	mockRuntime := runtime.NewKubeRuntime(cluster)

	notMountedFs1 := mountPodWithCacheID(mockFSID, mockNodename)
	notMountedFs2 := mountPodWithCacheID(mockFSID2, mockNodename)
	runtimePodsMap := make(map[*runtime.KubeRuntime][]k8sCore.Pod, 0)
	runtimePodsMap[mockRuntime.(*runtime.KubeRuntime)] = []k8sCore.Pod{notMountedFs1, notMountedFs2}

	p1 := gomonkey.ApplyFunc(deletePvPvc, func(fsID string) error {
		return nil
	})
	defer p1.Reset()
	p3 := gomonkey.ApplyMethod(reflect.TypeOf(mockRuntime), "DeletePod",
		func(_ *runtime.KubeRuntime, namespace, name string) error {
			return nil
		})
	defer p3.Reset()

	err = GetFileSystemService().cleanFsResources(runtimePodsMap, mockFSID)
	assert.Nil(t, err)

	l, err := storage.FsCache.List(mockFSID, "")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(l))
	l, err = storage.FsCache.List(mockFSID2, "")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(l))
}

func Test_FileSystem(t *testing.T) {
	driver.InitMockDB()

	svc := GetFileSystemService()

	ctx := &logger.RequestContext{UserName: mockRootName}
	listReq := &ListFileSystemRequest{}
	fsList, _, err := svc.ListFileSystem(ctx, listReq)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fsList))

	_, err = svc.GetFileSystem(mockRootName, mockFSName)
	assert.NotNil(t, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "not found"))

	createRep := CreateFileSystemRequest{
		Name:       mockFSName,
		Username:   mockRootName,
		Url:        "mockUrl",
		Properties: map[string]string{"cat": "meow"},
	}
	fs, err := svc.CreateFileSystem(ctx, &createRep)
	assert.Nil(t, err)
	assert.Equal(t, createRep.Name, fs.Name)
	assert.Equal(t, createRep.Username, fs.UserName)
	fsType, serverAddress, subPath := common.InformationFromURL(createRep.Url, createRep.Properties)
	assert.Equal(t, fsType, fs.Type)
	assert.Equal(t, serverAddress, fs.ServerAddress)
	assert.Equal(t, subPath, fs.SubPath)

	fsGet, err := svc.GetFileSystem(mockRootName, mockFSName)
	assert.Nil(t, err)
	assert.Equal(t, fs.Name, fsGet.Name)

	err = svc.DeleteFileSystem(ctx, mockFSID)
	assert.Nil(t, err)
}
