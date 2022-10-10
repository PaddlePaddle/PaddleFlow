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

package mount

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	k8sCore "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

var testNew = &k8sCore.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Name:        "pfs-test-node-fs-root-testfs",
		Namespace:   "default",
		Annotations: map[string]string{},
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

var testTargetPath = "/var/lib/kubelet/pods/abc/volumes/kubernetes.io~csi/pfs-fs-root-test-default-pv/mount"
var testTargetPath2 = "/var/lib/kubelet/pods/def/volumes/kubernetes.io~csi/pfs-fs-root-test-default-pv/mount"

var testExist = &k8sCore.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "pfs-test-node-fs-root-testfs",
		Namespace: "default",
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

func TestPFSMountWithCache(t *testing.T) {
	csiconfig.Namespace = "default"
	csiconfig.NodeName = "node1"
	fakeClientSet := utils.GetFakeK8sClient()
	fs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "testfs",
		Type:          "s3",
		SubPath:       "/supath",
		ServerAddress: "server_address",

		PropertiesMap: map[string]string{
			"accessKey":     "accessKey",
			"bucket":        "bucket",
			"endpoint":      "server_address",
			"region":        "bj",
			"secretKey":     "secretKey",
			common.DirMode:  "0755",
			common.FileMode: "0644",
		},
	}
	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

	fsCache := model.FSCacheConfig{
		FsID:       fs.ID,
		CacheDir:   "/data/paddleflow-FS/mnt",
		MetaDriver: "disk",
		BlockSize:  4096,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	fsCacheStr, err := json.Marshal(fsCache)
	assert.Nil(t, err)
	fsCacheBase64 := base64.StdEncoding.EncodeToString(fsCacheStr)

	info, err := ConstructMountInfo(fsBase64, fsCacheBase64, testTargetPath, fakeClientSet, false)
	assert.Nil(t, err)

	patch1 := ApplyFunc(isPodReady, func(pod *k8sCore.Pod) bool {
		return true
	})
	defer patch1.Reset()

	type args struct {
		volumeID  string
		mountInfo Info
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "fuse-pod",
			args: args{
				volumeID:  "aaaaa",
				mountInfo: info,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := PFSMount(tt.args.volumeID, tt.args.mountInfo); (err != nil) != tt.wantErr {
				t.Errorf("PodMount() error = %v, wantErr %v", err, tt.wantErr)
			}
			newPod, errGetpod := tt.args.mountInfo.K8sClient.GetPod(csiconfig.Namespace, GeneratePodNameByVolumeID(tt.args.volumeID))
			assert.Nil(t, errGetpod)
			assert.Equal(t, GeneratePodNameByVolumeID(tt.args.volumeID), newPod.Name)
			assert.Equal(t, csiconfig.Namespace, newPod.Namespace)
			assert.Equal(t, testTargetPath, newPod.Annotations[schema.AnnotationKeyMountPrefix+utils.GetPodUIDFromTargetPath(testTargetPath)])
			assert.Equal(t, "mkdir -p /home/paddleflow/mnt/storage;"+
				"/home/paddleflow/pfs-fuse mount --mount-point="+FusePodMountPoint+" --fs-id=fs-root-testfs --fs-info="+fsBase64+
				" --block-size=4096 --meta-cache-driver=disk --file-mode=0644 --dir-mode=0755"+
				" --data-cache-path="+FusePodCachePath+DataCacheDir+
				" --meta-cache-path="+FusePodCachePath+MetaCacheDir, newPod.Spec.Containers[0].Command[2])
		})
	}
}

func Test_addRef(t *testing.T) {
	fakeClientSet := utils.GetFakeK8sClient()
	type args struct {
		c          utils.Client
		pod        *k8sCore.Pod
		targetPath string
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantAnno map[string]string
	}{
		{
			name: "new pod add annotation",
			args: args{
				c:          fakeClientSet,
				pod:        testNew,
				targetPath: testTargetPath,
			},
			wantErr: false,
			wantAnno: map[string]string{
				schema.AnnotationKeyMountPrefix + utils.GetPodUIDFromTargetPath(testTargetPath): testTargetPath,
				schema.AnnotationKeyMTime: time.Now().Format(model.TimeFormat),
			},
		},
		{
			name: "exist pod add annotation",
			args: args{
				c:          fakeClientSet,
				pod:        testExist,
				targetPath: testTargetPath2,
			},
			wantErr: false,
			wantAnno: map[string]string{
				schema.AnnotationKeyMountPrefix + utils.GetPodUIDFromTargetPath(testTargetPath):  testTargetPath,
				schema.AnnotationKeyMountPrefix + utils.GetPodUIDFromTargetPath(testTargetPath2): testTargetPath2,
				schema.AnnotationKeyMTime: time.Now().Format(model.TimeFormat),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.pod != nil {
				_, _ = tt.args.c.CreatePod(tt.args.pod)
			}
			if err := addRef(tt.args.c, tt.args.pod, tt.args.targetPath); (err != nil) != tt.wantErr {
				t.Errorf("addRef() error = %v, wantErr %v", err, tt.wantErr)
			}
			newPod, _ := tt.args.c.GetPod("default", "pfs-test-node-fs-root-testfs")
			if newPod == nil ||
				newPod.Annotations[schema.AnnotationKeyMountPrefix+utils.GetPodUIDFromTargetPath(testTargetPath)] != tt.wantAnno[schema.AnnotationKeyMountPrefix+utils.GetPodUIDFromTargetPath(testTargetPath)] ||
				newPod.Annotations[schema.AnnotationKeyMountPrefix+utils.GetPodUIDFromTargetPath(testTargetPath2)] != tt.wantAnno[schema.AnnotationKeyMountPrefix+utils.GetPodUIDFromTargetPath(testTargetPath2)] {
				t.Errorf("waitUntilMount() got = %v, wantAnnotation = %v", newPod, tt.wantAnno)
			}
			if len(newPod.Annotations) != len(tt.wantAnno) {
				t.Errorf("pod annotions nums not correct () got = %v, wantAnnotation = %v", newPod, tt.wantAnno)
			}

		})
	}
}
