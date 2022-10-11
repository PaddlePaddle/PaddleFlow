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

package runtime

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

const (
	testJobID   = "test_pf_id"
	jobManifest = `
apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: null
  labels:
    volcano.sh/queue-name: default
  name: job-normal-00000001
  namespace: default
spec:
  containers:
    - image: nginx
      name: job-normal-00000001
  priorityClassName: normal
  terminationGracePeriodSeconds: 30
  schedulerName: volcano
status: {}
`
)

func newFakeDynamicClient(server *httptest.Server) *k8s.DynamicClientOption {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})
	return &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo:     &schema.Cluster{Name: "test-cluster"},
	}
}

// init trace logger
func initTestTraceLogger() error {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	conf := trace_logger.TraceLoggerConfig{
		Dir:             tmpDir,
		FilePrefix:      "trace_logger",
		Level:           "debug",
		MaxKeepDays:     2,
		MaxFileNum:      10,
		MaxFileSizeInMB: 1,
		IsCompress:      false,
		Timeout:         "2s",
		MaxCacheSize:    2,
	}

	return trace_logger.InitTraceLoggerManager(conf)
}

func TestKubeRuntimeJob(t *testing.T) {
	if err := initTestTraceLogger(); !assert.Equal(t, nil, err) {
		return
	}
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	client := fakedclient.NewSimpleClientset()
	kubeRuntime := &KubeRuntime{
		clientset:        client,
		dynamicClientOpt: dynamicClient,
		cluster:          dynamicClient.ClusterInfo,
	}

	pfJob := &api.PFJob{
		ID:                testJobID,
		Namespace:         "default",
		JobType:           schema.TypeSingle,
		JobMode:           schema.EnvJobModePod,
		ExtensionTemplate: []byte(jobManifest),
		Conf: schema.Conf{
			Env: map[string]string{
				schema.EnvJobQueueName: "default",
				schema.EnvJobFlavour:   "flavour1",
			},
			Flavour: schema.Flavour{
				ResourceInfo: schema.ResourceInfo{
					CPU: "1",
					Mem: "1Gi",
				},
			},
		},
	}
	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	err := storage.Job.CreateJob(&model.Job{
		ID: testJobID,
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
	})
	assert.Equal(t, nil, err)
	stopCh := make(chan struct{})
	defer close(stopCh)
	// create kubernetes job
	err = kubeRuntime.SubmitJob(pfJob)
	assert.Equal(t, nil, err)
	// stop kubernetes job
	err = kubeRuntime.StopJob(pfJob)
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeVCQueue(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:          dynamicClient.ClusterInfo,
		dynamicClientOpt: dynamicClient,
	}

	q := &model.Queue{
		Model: model.Model{
			ID: "test_queue_id",
		},
		Name:      "test_queue_name",
		Namespace: "default",
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: &resources.Resource{
			Resources: map[string]resources.Quantity{
				"cpu": 20 * 1000,
				"mem": 20 * 1024 * 1024 * 1024,
			},
		},
	}
	// create vc queue
	err := kubeRuntime.CreateQueue(q)
	assert.Equal(t, nil, err)
	// close vc queue
	err = kubeRuntime.CloseQueue(q)
	assert.Equal(t, nil, err)
	// delete vc queue
	err = kubeRuntime.DeleteQueue(q)
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeElasticQuota(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:          dynamicClient.ClusterInfo,
		dynamicClientOpt: dynamicClient,
	}

	q := &model.Queue{
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
	// create elastic quota
	err := kubeRuntime.CreateQueue(q)
	assert.Equal(t, nil, err)
	// close elastic quota
	err = kubeRuntime.CloseQueue(q)
	assert.Equal(t, nil, err)
	// delete elastic quota
	err = kubeRuntime.DeleteQueue(q)
	assert.Equal(t, nil, err)
}

func TestKubeRuntimePVAndPVC(t *testing.T) {
	client := fakedclient.NewSimpleClientset()
	kubeRuntime := &KubeRuntime{
		clientset: client,
		cluster:   &schema.Cluster{Name: "test-cluster", ID: "clustermock"},
	}
	driver.InitMockDB()

	config.DefaultPV = &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
		Spec: apiv1.PersistentVolumeSpec{
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"pfs.fs.id":  "$(pfs.fs.id)",
						"pfs.server": "$(pfs.server)",
					},
					VolumeHandle: "pfs-$(pfs.fs.id)-$(namespace)-pv",
				},
			},
		},
	}
	config.DefaultPVC = &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pfs-$(pfs.fs.id)-pvc",
			Namespace: "$(namespace)",
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			VolumeName: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
	}
	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			Host: "paddleflow-server",
			Port: 8999,
		},
	}

	namespace := "default"
	fsID := "fs-test"
	fs := model.FileSystem{
		Model: model.Model{
			ID: fsID,
		},
		Type:    "s3",
		SubPath: "elsie",
	}
	err := storage.Filesystem.CreatFileSystem(&fs)
	assert.Nil(t, err)
	fsCache := model.FSCacheConfig{
		FsID:       fsID,
		CacheDir:   "/data/paddleflow-fs/mnt",
		MetaDriver: "nutsdb",
	}
	err = storage.Filesystem.CreateFSCacheConfig(&fsCache)
	assert.Nil(t, err)

	pvc := fmt.Sprintf("pfs-%s-pvc", fsID)
	// create pv
	pv, err := kubeRuntime.CreatePV(namespace, fsID)
	assert.Equal(t, nil, err)
	// create pvc
	err = kubeRuntime.CreatePVC(namespace, fsID, pv)
	assert.Equal(t, nil, err)
	// delete pvc
	err = kubeRuntime.DeletePersistentVolumeClaim(namespace, pvc, metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
	// delete pv
	err = kubeRuntime.DeletePersistentVolume(pv, metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeNodeResource(t *testing.T) {
	client := fakedclient.NewSimpleClientset()
	kubeRuntime := &KubeRuntime{
		clientset: client,
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{},
	}
	namespace := "default"
	nodeName := "node1"
	node := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: apiv1.NodeStatus{
			Capacity: apiv1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: apiv1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: namespace,
		},
		Spec: apiv1.PodSpec{
			NodeName: nodeName,
			Containers: []apiv1.Container{
				{
					Name: "c1",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							"cpu":    resource.MustParse("2"),
							"memory": resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	// create node
	_, err := kubeRuntime.clientset.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	_, err = kubeRuntime.clientset.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// list node quota
	quotaSummary, nodeQuotaInfos, err := kubeRuntime.ListNodeQuota()
	assert.Equal(t, nil, err)
	t.Logf("quota summary: %v", quotaSummary)
	t.Logf("node  quota info: %v", nodeQuotaInfos)
}

func TestKubeRuntimeObjectOperation(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:          dynamicClient.ClusterInfo,
		dynamicClientOpt: dynamicClient,
	}

	gvk := k8sschema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

	namespace := "default"
	name := "cm1"
	cm := &apiv1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			"test.conf": "a:b1\nkey2:value2",
		},
	}

	cmObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cm)
	assert.Equal(t, nil, err)
	err = kubeRuntime.CreateObject(&unstructured.Unstructured{Object: cmObj})
	assert.Equal(t, nil, err)

	// update ConfigMap
	cm.Data["test2.conf"] = "a2:b2\nkey2:value2"
	cmObj, err = runtime.DefaultUnstructuredConverter.ToUnstructured(cm)
	assert.Equal(t, nil, err)
	err = kubeRuntime.UpdateObject(&unstructured.Unstructured{Object: cmObj})
	assert.Equal(t, nil, err)

	// get ConfigMap
	obj, err := kubeRuntime.GetObject(namespace, name, gvk)
	assert.Equal(t, nil, err)
	t.Logf("get object: %v", obj)

}
