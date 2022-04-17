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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database/dbinit"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/job/api"
)

const (
	testJobID     = "test_pf_id"
	vcjobManifest = `
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: vcJobName
spec:
  minAvailable: 1
  schedulerName: volcano
  priorityClassName: normal
  maxRetry: 5
  queue: default
  tasks:
  - replicas: 1
    name: "task"
    template:
      metadata:
        name: pod
      spec:
        containers:
        -  image: nginx
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
	}
}

func TestKubeRuntimeJob(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	client := fakedclient.NewSimpleClientset()
	kubeRuntime := &KubeRuntime{
		clientset:        client,
		dynamicClientOpt: dynamicClient,
	}
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "volcano"
	pfJob := &api.PFJob{
		ID:             testJobID,
		Namespace:      "default",
		JobType:        schema.TypeVcJob,
		JobMode:        schema.EnvJobModePod,
		ExtRuntimeConf: []byte(vcjobManifest),
		Conf: schema.Conf{
			Env: map[string]string{
				schema.EnvJobQueueName: "default",
				schema.EnvJobFlavour:   "default",
			},
			Flavour: schema.Flavour{
				Name: schema.CustomFlavour,
				ResourceInfo: schema.ResourceInfo{
					CPU: "1",
					Mem: "1",
				},
			},
		},
	}
	dbinit.InitMockDB()

	err := models.CreateJob(&models.Job{
		ID: testJobID,
		Config: schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
			Flavour: schema.Flavour{
				Name: schema.CustomFlavour,
				ResourceInfo: schema.ResourceInfo{
					CPU: "1",
					Mem: "1",
				},
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
		dynamicClientOpt: dynamicClient,
	}

	q := &models.Queue{
		Model: models.Model{
			ID: "test_queue_id",
		},
		Name:      "test_queue_name",
		Namespace: "default",
		QuotaType: schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "20",
			Mem: "20Gi",
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
		dynamicClientOpt: dynamicClient,
	}

	q := &models.Queue{
		Model: models.Model{
			ID: "test_queue_id",
		},
		Name:      "test_queue_name",
		Namespace: "default",
		QuotaType: schema.TypeElasticQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "20",
			Mem: "20Gi",
		},
		MinResources: schema.ResourceInfo{
			CPU: "10",
			Mem: "10Gi",
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
	}

	config.DefaultPV = &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
		Spec: apiv1.PersistentVolumeSpec{
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"pfs.fs.id":     "$(pfs.fs.id)",
						"pfs.user.name": "$(pfs.user.name)",
						"pfs.server":    "$(pfs.server)",
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
		Fs: config.FsServerConf{
			K8sServiceName: "paddleflow",
			K8sServicePort: 8083,
		},
	}

	namespace := "default"
	fsID := "fs-test"
	userName := "test"
	pvc := fmt.Sprintf("pfs-%s-pvc", fsID)
	// create pv
	pv, err := kubeRuntime.CreatePV(namespace, fsID, userName)
	assert.Equal(t, nil, err)
	// create pvc
	err = kubeRuntime.CreatePVC(namespace, fsID, pv)
	assert.Equal(t, nil, err)
	// delete pvc
	err = kubeRuntime.deletePersistentVolumeClaim(namespace, pvc, &metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
	// delete pv
	err = kubeRuntime.deletePersistentVolume(pv, &metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeNodeResource(t *testing.T) {
	client := fakedclient.NewSimpleClientset()
	kubeRuntime := &KubeRuntime{
		clientset: client,
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			ScalarResourceArray: []string{
				"",
			},
		},
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
