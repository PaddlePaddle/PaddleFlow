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

package runtime_v2

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

func initK3STestTraceLogger() error {
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

func TestK3SRuntimeJob(t *testing.T) {
	if err := initK3STestTraceLogger(); !assert.Equal(t, nil, err) {
		return
	}
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeK3SRuntimeClient(server)
	kubeRuntime := &K3SRuntimeService{
		cluster: &schema.Cluster{Name: "default-cluster", Type: schema.K3SType},
		client:  kubeClient,
	}

	pfJob := &api.PFJob{
		ID:                testJobID,
		Namespace:         "default",
		JobType:           schema.TypeSingle,
		Framework:         schema.FrameworkStandalone,
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
		Tasks: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "busybox:v1",
					Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
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
	assert.NoError(t, err)
	stopCh := make(chan struct{})
	defer close(stopCh)
	fwVersion := client.KubeFrameworkVersion(k8s.PodGVK)
	// create kubernetes job
	err = kubeRuntime.Job(fwVersion).Submit(context.TODO(), pfJob)
	assert.NoError(t, err)
	// update job
	err = kubeRuntime.UpdateJob(pfJob)
	assert.NoError(t, err)
	// stop kubernetes job
	err = kubeRuntime.StopJob(pfJob)
	assert.NoError(t, err)
	// update job
	err = kubeRuntime.DeleteJob(pfJob)
	assert.Error(t, err)
	t.SkipNow()
}

func TestK3SRuntime_Init(t *testing.T) {
	k3src := K3SRuntimeService{}
	err := k3src.Init()
	// build k3s runtime err
	assert.NotNil(t, err)
	// set cluster
	cluster := &schema.Cluster{
		Name: "default-cluster",
		ID:   uuid.GenerateID("cluster"),
		Type: schema.K3SType}
	NewK3SRuntime(*cluster)

	k3src.cluster = cluster
	err = k3src.Init()
	assert.NotNil(t, err)
	// use service account,but service account not setting
	k3src.cluster = nil
	err = k3src.Init()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined")
	// user service account, set os env, but no token setting
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	urlArr := strings.Split(server.URL, ":")

	os.Setenv("KUBERNETES_SERVICE_HOST", urlArr[0])
	os.Setenv("KUBERNETES_SERVICE_PORT", urlArr[1])
	err = k3src.Init()
	assert.Contains(t, err.Error(), "open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory")
}

func TestK3SRuntimeNodeResource(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeK3SRuntimeClient(server)
	kubeRuntime := &K3SRuntimeService{
		cluster: &schema.Cluster{Name: "test-cluster", Type: schema.K3SType},
		client:  kubeClient,
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{},
	}
	namespace := "default"
	nodeName := "node1"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Name: "c1",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"cpu":    resource.MustParse("2"),
							"memory": resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	// create node
	_, err := kubeRuntime.clientSet().CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	_, err = kubeRuntime.clientSet().CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// list node quota
	quotaSummary, nodeQuotaInfos, err := kubeRuntime.ListNodeQuota()
	assert.Equal(t, nil, err)
	t.Logf("quota summary: %v", quotaSummary)
	t.Logf("node  quota info: %v", nodeQuotaInfos)
}

func TestK3SRuntime_SyncController(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeK3SRuntimeClient(server)
	kubeRuntime := &K3SRuntimeService{
		cluster: &schema.Cluster{Name: "test-cluster", Type: schema.K3SType},
		client:  kubeClient,
	}
	ch := make(chan struct{})
	kubeRuntime.SyncController(ch)
}

func TestK3SRuntime_GetLog(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeK3SRuntimeClient(server)
	kubeRuntime := &K3SRuntimeService{
		cluster: &schema.Cluster{Name: "test-cluster", Type: schema.K3SType},
		client:  kubeClient,
	}
	mockK3SCreateLog(t, kubeRuntime)

	type args struct {
		req              schema.JobLogRequest
		mockListFailed   bool
		mockGetPodFailed bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name: "job id is not setting",
			args: args{
				req: schema.JobLogRequest{
					JobID: "",
				},
			},
			wantErr: true,
		},
		{
			name: "job type is not support",
			args: args{
				req: schema.JobLogRequest{
					JobID:   mockPodName,
					JobType: "",
				},
			},
			wantErr: true,
		},
		{
			name: "job info is validate",
			args: args{
				req: schema.JobLogRequest{
					JobID:   mockPodName,
					JobType: string(schema.TypeSingle),
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			if tt.args.mockListFailed || tt.args.mockGetPodFailed {
				patch := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientSet().CoreV1().Pods("")), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
					return nil, fmt.Errorf("err")
				})
				defer patch.Reset()
				patch2 := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientSet().CoreV1().Pods("")), "List", func(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					return nil, fmt.Errorf("err")
				})
				defer patch2.Reset()

				res, err := kubeRuntime.GetLog(tt.args.req, schema.MixedLogRequest{})
				t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
				if tt.wantErr {
					assert.Error(t, err)
					if err != nil {
						t.Logf("wantError: %s", err.Error())
					}
				} else {
					assert.NoError(t, err)
					t.Logf("name=%s, res=%#v", tt.name, res)
				}
				t.SkipNow()
			} else {
				res, err := kubeRuntime.GetLog(tt.args.req, schema.MixedLogRequest{})
				t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
				if tt.wantErr {
					assert.Error(t, err)
					if err != nil {
						t.Logf("wantError: %s", err.Error())
					}
				} else {
					assert.NoError(t, err)
					t.Logf("name=%s, res=%#v", tt.name, res)
				}
			}

		})
	}

}

func createMockNode(client kubernetes.Interface) error {
	nodeName := "mockNodeName"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	// create node
	_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	return err
}

func TestKubeRuntime_SubmitJob(t *testing.T) {
	err := createSingleJobTemplate()
	assert.Nil(t, err)
	currentConfigDir, _ := os.Getwd()
	filePath := currentConfigDir + "/job_template.yaml"
	err = config.InitJobTemplate(filePath)
	assert.Nil(t, err)
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeK3SRuntimeClient(server)
	k3srs := &K3SRuntimeService{
		cluster: &schema.Cluster{Name: "test-cluster", Type: schema.K3SType},
		client:  kubeClient,
	}
	// create mock node
	err = createMockNode(k3srs.clientSet())
	assert.Nil(t, err)
	// submit empty job
	err = k3srs.SubmitJob(nil)
	assert.Contains(t, err.Error(), "submit job failed, job is nil")
	// FsNodeAffinity not support, schedule will be panic when add fs in pod info
	pfJob := api.PFJob{}
	// fs := schema.FileSystem{
	// 	Type:      schema.PFSTypeLocal,
	// 	Name:      "fsname",
	// 	ID:        "fsID",
	// 	HostPath:  "/tmp",
	// 	MountPath: "/mnt/test",
	// }
	// extfs := []schema.FileSystem{
	// 	{
	// 		Type: "HDFS",
	// 	},
	// 	{
	// 		Type:      schema.PFSTypeLocal,
	// 		Name:      "fsname",
	// 		ID:        "fsID",
	// 		HostPath:  "/tmp",
	// 		MountPath: "/mnt/test",
	// 	},
	// 	{
	// 		Name:      "fsname2",
	// 		ID:        "fsID2",
	// 		HostPath:  "/tmp2",
	// 		MountPath: "/mnt/test2",
	// 	},
	// }
	pfJob.JobType = schema.TypeSingle
	// submit valid pf job
	err = k3srs.SubmitJob(&pfJob)
	assert.Contains(t, err.Error(), "job member is nil")
	// create mock task
	mem := []schema.Member{
		{
			ID: "mockTaskID",
		},
	}
	pfJob.Tasks = mem
	sc := &config.ServerConfig{}
	config.GlobalServerConfig = sc
	config.GlobalServerConfig.Job.SchedulerName = "sss"
	err = k3srs.SubmitJob(&pfJob)
	assert.Nil(t, err)
	os.RemoveAll(filePath)
}

func createSingleJobTemplate() error {
	content := `apiVersion: v1
kind: Pod
metadata:
    name: default-name
    namespace: default
spec:
    containers:
        - image: nginx
          imagePullPolicy: IfNotPresent
          name: job-default-name
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
    dnsPolicy: ClusterFirst
    enableServiceLinks: true
    priorityClassName: normal
    restartPolicy: Never
    schedulerName: volcano
    securityContext: {}
    serviceAccount: default
    serviceAccountName: default
    terminationGracePeriodSeconds: 30
# single-job
---`
	err := os.WriteFile("./job_template.yaml", []byte(content), 0666)
	return err
}

func mockK3SCreateLog(t *testing.T, kr *K3SRuntimeService) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	podSpec := corev1.PodSpec{
		NodeName: nodeName,
		Containers: []corev1.Container{
			{
				Name: "c1",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2Gi"),
					},
				},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mockPodName,
			Namespace: mockNS,
			Labels: map[string]string{
				"app":             mockDeployName,
				schema.JobIDLabel: mockPodName,
			},
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	// create node
	_, err := kr.clientSet().CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	for i := 0; i < 10; i++ {
		t.Logf("create pod %s", pod.Name)
		_, err = kr.clientSet().CoreV1().Pods(mockNS).Create(context.TODO(), pod, metav1.CreateOptions{})
		assert.NoError(t, err)
		pod.Name = uuid.GenerateIDWithLength("pod", 5)
	}
	// create random events
	mockCreateK3SEvents(t, kr, mockPodName, mockNS)
}

func mockCreateK3SEvents(t *testing.T, kr *K3SRuntimeService, objectName, namespace string) {
	for i := 0; i < 10; i++ {
		mockEventName := uuid.GenerateIDWithLength("randomName", 5)
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      mockEventName,
				Namespace: namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				Name:      objectName,
				Namespace: namespace,
			},
			Reason:         "start",
			Message:        "end msg",
			FirstTimestamp: metav1.NewTime(time.Now()),
			LastTimestamp:  metav1.NewTime(time.Now()),
		}
		_, err := kr.clientSet().CoreV1().Events(namespace).Create(context.TODO(), event, metav1.CreateOptions{})
		assert.Equal(t, nil, err)
	}
}

func TestK3SRuntimeService_QueueRelated(t *testing.T) {
	k3src := K3SRuntimeService{}
	k3src.Init()
	assert.Nil(t, k3src.Queue(schema.FrameworkVersion{}))
	assert.Nil(t, k3src.CreateQueue(&api.QueueInfo{}))
	assert.Nil(t, k3src.DeleteQueue(&api.QueueInfo{}))
	assert.Nil(t, k3src.UpdateQueue(&api.QueueInfo{}))
}

func TestK3SNewK3SRuntime(t *testing.T) {
	type args struct {
		cluster schema.Cluster
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "init success",
			args: args{
				cluster: schema.Cluster{
					Type: schema.K3SType,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NewK3SRuntime(tt.args.cluster)
		})
	}
}

func TestK3SInit(t *testing.T) {
	type args struct {
		rs                           K3SRuntimeService
		passBuildConfig              bool
		passDecode                   bool
		passRESTConfigFromKubeConfig bool
		passBuildConfigFromFlags     bool
		clientInitErr                error
		configErr                    error
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "invalid configuration",
			args: args{
				rs: K3SRuntimeService{
					cluster: &schema.Cluster{
						Type: schema.K3SType,
					},
				},
				configErr: fmt.Errorf("invalid configuration"),
			},
		},
		{
			name: "cluster nil, no such file or directory",
			args: args{
				rs:              K3SRuntimeService{},
				passBuildConfig: true,
				clientInitErr:   fmt.Errorf("no such file or directory"),
			},
		},
		{
			name: "invalid leading UTF-8",
			args: args{
				rs: K3SRuntimeService{
					cluster: &schema.Cluster{
						Name: "mockname",
						Type: schema.K3SType,
						ClientOpt: schema.ClientOptions{
							Master: "127.0.0.1:6443",
							Config: "mock",
							QPS:    1000,
							Burst:  1000,
						},
					},
				},
				passDecode:    false,
				clientInitErr: fmt.Errorf("invalid leading UTF-8"),
			},
		},
		{
			name: "decode pass",
			args: args{
				rs: K3SRuntimeService{
					cluster: &schema.Cluster{
						Name: "mockname",
						Type: schema.K3SType,
						ClientOpt: schema.ClientOptions{
							Master: "127.0.0.1:6443",
							Config: "mock",
							QPS:    1000,
							Burst:  1000,
						},
					},
				},
				passDecode:    true,
				clientInitErr: fmt.Errorf("couldn't get version"),
			},
		},
		{
			name: "build config success",
			args: args{
				rs: K3SRuntimeService{
					cluster: &schema.Cluster{
						Name: "mockname",
						Type: schema.K3SType,
						ClientOpt: schema.ClientOptions{
							Master: "127.0.0.1:6443",
							QPS:    1000,
							Burst:  1000,
						},
					},
				},
				passBuildConfigFromFlags: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.configErr == nil {
				var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
				defer server.Close()
				urlArr := strings.Split(server.URL, ":")
				os.Setenv("KUBERNETES_SERVICE_HOST", urlArr[0])
				os.Setenv("KUBERNETES_SERVICE_PORT", urlArr[1])
			}
			if tt.args.passBuildConfig {
				var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(&tt.args.rs), "buildConfig", func() (*rest.Config, error) {
					return &rest.Config{}, nil
				})
				defer p2.Reset()
			}
			//base64.StdEncoding.DecodeString(k3srs.cluster.ClientOpt.Config)
			if tt.args.passDecode {
				stde := base64.StdEncoding
				var p3 = gomonkey.ApplyMethodFunc(reflect.TypeOf(stde), "DecodeString", func(s string) ([]byte, error) {
					return []byte("mock"), nil
				})
				defer p3.Reset()
			}
			if tt.args.passBuildConfigFromFlags {
				clientcmd.BuildConfigFromFlags("", "")
				var p4 = gomonkey.ApplyFunc(clientcmd.BuildConfigFromFlags,
					func(_, _ string) (*rest.Config, error) {
						return &rest.Config{}, nil
					})
				defer p4.Reset()
			}
			err := tt.args.rs.Init()
			if tt.args.configErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.args.configErr.Error())
			}
			if tt.args.clientInitErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.args.clientInitErr.Error())
			}
		})
	}
}
