package log

import (
	"net/http/httptest"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	mockRootUser    = "root"
	mockPodName     = "jobName"
	MockClusterName = "testCn"
	MockNamespace   = "paddle"
)

var clusterInfo = model.ClusterInfo{
	Name:          MockClusterName,
	Description:   "Description",
	Endpoint:      "Endpoint",
	Source:        "Source",
	ClusterType:   pfschema.KubernetesType,
	Version:       "1.16",
	Status:        model.ClusterStatusOnLine,
	Credential:    "credential",
	Setting:       "Setting",
	NamespaceList: []string{"default", "n2", MockNamespace},
}

func TestGetPFJobLogs(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	krc := client.NewFakeKubeRuntimeClient(server)
	clientset := fakedclient.NewSimpleClientset()

	//CreateRuntime
	e1 := &runtime.KubeRuntime{}
	patch4 := gomonkey.ApplyPrivateMethod(e1, "BuildConfig", func() (*rest.Config, error) {
		return krc.Config, nil
	})
	defer patch4.Reset()

	patch2 := gomonkey.ApplyPrivateMethod(e1, "clientset", func() kubernetes.Interface {
		return clientset
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyFunc(client.CreateKubeRuntimeClient, func(_ *rest.Config, _ *pfschema.Cluster) (framework.RuntimeClientInterface, error) {
		return krc, nil
	})
	defer patch3.Reset()

	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true

	// init pod
	objectMeta := metav1.ObjectMeta{
		Name:      "pod",
		Namespace: "default",
		Labels:    map[string]string{},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: "batch.paddlepaddle.org/v1",
				Kind:       "PaddleJob",
				Name:       mockPodName,
			},
		},
	}
	pod := v1.Pod{
		ObjectMeta: objectMeta,
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "c1",
					Image: "busybox:v1",
				},
			},
		},
	}

	gvk := k8s.PodGVK
	frameworkVersion := pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	err := krc.Create(&pod, frameworkVersion)
	assert.NoError(t, err)
	findPod, err := krc.Get(pod.Namespace, pod.Name, frameworkVersion)
	assert.NoError(t, err)
	t.Logf("findPod=%v", findPod)

	type args struct {
		ctx *logger.RequestContext
		req GetMixedLogRequest
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "job framework unsupported",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:      "",
					Framework: "",
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "job",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:      "",
					Framework: string(pfschema.FrameworkSpark),
				},
			},
			wantErr:      false,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := GetPFJobLogs(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetKubernetesResourceLogs(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	krc := client.NewFakeKubeRuntimeClient(server)
	clientset := fakedclient.NewSimpleClientset()

	//CreateRuntime
	e1 := &runtime.KubeRuntime{}

	patch4 := gomonkey.ApplyPrivateMethod(e1, "BuildConfig", func() (*rest.Config, error) {
		return krc.Config, nil
	})
	defer patch4.Reset()

	patch2 := gomonkey.ApplyPrivateMethod(e1, "clientset", func() kubernetes.Interface {
		return clientset
	})
	defer patch2.Reset()

	patch3 := gomonkey.ApplyPrivateMethod(e1, "clientset", func() kubernetes.Interface {
		return clientset
	})
	defer patch3.Reset()

	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.IsSingleCluster = true

	// init pod
	objectMeta := metav1.ObjectMeta{
		Name:      "pod",
		Namespace: "default",
		Labels:    map[string]string{},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: "batch.paddlepaddle.org/v1",
				Kind:       "PaddleJob",
				Name:       mockPodName,
			},
		},
	}
	pod := v1.Pod{
		ObjectMeta: objectMeta,
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "c1",
					Image: "busybox:v1",
				},
			},
		},
	}

	gvk := k8s.PodGVK
	frameworkVersion := pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	err := krc.Create(&pod, frameworkVersion)
	assert.NoError(t, err)
	findPod, err := krc.Get(pod.Namespace, pod.Name, frameworkVersion)
	assert.NoError(t, err)
	t.Logf("findPod=%v", findPod)

	// init cluster
	assert.Nil(t, storage.Cluster.CreateCluster(&clusterInfo))

	type args struct {
		ctx          *logger.RequestContext
		req          GetMixedLogRequest
		clientEnable bool
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request, cluster type[] is not support",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name: "",
				},
				clientEnable: true,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "KubernetesType, but unknown jobtype",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:        "",
					ClusterName: MockClusterName,
				},
				clientEnable: true,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "TypePodJob, KubernetesType, but pods not found",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:         "",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
				},
				clientEnable: true,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "TypePodJob, KubernetesType ",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:         mockPodName,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
				},
				clientEnable: true,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "init client failed",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:         mockPodName,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
				},
				clientEnable: false,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "unknown job type",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name:         mockPodName,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: "wrong",
				},
				clientEnable: false,
			},
			wantErr:      true,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.clientEnable {
				patch3 := gomonkey.ApplyFunc(client.CreateKubeRuntimeClient, func(_ *rest.Config, _ *pfschema.Cluster) (framework.RuntimeClientInterface, error) {
					return krc, nil
				})
				defer patch3.Reset()
			}

			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := GetLogs(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
