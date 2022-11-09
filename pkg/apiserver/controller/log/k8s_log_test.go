package log

import (
	kuberuntime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"net/http/httptest"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	mockRootUser = "root"
	mockPodName  = "jobName"
)

func newFakeDynamicClient(server *httptest.Server) *k8s.DynamicClientOption {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})
	return &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo:     &pfschema.Cluster{Name: "test-cluster"},
	}
}

func TestGetK8sLog(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	clientset := fakedclient.NewSimpleClientset()

	//CreateRuntime
	e1 := &kuberuntime.KubeRuntime{}
	//patch1 := gomonkey.ApplyMethodReturn(e1, "Init", nil)
	//defer patch1.Reset()
	patch4 := gomonkey.ApplyPrivateMethod(e1, "BuildConfig", func() (*rest.Config, error) {
		return dynamicClient.Config, nil
	})
	defer patch4.Reset()

	patch2 := gomonkey.ApplyPrivateMethod(e1, "clientset", func() kubernetes.Interface {
		return clientset
	})
	defer patch2.Reset()

	//func CreateKubeRuntimeClient(config *rest.Config, cluster *pfpfschema.Cluster) (framework.RuntimeClientInterface, error) {
	krc := client.KubeRuntimeClient{
		Client:           clientset,
		DynamicClient:    dynamicClient.DynamicClient,
		DynamicFactory:   dynamicClient.DynamicFactory,
		DiscoveryClient:  dynamicClient.DiscoveryClient,
		Config:           dynamicClient.Config,
		ClusterInfo:      dynamicClient.ClusterInfo,
		JobInformerMap:   make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		QueueInformerMap: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
	}
	patch3 := gomonkey.ApplyFunc(client.CreateKubeRuntimeClient, func(_ *rest.Config, _ *pfschema.Cluster) (framework.RuntimeClientInterface, error) {
		return &krc, nil
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
			name: "empty request, cluster type[] is not support",
			args: args{
				ctx: &logger.RequestContext{
					UserName: mockRootUser,
				},
				req: GetMixedLogRequest{
					Name: "",
				},
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
					Name: "",
					ClusterInfo: model.ClusterInfo{
						ClusterType: pfschema.KubernetesType,
					},
				},
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
					Name: "",
					ClusterInfo: model.ClusterInfo{
						ClusterType: pfschema.KubernetesType,
					},
					ResourceType: string(pfschema.TypePodJob),
				},
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
					Name:      mockPodName,
					Namespace: "default",
					ClusterInfo: model.ClusterInfo{
						ClusterType: pfschema.KubernetesType,
					},
					ResourceType: string(pfschema.TypePodJob),
				},
			},
			wantErr:      true,
			responseCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			res, err := GetK8sLog(tt.args.ctx, tt.args.req)
			t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
