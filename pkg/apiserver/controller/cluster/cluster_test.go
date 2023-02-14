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

package cluster

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser    = "root"
	MockNonRootUser = "notroot"
	MockClusterName = "testCn"
)

func TestCreateCluster(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	tests := []struct {
		name             string
		createClusterReq CreateClusterRequest
		mockInitClient   bool
		expectedErr      error
	}{
		{
			name: "ClusterName regex",
			createClusterReq: CreateClusterRequest{
				Name: "",
				ClusterCommonInfo: ClusterCommonInfo{
					ID:            "error_init",
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        "wrong",
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			expectedErr: fmt.Errorf("does not compile with regex rule"),
		},
		{
			name: "clusterStatus can only be 'online' or 'offline'",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					ID:            "error_init",
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        "wrong",
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			expectedErr: fmt.Errorf("clusterStatus can only be 'online' or 'offline'"),
		},
		{
			name: "clustertype",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					ID:            "error_init",
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   "wrong",
				},
			},
			expectedErr: fmt.Errorf("ClusterType [wrong] is invalid"),
		},
		{
			name: "clustertype should not be empty",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					ID:            "error_init",
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   "",
				},
			},
			expectedErr: fmt.Errorf("ClusterType should not be empty"),
		},
		{
			name: "Endpoint should not be empty",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					Endpoint:      "",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
				},
			},
			expectedErr: fmt.Errorf("Endpoint should not be empty"),
		},
		{
			name: "version should not be empty",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			expectedErr: fmt.Errorf("version of cluster"),
		},
		{
			name: "wrong case without client",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					ID:            "error_init",
					Endpoint:      "http://1.1.1.1:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			expectedErr: fmt.Errorf("invalid configuration: no configuration has been provided, try setting KUBERNETES_MASTER environment variable"),
		},
		{
			name: "ns list case",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					Endpoint:      "http://10.204.9.128:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n-1", "n_2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			mockInitClient: true,
			expectedErr:    fmt.Errorf("namespace[n_2] is invalid"),
		},
		{
			name: "Success case",
			createClusterReq: CreateClusterRequest{
				Name: MockClusterName,
				ClusterCommonInfo: ClusterCommonInfo{
					Endpoint:      "http://10.204.9.128:8086",
					Source:        "build",
					Status:        model.ClusterStatusOnLine,
					Version:       "1.15",
					NamespaceList: []string{"n1", "n2"},
					ClusterType:   schema.KubernetesType,
				},
			},
			mockInitClient: true,
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.createClusterReq, tt.expectedErr)
			if tt.mockInitClient {
				rts := &runtime.KubeRuntime{}
				var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
					return nil
				})
				defer p2.Reset()
				resp, err := CreateCluster(ctx, &tt.createClusterReq)
				if tt.expectedErr != nil && assert.Error(t, err) {
					assert.ErrorContains(t, err, tt.expectedErr.Error())
				}
				if err == nil {
					t.Logf("resp= %v", resp)
				}
			} else {
				resp, err := CreateCluster(ctx, &tt.createClusterReq)
				if tt.expectedErr != nil && assert.Error(t, err) {
					assert.ErrorContains(t, err, tt.expectedErr.Error())
				}
				if err == nil {
					t.Logf("resp= %v", resp)
				}
			}
		})
	}
}

func TestGetCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// test get clusterInfo
	resp, err := GetCluster(ctx, MockClusterName)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestUpdateCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	updateClusterRequest := UpdateClusterRequest{
		ClusterCommonInfo: ClusterCommonInfo{
			ClusterType: schema.LocalType,
		},
	}

	// test update clusterInfo
	resp, err := UpdateCluster(ctx, MockClusterName, &updateClusterRequest)
	assert.Nil(t, err)
	// expect status changes from online to offline
	assert.Equal(t, MockClusterName, resp.Name)
	t.Logf("resp=%v", resp)
}

func TestListCluster(t *testing.T) {
	TestCreateCluster(t)

	ctx := &logger.RequestContext{UserName: MockRootUser}
	ctxNoRoot := &logger.RequestContext{UserName: MockNonRootUser}

	type args struct {
		ctx             *logger.RequestContext
		marker          string
		maxKeys         int
		clusterNameList []string
		clusterStatus   string
	}
	tests := []struct {
		name           string
		args           args
		mockInitClient bool
		expectedErr    error
	}{
		{
			name: "true",
			args: args{
				ctx:             ctx,
				marker:          "",
				maxKeys:         0,
				clusterNameList: []string{MockClusterName, "fake"},
			},
			expectedErr: nil,
		},
		{
			name: "Non root user",
			args: args{
				ctx:             ctxNoRoot,
				marker:          "1",
				maxKeys:         2,
				clusterNameList: []string{},
			},
			expectedErr: errors.New("list cluster failed"),
		},
		{
			name: "wrong marker",
			args: args{
				ctx:             ctx,
				marker:          "1",
				maxKeys:         0,
				clusterNameList: []string{MockClusterName, "fake"},
			},
			expectedErr: errors.New("odd length hex string"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.expectedErr)
			resp, err := ListCluster(tt.args.ctx, tt.args.marker, tt.args.maxKeys, tt.args.clusterNameList, tt.args.clusterStatus)
			if tt.expectedErr != nil && assert.Error(t, err) {
				t.Logf("got error %v", err)
				assert.ErrorContains(t, err, tt.expectedErr.Error())
			}
			if err == nil {
				t.Logf("resp= %v", resp)
			}
		})
	}
}

func TestDeleteCluster(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}
	err := DeleteCluster(ctx, MockClusterName)
	assert.Nil(t, err)
}

func TestInitDefaultClusters(t *testing.T) {
	TestCreateCluster(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	type args struct {
		ctx              *logger.RequestContext
		createClusterReq *CreateClusterRequest
	}
	tests := []struct {
		name           string
		args           args
		mockInitClient bool
		expectedErr    error
	}{
		{
			name: "success",
			args: args{
				createClusterReq: &CreateClusterRequest{
					Name: "default-cluster",
					ClusterCommonInfo: ClusterCommonInfo{
						Endpoint:      "http://10.204.9.128:8086",
						Source:        "build",
						Status:        model.ClusterStatusOnLine,
						Version:       "1.15",
						NamespaceList: []string{"n1", "n2"},
						ClusterType:   schema.KubernetesType,
					},
				},
				ctx: ctx,
			},
			mockInitClient: true,
			expectedErr:    nil,
		},
		{
			name: "true",
			args: args{
				createClusterReq: nil,
				ctx:              ctx,
			},
			mockInitClient: false,
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.expectedErr)
			if tt.args.createClusterReq != nil {
				rts := &runtime.KubeRuntime{}
				var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
					return nil
				})
				defer p2.Reset()
				_, err := CreateCluster(tt.args.ctx, tt.args.createClusterReq)
				assert.NoError(t, err)
			}
			err := InitDefaultCluster()
			if tt.expectedErr != nil && assert.Error(t, err) {
				t.Logf("got error %v", err)
				assert.ErrorContains(t, err, tt.expectedErr.Error())
			} else {
				err = DeleteCluster(ctx, "default-cluster")
				assert.NoError(t, err)
			}
		})
	}
}

func TestListClusterQuota(t *testing.T) {
	TestCreateCluster(t)

	ctx := &logger.RequestContext{UserName: MockRootUser}
	ctxNoRoot := &logger.RequestContext{UserName: MockNonRootUser}

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
	patch3 := gomonkey.ApplyPrivateMethod(e1, "listNodes", func(listOptions metav1.ListOptions) (*corev1.NodeList, error) {
		return &corev1.NodeList{
			Items: []corev1.Node{},
		}, nil
	})
	defer patch3.Reset()

	type args struct {
		ctx             *logger.RequestContext
		clusterNameList []string
	}
	tests := []struct {
		name           string
		args           args
		mockInitClient bool
		expectedErr    error
	}{
		{
			name: "true",
			args: args{
				ctx:             ctx,
				clusterNameList: []string{MockClusterName, "fake"},
			},
			expectedErr: nil,
		},
		{
			name: "Non root user",
			args: args{
				ctx:             ctxNoRoot,
				clusterNameList: []string{},
			},
			expectedErr: errors.New("get cluster failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.expectedErr)
			resp, err := ListClusterQuota(tt.args.ctx, tt.args.clusterNameList)
			if tt.expectedErr != nil && assert.Error(t, err) {
				t.Logf("got error %v", err)
				assert.ErrorContains(t, err, tt.expectedErr.Error())
			}
			if err == nil {
				t.Logf("resp= %v", resp)
			}
		})
	}
}
