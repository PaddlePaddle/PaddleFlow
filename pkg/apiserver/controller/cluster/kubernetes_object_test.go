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
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestCreateOrDeleteClusterObject(t *testing.T) {
	testClusterName := "test-cluster"
	configMapGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

	testCases := []struct {
		name    string
		request ObjectRequest
		action  string
		err     error
	}{
		{
			name: "create cluster configmap",
			request: ObjectRequest{
				ClusterName: testClusterName,
				Object:      map[string]interface{}{},
				Name:        "test-cm",
				Namespace:   "default",
			},
			action: util.QueryActionCreate,
			err:    nil,
		},
		{
			name: "delete cluster object",
			request: ObjectRequest{
				ClusterName:      testClusterName,
				GroupVersionKind: configMapGVK,
				Name:             "test-cm",
				Namespace:        "default",
			},
			action: util.QueryActionDelete,
			err:    nil,
		},
	}

	driver.InitMockDB()
	err := storage.Cluster.CreateCluster(&model.ClusterInfo{
		Name:        testClusterName,
		ClusterType: pfschema.KubernetesType,
	})
	assert.Equal(t, nil, err)
	// patch runtime
	rts := &runtime.KubeRuntime{}
	var p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p1.Reset()

	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "CreateObject", func(*unstructured.Unstructured) error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "DeleteObject", func(string, string, schema.GroupVersionKind) error {
		return nil
	})
	defer p3.Reset()

	ctx := &logger.RequestContext{UserName: MockRootUser}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := CreateOrDeleteClusterObject(ctx, testCase.request, testCase.action)
			assert.Equal(t, testCase.err, err)
		})
	}
}
