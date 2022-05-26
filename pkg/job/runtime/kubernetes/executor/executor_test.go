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

package executor

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
)

func newFakeDynamicClient(server *httptest.Server) *k8s.DynamicClientOption {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	opt := &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
	}
	return opt
}

func TestExecutor(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)

	// create namespaced kubernetes resource
	gvk := k8s.VCJobGVK
	name := "vcjob"
	namespace := "default"

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"status": make(map[string]interface{}),
		}}
	patchJSON, err := json.Marshal(struct {
		metav1.ObjectMeta
	}{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"label1": "value1",
			},
			Annotations: map[string]string{
				"anno1": "value1",
			},
		},
	})
	t.Logf("patch resource %s", string(patchJSON))
	assert.Equal(t, nil, err)
	// create kubernetes resource with dynamic client
	err = Create(obj, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	// patch kubernetes resource with dynamic client
	err = Patch(namespace, name, gvk, patchJSON, dynamicClient)
	assert.Equal(t, nil, err)
	// get kubernetes resource with dynamic client
	obj, err = Get(namespace, name, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	t.Logf("get patched resource %v", obj)
	// delete kubernetes resource with dynamic client
	err = Delete(namespace, name, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	// kubernetes resource is not found
	err = Delete(namespace, name, gvk, dynamicClient)
	assert.NotEqual(t, nil, err)

	// create non namespaced kubernetes resource
	gvk = k8s.VCQueueGVK
	obj = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
			"status": make(map[string]interface{}),
		},
	}
	// create kubernetes resource with dynamic client
	err = Create(obj, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	// get kubernetes resource with dynamic client
	_, err = Get(namespace, name, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	// delete kubernetes resource with dynamic client
	err = Delete(namespace, name, gvk, dynamicClient)
	assert.Equal(t, nil, err)
}
