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
	"fmt"
	"net/http"
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

	"paddleflow/pkg/common/k8s"
)

var DiscoveryHandlerFunc = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	var obj interface{}
	switch req.URL.Path {
	case "/apis/batch.volcano.sh/v1alpha1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.volcano.sh/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "jobs", Namespaced: true, Kind: "Job"},
			},
		}
	case "/apis/scheduling.volcano.sh/v1beta1":
		obj = &metav1.APIResourceList{
			GroupVersion: "scheduling.volcano.sh/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "queues", Namespaced: false, Kind: "Queue"},
			},
		}
	case "/apis/batch.paddlepaddle.org/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.paddlepaddle.org/v1",
			APIResources: []metav1.APIResource{
				{Name: "paddlejobs", Namespaced: true, Kind: "PaddleJob"},
			},
		}
	case "/apis":
		obj = &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "batch.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.volcano.sh/v1alpha1", Version: "v1alpha1"},
					},
				},
				{
					Name: "scheduling.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "scheduling.volcano.sh/v1beta1", Version: "v1beta1"},
					},
				},
				{
					Name: "batch.paddlepaddle.org",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.paddlepaddle.org/v1", Version: "v1"},
					},
				},
			},
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	output, err := json.Marshal(obj)
	if err != nil {
		fmt.Printf("unexpected encoding error: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(output)
})

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
	var server = httptest.NewServer(DiscoveryHandlerFunc)
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
		},
	}
	// create kubernetes resource with dynamic client
	err := Create(obj, gvk, dynamicClient)
	assert.Equal(t, nil, err)
	// get kubernetes resource with dynamic client
	_, err = Get(namespace, name, gvk, dynamicClient)
	assert.Equal(t, nil, err)
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
