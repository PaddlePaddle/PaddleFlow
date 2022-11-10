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

package client

import (
	"net/http/httptest"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func NewFakeKubeRuntimeClient(server *httptest.Server) *KubeRuntimeClient {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})
	kubeClient := fakedclient.NewSimpleClientset()

	return &KubeRuntimeClient{
		Client:          kubeClient,
		InformerFactory: informers.NewSharedInformerFactory(kubeClient, 0),
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo: &schema.Cluster{
			Name: "default-cluster",
			ID:   "cluster-123",
			Type: "Kubernetes",
		},
		Config: &rest.Config{Host: server.URL},
	}
}
