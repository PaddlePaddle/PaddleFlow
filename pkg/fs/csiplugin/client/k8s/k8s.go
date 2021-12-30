/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package k8s

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"paddleflow/pkg/fs/utils/common"
)

var k8sClient *K8SClient

type K8SInterface interface {
	ProxyGetPods(nodeID string) (result *v1.PodList, err error)
	GetPersistentVolumeClaim(namespace, name string,
		getOptions metav1.GetOptions) (*v1.PersistentVolumeClaim, error)
	ListPersistentVolume(listOptions metav1.ListOptions) (*v1.PersistentVolumeList, error)
}

type K8SClient struct {
	Clientset kubernetes.Interface
	Config    *rest.Config
}

func GetK8sClient() (K8SInterface, error) {
	if k8sClient == nil {
		var err error
		if k8sClient, err = New(common.GetK8SConfigPathEnv(), common.GetK8STimeoutEnv()); err != nil {
			log.Errorf("init k8sClient failed: %v", err)
			return nil, err
		}
	}
	return k8sClient, nil
}

func New(k8sConfigPath string, k8sClientTimeout int) (*K8SClient, error) {
	config, err := clientcmd.BuildConfigFromFlags("", k8sConfigPath)
	if err != nil {
		return nil, err
	}
	if k8sClientTimeout > 0 {
		config.Timeout = time.Second * time.Duration(k8sClientTimeout)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &K8SClient{
		Clientset: clientset,
		Config:    config,
	}, nil
}

// ProxyGetPods returns all pods on the node with nodeName, and the api server will forward this request to
// the kubelet proxy on the node. Therefore， the pods information comes from kubelet cache, not etcd
func (c *K8SClient) ProxyGetPods(nodeID string) (result *v1.PodList, err error) {
	result = &v1.PodList{}
	err = c.Clientset.CoreV1().RESTClient().Get().Resource("nodes").
		Name(nodeID).Suffix("/proxy/pods").Do(context.TODO()).Into(result)
	return
}

func (c *K8SClient) GetPersistentVolumeClaim(namespace, name string,
	getOptions metav1.GetOptions) (*v1.PersistentVolumeClaim, error) {
	return c.Clientset.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}

func (c *K8SClient) ListPersistentVolume(listOptions metav1.ListOptions) (*v1.PersistentVolumeList, error) {
	return c.Clientset.CoreV1().PersistentVolumes().List(context.TODO(), listOptions)
}
