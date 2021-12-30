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
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type K8sOperator interface {
	CreatePersistentVolume(pv *apiv1.PersistentVolume) (*apiv1.PersistentVolume, error)
	DeletePersistentVolume(name string, deleteOptions *metav1.DeleteOptions) error
	GetPersistentVolume(name string, getOptions metav1.GetOptions) (*apiv1.PersistentVolume, error)
	CreatePersistentVolumeClaim(namespace string, pvc *apiv1.PersistentVolumeClaim) (*apiv1.PersistentVolumeClaim, error)
	DeletePersistentVolumeClaim(namespace string, name string, deleteOptions *metav1.DeleteOptions) error
	GetPersistentVolumeClaim(namespace string, name string, getOptions metav1.GetOptions) (*apiv1.PersistentVolumeClaim, error)
	GetNamespace(namespace string, getOptions metav1.GetOptions) (*apiv1.Namespace, error)
	ListNamespaces(listOptions metav1.ListOptions) (*apiv1.NamespaceList, error)
}

var k8sInit sync.Once
var k8sOperator K8sOperator
var k8sClient *K8sClient

func GetK8sOperator() K8sOperator {
	return k8sOperator
}

type K8sClient struct {
	clientset kubernetes.Interface
	config    *rest.Config
}

func New(k8sConfigPath string, k8sClientQPS, k8sClientBurst, k8sClientTimeout int) error {
	var err error
	var config *rest.Config
	var clientset *kubernetes.Clientset
	k8sInit.Do(func() {
		config, err = clientcmd.BuildConfigFromFlags("", k8sConfigPath)
		if err != nil {
			return
		}
		config.QPS = float32(k8sClientQPS)
		config.Burst = k8sClientBurst
		if k8sClientTimeout > 0 {
			config.Timeout = time.Second * time.Duration(k8sClientTimeout)
		}
		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			return
		}
		k8sClient = &K8sClient{
			clientset: clientset,
			config:    config,
		}
		k8sOperator = k8sClient
	})
	return err
}
func (c *K8sClient) CreatePersistentVolume(pv *apiv1.PersistentVolume) (*apiv1.PersistentVolume, error) {
	return c.clientset.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}
func (c *K8sClient) DeletePersistentVolume(name string, deleteOptions *metav1.DeleteOptions) error {
	return c.clientset.CoreV1().PersistentVolumes().Delete(context.TODO(), name, *deleteOptions)
}
func (c *K8sClient) GetPersistentVolume(name string, getOptions metav1.GetOptions) (*apiv1.PersistentVolume, error) {
	return c.clientset.CoreV1().PersistentVolumes().Get(context.TODO(), name, getOptions)
}
func (c *K8sClient) CreatePersistentVolumeClaim(namespace string, pvc *apiv1.PersistentVolumeClaim) (*apiv1.
	PersistentVolumeClaim, error) {
	return c.clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}
func (c *K8sClient) DeletePersistentVolumeClaim(namespace string, name string,
	deleteOptions *metav1.DeleteOptions) error {
	return c.clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, *deleteOptions)
}
func (c *K8sClient) GetPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*apiv1.
	PersistentVolumeClaim, error) {
	return c.clientset.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}
func (c *K8sClient) GetNamespace(namespace string, getOptions metav1.GetOptions) (*apiv1.Namespace, error) {
	return c.clientset.CoreV1().Namespaces().Get(context.TODO(), namespace, getOptions)
}

func (c *K8sClient) ListNamespaces(listOptions metav1.ListOptions) (*apiv1.NamespaceList, error) {
	return c.clientset.CoreV1().Namespaces().List(context.TODO(), listOptions)
}
