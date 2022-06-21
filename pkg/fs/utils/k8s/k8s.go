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
	"bytes"
	"context"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
)

var client *k8sClient

type Client interface {
	// pod
	ProxyGetPods(nodeID string) (result *v1.PodList, err error)
	CreatePod(pod *v1.Pod) (*v1.Pod, error)
	GetPod(podName, namespace string) (*v1.Pod, error)
	DeletePod(pod *v1.Pod) error
	GetPodLog(podName, namespace, containerName string) (string, error)
	// pv
	CreatePersistentVolume(pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
	DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error
	GetPersistentVolume(name string, getOptions metav1.GetOptions) (*v1.PersistentVolume, error)
	ListPersistentVolume(listOptions metav1.ListOptions) (*v1.PersistentVolumeList, error)
	// pvc
	CreatePersistentVolumeClaim(namespace string, pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error)
	DeletePersistentVolumeClaim(namespace string, name string, deleteOptions metav1.DeleteOptions) error
	GetPersistentVolumeClaim(namespace string, name string, getOptions metav1.GetOptions) (*v1.PersistentVolumeClaim, error)
	// ns
	GetNamespace(namespace string, getOptions metav1.GetOptions) (*v1.Namespace, error)
	ListNamespaces(listOptions metav1.ListOptions) (*v1.NamespaceList, error)
}

type k8sClient struct {
	kubernetes.Interface
}

func GetK8sClient() (Client, error) {
	if client == nil {
		var err error
		if client, err = New(common.GetK8SConfigPathEnv(), common.GetK8STimeoutEnv()); err != nil {
			log.Errorf("init k8s client failed: %v", err)
			return nil, err
		}
	}
	return client, nil
}

func New(k8sConfigPath string, k8sClientTimeout int) (*k8sClient, error) {
	// if k8sConfigPath == "", k8s invokes InClusterConfig, using service account to init
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
	return &k8sClient{Interface: clientset}, nil
}

func (c *k8sClient) CreatePod(pod *v1.Pod) (*v1.Pod, error) {
	if pod == nil {
		log.Info("Create pod: pod is nil")
		return nil, nil
	}
	log.Infof("Create pod %s", pod.Name)
	mntPod, err := c.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("Can't create pod %s: %v", pod.Name, err)
		return nil, err
	}
	return mntPod, nil
}

func (c *k8sClient) GetPod(podName, namespace string) (*v1.Pod, error) {
	log.Infof("Get pod %s", podName)
	mntPod, err := c.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Can't get pod %s namespace %s: %v", podName, namespace, err)
		return nil, err
	}
	return mntPod, nil
}

func (c *k8sClient) DeletePod(pod *v1.Pod) error {
	if pod == nil {
		log.Infof("Delete pod: pod is nil")
		return nil
	}
	log.Infof("Delete pod %v", pod.Name)
	return c.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
}

func (c *k8sClient) GetPodLog(podName, namespace, containerName string) (string, error) {
	log.Infof("Get pod %s log", podName)
	tailLines := int64(20)
	req := c.CoreV1().Pods(namespace).GetLogs(podName, &v1.PodLogOptions{
		Container: containerName,
		TailLines: &tailLines,
	})
	podLogs, err := req.Stream(context.TODO())
	if err != nil {
		return "", err
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return "", err
	}
	str := buf.String()
	return str, nil
}

// ProxyGetPods returns all pods on the node with nodeName, and the api server will forward this request to
// the kubelet proxy on the node. Therefore， the pods information comes from kubelet cache, not etcd
func (c *k8sClient) ProxyGetPods(nodeID string) (result *v1.PodList, err error) {
	result = &v1.PodList{}
	err = c.CoreV1().RESTClient().Get().Resource("nodes").
		Name(nodeID).Suffix("/proxy/pods").Do(context.TODO()).Into(result)
	return
}

func (c *k8sClient) ListPersistentVolume(listOptions metav1.ListOptions) (*v1.PersistentVolumeList, error) {
	return c.CoreV1().PersistentVolumes().List(context.TODO(), listOptions)
}

func (c *k8sClient) CreatePersistentVolume(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	return c.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}
func (c *k8sClient) DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error {
	return c.CoreV1().PersistentVolumes().Delete(context.TODO(), name, deleteOptions)
}
func (c *k8sClient) GetPersistentVolume(name string, getOptions metav1.GetOptions) (*v1.PersistentVolume, error) {
	return c.CoreV1().PersistentVolumes().Get(context.TODO(), name, getOptions)
}
func (c *k8sClient) CreatePersistentVolumeClaim(namespace string, pvc *v1.PersistentVolumeClaim) (*v1.
	PersistentVolumeClaim, error) {
	return c.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}
func (c *k8sClient) DeletePersistentVolumeClaim(namespace string, name string, deleteOptions metav1.DeleteOptions) error {
	return c.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, deleteOptions)
}
func (c *k8sClient) GetPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*v1.
	PersistentVolumeClaim, error) {
	return c.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}
func (c *k8sClient) GetNamespace(namespace string, getOptions metav1.GetOptions) (*v1.Namespace, error) {
	return c.CoreV1().Namespaces().Get(context.TODO(), namespace, getOptions)
}

func (c *k8sClient) ListNamespaces(listOptions metav1.ListOptions) (*v1.NamespaceList, error) {
	return c.CoreV1().Namespaces().List(context.TODO(), listOptions)
}
