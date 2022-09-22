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

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
)

var client *k8sClient

type Client interface {
	// pod
	ProxyGetPods(nodeID string) (result *corev1.PodList, err error)
	CreatePod(pod *corev1.Pod) (*corev1.Pod, error)
	GetPod(namespace, name string) (*corev1.Pod, error)
	PatchPod(pod *corev1.Pod, data []byte) error
	UpdatePod(namespace string, pod *corev1.Pod) (*corev1.Pod, error)
	DeletePod(pod *corev1.Pod) error
	GetPodLog(namespace, podName, containerName string) (string, error)
	PatchPodAnnotation(pod *corev1.Pod) error
	// pv
	CreatePersistentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error)
	DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error
	GetPersistentVolume(name string, getOptions metav1.GetOptions) (*corev1.PersistentVolume, error)
	ListPersistentVolume(listOptions metav1.ListOptions) (*corev1.PersistentVolumeList, error)
	// pvc
	CreatePersistentVolumeClaim(namespace string, pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error)
	DeletePersistentVolumeClaim(namespace, name string, deleteOptions metav1.DeleteOptions) error
	GetPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*corev1.PersistentVolumeClaim, error)
	// ns
	GetNamespace(namespace string, getOptions metav1.GetOptions) (*corev1.Namespace, error)
	ListNamespaces(listOptions metav1.ListOptions) (*corev1.NamespaceList, error)
}

type k8sClient struct {
	kubernetes.Interface
}

func GetK8sClient() (Client, error) {
	if client == nil {
		var err error
		if client, err = New(GetK8SConfigPathEnv(), GetK8STimeoutEnv()); err != nil {
			log.Errorf("init k8s client failed: %v", err)
			return nil, err
		}
	}
	return client, nil
}

func GetFakeK8sClient() Client {
	if client == nil {
		fakeClientSet := fake.NewSimpleClientset()
		client = &k8sClient{Interface: fakeClientSet}
	}
	return client
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

func (c *k8sClient) PatchPodAnnotation(pod *corev1.Pod) error {
	payload := []PatchMapValue{{
		Op:    "replace",
		Path:  "/metadata/annotations",
		Value: pod.Annotations,
	}}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("parse annotation json error: %v", err)
		return err
	}
	if err := c.PatchPod(pod, payloadBytes); err != nil {
		log.Errorf("patch pod %s error: %v", pod.Name, err)
		return err
	}
	return nil
}

func (c *k8sClient) CreatePod(pod *corev1.Pod) (*corev1.Pod, error) {
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

func (c *k8sClient) GetPod(namespace, name string) (*corev1.Pod, error) {
	log.Infof("Get pod %s", name)
	mntPod, err := c.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Can't get pod %s namespace %s: %v", name, namespace, err)
		return nil, err
	}
	return mntPod, nil
}

type PatchMapValue struct {
	Op    string            `json:"op"`
	Path  string            `json:"path"`
	Value map[string]string `json:"value"`
}

func (c *k8sClient) PatchPod(pod *corev1.Pod, data []byte) error {
	if pod == nil {
		log.Info("Patch pod: pod is nil")
		return nil
	}
	log.Infof("Patch pod %v", pod.Name)
	_, err := c.CoreV1().Pods(pod.Namespace).Patch(context.TODO(),
		pod.Name, types.JSONPatchType, data, metav1.PatchOptions{})
	return err
}

func (c *k8sClient) UpdatePod(namespace string, pod *corev1.Pod) (*corev1.Pod, error) {
	log.Infof("Get pod %s", pod.Name)
	updatedPod, err := c.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	if err != nil {
		log.Errorf("Can't get pod %s namespace %s: %v", pod.Name, namespace, err)
		return nil, err
	}
	return updatedPod, nil
}

func (c *k8sClient) DeletePod(pod *corev1.Pod) error {
	if pod == nil {
		log.Infof("Delete pod: pod is nil")
		return nil
	}
	log.Infof("Delete pod %v", pod.Name)
	return c.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
}

func (c *k8sClient) GetPodLog(namespace, podName, containerName string) (string, error) {
	log.Infof("Get pod %s log", podName)
	tailLines := int64(20)
	req := c.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
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
func (c *k8sClient) ProxyGetPods(nodeID string) (result *corev1.PodList, err error) {
	result = &corev1.PodList{}
	err = c.CoreV1().RESTClient().Get().Resource("nodes").
		Name(nodeID).Suffix("/proxy/pods").Do(context.TODO()).Into(result)
	return
}

func (c *k8sClient) ListPersistentVolume(listOptions metav1.ListOptions) (*corev1.PersistentVolumeList, error) {
	return c.CoreV1().PersistentVolumes().List(context.TODO(), listOptions)
}

func (c *k8sClient) CreatePersistentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return c.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}
func (c *k8sClient) DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error {
	return c.CoreV1().PersistentVolumes().Delete(context.TODO(), name, deleteOptions)
}
func (c *k8sClient) GetPersistentVolume(name string, getOptions metav1.GetOptions) (*corev1.PersistentVolume, error) {
	return c.CoreV1().PersistentVolumes().Get(context.TODO(), name, getOptions)
}
func (c *k8sClient) CreatePersistentVolumeClaim(namespace string, pvc *corev1.PersistentVolumeClaim) (*corev1.
	PersistentVolumeClaim, error) {
	return c.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}
func (c *k8sClient) DeletePersistentVolumeClaim(namespace string, name string, deleteOptions metav1.DeleteOptions) error {
	return c.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, deleteOptions)
}
func (c *k8sClient) GetPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*corev1.
	PersistentVolumeClaim, error) {
	return c.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}
func (c *k8sClient) GetNamespace(namespace string, getOptions metav1.GetOptions) (*corev1.Namespace, error) {
	return c.CoreV1().Namespaces().Get(context.TODO(), namespace, getOptions)
}

func (c *k8sClient) ListNamespaces(listOptions metav1.ListOptions) (*corev1.NamespaceList, error) {
	return c.CoreV1().Namespaces().List(context.TODO(), listOptions)
}
