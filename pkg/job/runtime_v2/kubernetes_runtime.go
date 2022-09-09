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

package runtime_v2

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/controller"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type KubeRuntime struct {
	cluster    *schema.Cluster
	kubeClient framework.RuntimeClientInterface
}

func NewKubeRuntime(cluster schema.Cluster) RuntimeService {
	kr := &KubeRuntime{
		cluster: &cluster,
	}
	return kr
}

func (kr *KubeRuntime) Name() string {
	return fmt.Sprintf("kubernetes runtime for cluster: %s", kr.cluster.Name)
}

func (kr *KubeRuntime) BuildConfig() (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if len(kr.cluster.ClientOpt.Config) == 0 {
		if cfg, err = clientcmd.BuildConfigFromFlags("", ""); err != nil {
			log.Errorf("Failed to build rest.config by BuildConfigFromFlags, err:[%v]", err)
			return nil, err
		}
	} else {
		// decode credential base64 string to []byte
		configBytes, decodeErr := base64.StdEncoding.DecodeString(kr.cluster.ClientOpt.Config)
		if decodeErr != nil {
			err := fmt.Errorf("decode cluster[%s] credential base64 string error! msg: %s",
				kr.cluster.Name, decodeErr.Error())
			return nil, err
		}
		cfg, err = clientcmd.RESTConfigFromKubeConfig(configBytes)
		if err != nil {
			log.Errorf("Failed to build rest.config from kubeConfBytes[%s], err:[%v]", string(configBytes[:]), err)
			return nil, err
		}
	}

	// set qps, burst
	cfg.QPS = kr.cluster.ClientOpt.QPS
	cfg.Burst = kr.cluster.ClientOpt.Burst
	return cfg, nil
}

func (kr *KubeRuntime) Init() error {
	config, err := kr.BuildConfig()
	if err != nil {
		log.Errorf("build config failed. error:%s", err)
		return err
	}
	kubeClient, err := client.CreateKubeRuntimeClient(config, kr.cluster)
	if err != nil {
		log.Errorf("create kubernetes client failed, err: %v", err)
		return err
	}
	kr.kubeClient = kubeClient
	return nil
}

func (kr *KubeRuntime) Job(frameworkVersion string) framework.JobInterface {
	jobBuilder, found := framework.GetJobBuilder(schema.KubernetesType, frameworkVersion)
	if !found {
		return nil
	}
	return jobBuilder(kr.kubeClient)
}

func (kr *KubeRuntime) Queue(quotaType string) framework.QueueInterface {
	log.Errorf("queue is not supported")
	return nil
}

func (kr *KubeRuntime) SyncController(stopCh <-chan struct{}) {
	log.Infof("start job sync loop for cluster[%s]", kr.cluster.ID)

	syncController := controller.NewJobSync()
	err := syncController.Initialize(kr.kubeClient)
	if err != nil {
		log.Errorf("init sync controller failed, err: %v", err)
		return
	}
	go syncController.Run(stopCh)
}

func (kr *KubeRuntime) GetQueueUsedQuota(q *api.QueueInfo) (*resources.Resource, error) {
	log.Infof("get used quota for queue %s, namespace %s", q.Name, q.Namespace)

	fieldSelector := fmt.Sprintf(
		"status.phase!=Succeeded,status.phase!=Failed,status.phase!=Unknown,spec.schedulerName=%s",
		config.GlobalServerConfig.Job.SchedulerName)
	// TODO: add label selector after set queue in pod labels
	listOpts := metav1.ListOptions{
		FieldSelector: fieldSelector,
	}
	podList, err := kr.ListPods(q.Namespace, listOpts)
	if err != nil || podList == nil {
		log.Errorf("get queue used quota failed, err: %v", err)
		return nil, fmt.Errorf("get queue used quota failed, err: %v", err)
	}
	usedResource := resources.EmptyResource()
	for idx := range podList.Items {
		if isAllocatedPod(&podList.Items[idx], q.Name) {
			podRes := k8s.CalcPodResources(&podList.Items[idx])
			usedResource.Add(podRes)
		}
	}
	return usedResource, nil
}

func isAllocatedPod(pod *corev1.Pod, queueName string) bool {
	log.Debugf("pod name %s/%s, nodeName: %s, phase: %s, annotations: %v\n",
		pod.Namespace, pod.Name, pod.Spec.NodeName, pod.Status.Phase, pod.Annotations)
	if pod.Annotations == nil || pod.Annotations[v1alpha1.QueueNameKey] != queueName {
		return false
	}
	if pod.Spec.NodeName != "" {
		return true
	}
	return false
}

func (kr *KubeRuntime) CreateObject(obj *unstructured.Unstructured) error {
	if obj == nil {
		return fmt.Errorf("create kubernetes resource failed, object is nil")
	}
	namespace := obj.GetNamespace()
	name := obj.GetName()
	gvk := obj.GroupVersionKind()

	// TODO: add more check
	log.Infof("create kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	if err := kr.kubeClient.Create(obj, client.KubeFrameworkVersion(gvk)); err != nil {
		log.Errorf("create kubernetes %s resource failed. name:[%s/%s] err:[%s]", gvk.String(), namespace, name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) UpdateObject(obj *unstructured.Unstructured) error {
	if obj == nil {
		return fmt.Errorf("update kubernetes resource failed, object is nil")
	}

	namespace := obj.GetNamespace()
	name := obj.GetName()
	gvk := obj.GroupVersionKind()
	// TODO: add more check
	log.Infof("update kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	if err := kr.kubeClient.Update(obj, client.KubeFrameworkVersion(gvk)); err != nil {
		log.Errorf("update kubernetes %s resource failed, name:[%s/%s] err:[%v]", gvk.String(), namespace, name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) GetObject(namespace, name string, gvk k8sschema.GroupVersionKind) (interface{}, error) {
	log.Debugf("get kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	resourceObj, err := kr.kubeClient.Get(namespace, name, client.KubeFrameworkVersion(gvk))
	if err != nil {
		log.Errorf("get kubernetes %s resource %s/%s failed, err: %v", gvk.String(), namespace, name, err.Error())
		return nil, err
	}
	return resourceObj, nil
}

func (kr *KubeRuntime) DeleteObject(namespace, name string, gvk k8sschema.GroupVersionKind) error {
	log.Infof("delete kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	if err := kr.kubeClient.Delete(namespace, name, client.KubeFrameworkVersion(gvk)); err != nil {
		log.Errorf("delete kubernetes %s resource %s/%s failed, err: %v", gvk.String(), namespace, name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) CreatePV(namespace, fsID string) (string, error) {
	pv := config.DefaultPV
	pv.Name = schema.ConcatenatePVName(namespace, fsID)
	// check pv existence
	if _, err := kr.getPersistentVolume(pv.Name, metav1.GetOptions{}); err == nil {
		return pv.Name, nil
	} else if !k8serrors.IsNotFound(err) {
		return "", err
	}
	// construct a new pv
	newPV := &corev1.PersistentVolume{}
	if err := copier.Copy(newPV, pv); err != nil {
		return "", err
	}
	if newPV.Spec.CSI == nil || newPV.Spec.CSI.VolumeAttributes == nil {
		err := fmt.Errorf("pv[%s] generation error: no csi or csi volume attributes", pv.Name)
		log.Errorf(err.Error())
		return "", err
	}
	if err := kr.buildPV(newPV, fsID); err != nil {
		log.Errorf(err.Error())
		return "", err
	}
	// create pv in k8s
	if _, err := kr.createPersistentVolume(newPV); err != nil {
		return "", err
	}
	return pv.Name, nil
}

func (kr *KubeRuntime) buildPV(pv *corev1.PersistentVolume, fsID string) error {
	// filesystem
	fs, err := storage.Filesystem.GetFileSystemWithFsID(fsID)
	if err != nil {
		retErr := fmt.Errorf("create PV get fs[%s] err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	fsStr, err := json.Marshal(fs)
	if err != nil {
		retErr := fmt.Errorf("create PV json.marshal fs[%s] err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	// fs_cache_config
	fsCacheConfig, err := storage.Filesystem.GetFSCacheConfig(fsID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		retErr := fmt.Errorf("create PV get fsCacheConfig[%s] err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	fsCacheConfigStr, err := json.Marshal(fsCacheConfig)
	if err != nil {
		retErr := fmt.Errorf("create PV json.marshal fsCacheConfig[%s] err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return retErr
	}

	// set VolumeAttributes
	pv.Spec.CSI.VolumeHandle = pv.Name
	pv.Spec.CSI.VolumeAttributes[schema.PFSServer] = config.GetServiceAddress()
	pv.Spec.CSI.VolumeAttributes[schema.PFSID] = fsID
	pv.Spec.CSI.VolumeAttributes[schema.PFSClusterID] = kr.cluster.ID
	pv.Spec.CSI.VolumeAttributes[schema.PFSInfo] = base64.StdEncoding.EncodeToString(fsStr)
	pv.Spec.CSI.VolumeAttributes[schema.PFSCache] = base64.StdEncoding.EncodeToString(fsCacheConfigStr)
	return nil
}

func (kr *KubeRuntime) CreatePVC(namespace, fsId, pv string) error {
	pvc := config.DefaultPVC
	pvcName := schema.ConcatenatePVCName(fsId)
	// check pvc existence
	if _, err := kr.getPersistentVolumeClaim(namespace, pvcName, metav1.GetOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return err
	}
	// construct a new pvc
	newPVC := &corev1.PersistentVolumeClaim{}
	if err := copier.Copy(newPVC, pvc); err != nil {
		return err
	}
	newPVC.Namespace = namespace
	newPVC.Name = pvcName
	newPVC.Spec.VolumeName = pv
	// create pvc in k8s
	if _, err := kr.createPersistentVolumeClaim(namespace, newPVC); err != nil {
		return err
	}
	return nil
}

func (kr *KubeRuntime) GetJobLog(jobLogRequest schema.JobLogRequest) (schema.JobLogInfo, error) {
	// TODO: GetJobLog
	return schema.JobLogInfo{}, nil
}

func (kr *KubeRuntime) clientset() kubernetes.Interface {
	kubeClient := kr.kubeClient.(*client.KubeRuntimeClient)
	return kubeClient.Client
}

func (kr *KubeRuntime) ListNamespaces(listOptions metav1.ListOptions) (*corev1.NamespaceList, error) {
	return kr.clientset().CoreV1().Namespaces().List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) createPersistentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return kr.clientset().CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}

func (kr *KubeRuntime) DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error {
	return kr.clientset().CoreV1().PersistentVolumes().Delete(context.TODO(), name, deleteOptions)
}

func (kr *KubeRuntime) getPersistentVolume(name string, getOptions metav1.GetOptions) (*corev1.PersistentVolume, error) {
	return kr.clientset().CoreV1().PersistentVolumes().Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) createPersistentVolumeClaim(namespace string, pvc *corev1.PersistentVolumeClaim) (*corev1.
	PersistentVolumeClaim, error) {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}

func (kr *KubeRuntime) DeletePersistentVolumeClaim(namespace string, name string,
	deleteOptions metav1.DeleteOptions) error {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, deleteOptions)
}

func (kr *KubeRuntime) getPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*corev1.
	PersistentVolumeClaim, error) {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) listNodes(listOptions metav1.ListOptions) (*corev1.NodeList, error) {
	return kr.clientset().CoreV1().Nodes().List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) ListPods(namespace string, listOptions metav1.ListOptions) (*corev1.PodList, error) {
	return kr.clientset().CoreV1().Pods(namespace).List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) DeletePod(namespace, name string) error {
	return kr.clientset().CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func (kr *KubeRuntime) getNodeQuotaListImpl(subQuotaFn func(r *resources.Resource, pod *corev1.Pod) error) (schema.QuotaSummary, []schema.NodeQuotaInfo, error) {
	result := []schema.NodeQuotaInfo{}
	summary := schema.QuotaSummary{
		TotalQuota: *k8s.NewResource(corev1.ResourceList{}),
		IdleQuota:  *k8s.NewResource(corev1.ResourceList{}),
	}
	nodes, _ := kr.listNodes(metav1.ListOptions{})
	log.Infof("ListNodeQuota nodes Items len: %d", len(nodes.Items))

	for _, node := range nodes.Items {
		nodeSchedulable := !node.Spec.Unschedulable
		// 过滤掉不能调度的节点
		if !nodeSchedulable {
			continue
		}
		totalQuota := k8s.NewResource(node.Status.Allocatable)
		idleQuota := k8s.NewResource(node.Status.Allocatable)
		nodeName := node.ObjectMeta.Name
		log.Infof("nodeName: %s, totalQuota: %v, idleQuota: %v", nodeName, totalQuota, idleQuota)

		fieldSelector := "status.phase!=Succeeded,status.phase!=Failed," +
			"status.phase!=Unknown,spec.nodeName=" + nodeName

		pods, _ := kr.ListPods("", metav1.ListOptions{
			FieldSelector: fieldSelector,
		})
		for _, pod := range pods.Items {
			err := subQuotaFn(idleQuota, &pod)
			if err != nil {
				return summary, result, err
			}
		}

		nodeQuota := schema.NodeQuotaInfo{
			NodeName:    nodeName,
			Schedulable: nodeSchedulable,
			Total:       *totalQuota,
			Idle:        *idleQuota,
		}
		result = append(result, nodeQuota)
		summary.TotalQuota.Add(totalQuota)
		summary.IdleQuota.Add(idleQuota)
	}

	return summary, result, nil
}

// 返回quota信息
func (kr *KubeRuntime) ListNodeQuota() (schema.QuotaSummary, []schema.NodeQuotaInfo, error) {
	return kr.getNodeQuotaListImpl(k8s.SubQuota)
}
