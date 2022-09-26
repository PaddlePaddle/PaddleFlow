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

package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	busv1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime/kubernetes/controller"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime/kubernetes/executor"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type KubeRuntime struct {
	cluster          *schema.Cluster
	clientset        kubernetes.Interface
	dynamicClientOpt *k8s.DynamicClientOption
}

func NewKubeRuntime(cluster schema.Cluster) RuntimeService {
	kr := &KubeRuntime{
		cluster: &cluster,
	}
	return kr
}

func getFileSystem(jobConf schema.Conf, tasks []schema.Member) []schema.FileSystem {
	fileSystems := jobConf.GetAllFileSystem()
	for _, task := range tasks {
		fileSystems = append(fileSystems, task.Conf.GetAllFileSystem()...)
	}
	return fileSystems
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
	kr.dynamicClientOpt, err = k8s.CreateDynamicClientOpt(config, kr.cluster)
	if err != nil {
		log.Errorf("init dynamic client failed. error:%s", err)
		return err
	}
	// new kubernetes typed client
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Errorf("create kubernetes client failed, err: %v", err)
		return err
	}
	kr.clientset = k8sClient
	return nil
}

func (kr *KubeRuntime) SubmitJob(jobInfo *api.PFJob) error {
	// add trace log point
	jobID := jobInfo.ID
	traceLogger := trace_logger.KeyWithUpdate(jobID)
	msg := fmt.Sprintf("submit job[%v] to cluster[%s] queue[%s]", jobInfo.ID, kr.cluster.ID, jobInfo.QueueID)
	log.Infof(msg)
	traceLogger.Infof(msg)
	// prepare kubernetes storage
	traceLogger.Infof("prepare kubernetes storage")
	jobFileSystems := getFileSystem(jobInfo.Conf, jobInfo.Tasks)
	for _, fs := range jobFileSystems {
		fsID := common.ID(jobInfo.UserName, fs.Name)
		pvName, err := kr.CreatePV(jobInfo.Namespace, fsID)
		if err != nil {
			log.Errorf("create pv for job[%s] failed, err: %v", jobInfo.ID, err)
			return err
		}
		msg = fmt.Sprintf("SubmitJob CreatePV fsID=%s pvName=%s", fsID, pvName)
		log.Infof(msg)
		traceLogger.Infof(msg)
		err = kr.CreatePVC(jobInfo.Namespace, fsID, pvName)
		if err != nil {
			log.Errorf("create pvc for job[%s] failed, err: %v", jobInfo.ID, err)
			return err
		}
	}
	// submit job
	traceLogger.Infof("new kube job")
	job, err := executor.NewKubeJob(jobInfo, kr.dynamicClientOpt)
	if err != nil {
		log.Warnf("new kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
		return err
	}
	traceLogger.Infof("create job")
	jobID, err = job.CreateJob()
	if err != nil {
		log.Warnf("create kubernetes job[%s] failed, err: %v", jobInfo.Name, err)
		return err
	}

	log.Debugf("submit job[%s] successful", jobID)
	return nil
}

func (kr *KubeRuntime) StopJob(jobInfo *api.PFJob) error {
	log.Infof("stop job[%s] on cluster[%s] queue[%s]", jobInfo.ID, kr.cluster.ID, jobInfo.QueueID)
	job, err := executor.NewKubeJob(jobInfo, kr.dynamicClientOpt)
	if err != nil {
		log.Warnf("stop kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
		return err
	}
	err = job.StopJobByID(jobInfo.ID)
	if err != nil && !k8serrors.IsNotFound(err) {
		log.Warnf("stop kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
		return err
	}
	log.Debugf("stop job[%s] successful", jobInfo.ID)
	return nil
}

func (kr *KubeRuntime) UpdateJob(jobInfo *api.PFJob) error {
	log.Infof("update job[%s] on cluster[%s] queue[%s]", jobInfo.ID, kr.cluster.ID, jobInfo.QueueID)
	job, err := executor.NewKubeJob(jobInfo, kr.dynamicClientOpt)
	if err != nil {
		log.Warnf("update kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
		return err
	}

	// update job priority
	if len(jobInfo.PriorityClassName) != 0 {
		err = kr.updateJobPriority(jobInfo)
		if err != nil {
			return err
		}
	}
	// update labels and annotations
	if (jobInfo.Labels != nil && len(jobInfo.Labels) != 0) ||
		(jobInfo.Annotations != nil && len(jobInfo.Annotations) != 0) {
		patchJSON := struct {
			metav1.ObjectMeta `json:"metadata,omitempty"`
		}{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      jobInfo.Labels,
				Annotations: jobInfo.Annotations,
			},
		}
		updateData, err := json.Marshal(patchJSON)
		if err != nil {
			log.Errorf("update kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
			return err
		}
		err = job.UpdateJob(updateData)
		if err != nil && !k8serrors.IsNotFound(err) {
			log.Warnf("update kubernetes job[%s] failed, err: %v", jobInfo.ID, err)
			return err
		}
	}
	return nil
}

func (kr *KubeRuntime) updateJobPriority(jobInfo *api.PFJob) error {
	// get pod group name for job
	pgName := executor.GetPodGroupName(jobInfo.ID)
	if len(pgName) == 0 {
		err := fmt.Errorf("update priority for job %s failed, pod group not found", jobInfo.ID)
		log.Errorln(err)
		return err
	}

	obj, err := executor.Get(jobInfo.Namespace, pgName, k8s.PodGroupGVK, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("get pod group for job %s failed, err: %v", jobInfo.ID, err)
		return err
	}
	oldPG := &schedulingv1beta1.PodGroup{}
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, oldPG); err != nil {
		log.Errorf("convert unstructured object [%v] to pod group failed. err: %v", obj, err)
		return err
	}
	if oldPG.Status.Phase != schedulingv1beta1.PodGroupInqueue &&
		oldPG.Status.Phase != schedulingv1beta1.PodGroupPending {
		errmsg := fmt.Errorf("the job %s is already scheduled", jobInfo.ID)
		log.Errorln(errmsg)
		return errmsg
	}

	priorityClassName := executor.KubePriorityClass(jobInfo.PriorityClassName)
	if oldPG.Spec.PriorityClassName != priorityClassName {
		oldPG.Spec.PriorityClassName = priorityClassName
	} else {
		err = fmt.Errorf("the priority of job %s is already %s", jobInfo.ID, oldPG.Spec.PriorityClassName)
		log.Errorln(err)
		return err
	}
	err = executor.Update(oldPG, k8s.PodGroupGVK, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("update priority for job %s failed. err: %v", jobInfo.ID, err)
	}
	return err
}

func (kr *KubeRuntime) DeleteJob(jobInfo *api.PFJob) error {
	log.Infof("delete job %v from cluster %s, and queue %s", jobInfo.ID, kr.cluster.ID, jobInfo.QueueID)
	job, err := executor.NewKubeJob(jobInfo, kr.dynamicClientOpt)
	if err != nil {
		log.Warnf("create kubernetes job %s failed, err: %v", jobInfo.ID, err)
		return err
	}
	// TODO: add DeleteJob interface
	err = job.StopJobByID(jobInfo.ID)
	if err != nil && !k8serrors.IsNotFound(err) {
		log.Warnf("delete kubernetes job %s failed, err: %v", jobInfo.ID, err)
		return err
	}
	log.Debugf("delete job %s successful", jobInfo.ID)
	return nil
}

func (kr *KubeRuntime) SyncJob(stopCh <-chan struct{}) {
	log.Infof("start job sync loop for cluster[%s]", kr.cluster.ID)

	syncController, err := controller.New(controller.JobSyncControllerName, kr.dynamicClientOpt.Config, kr.cluster)
	if err != nil {
		log.Errorf("init sync controller failed, err: %v", err)
		return
	}
	go syncController.Run(stopCh)
}

func (kr *KubeRuntime) GCJob(stopCh <-chan struct{}) {
	log.Infof("start job gc loop for cluster[%s]", kr.cluster.ID)

	gcController, err := controller.New(controller.JobGCControllerName, kr.dynamicClientOpt.Config, kr.cluster)
	if err != nil {
		log.Errorf("init sync controller failed, err: %v", err)
		return
	}
	go gcController.Run(stopCh)
}

func (kr *KubeRuntime) SyncQueue(stopCh <-chan struct{}) {
	log.Infof("start queue sync loop for cluster[%s]", kr.cluster.ID)

	queueController, err := controller.New(controller.QueueSyncControllerName, kr.dynamicClientOpt.Config, kr.cluster)
	if err != nil {
		log.Errorf("init queue sync controller failed, err: %v", err)
		return
	}
	go queueController.Run(stopCh)

}

func (kr *KubeRuntime) CreateQueue(q *model.Queue) error {
	switch q.QuotaType {
	case schema.TypeVolcanoCapabilityQuota:
		return kr.createVCQueue(q)
	case schema.TypeElasticQuota:
		return kr.createElasticResourceQuota(q)
	default:
		return fmt.Errorf("quota type %s is not supported", q.QuotaType)
	}
}

func (kr *KubeRuntime) createVCQueue(q *model.Queue) error {
	capability := k8s.NewResourceList(q.MaxResources)
	log.Debugf("CreateQueue resourceList[%v]", capability)

	queue := &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: q.Name,
		},
		Spec: schedulingv1beta1.QueueSpec{
			Capability: capability,
		},
		Status: schedulingv1beta1.QueueStatus{
			State: schedulingv1beta1.QueueStateOpen,
		},
	}
	log.Debugf("CreateQueue queue info:%#v", queue)
	if err := executor.Create(queue, k8s.VCQueueGVK, kr.dynamicClientOpt); err != nil {
		log.Errorf("CreateQueue error. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) createElasticResourceQuota(q *model.Queue) error {
	maxResources := k8s.NewResourceList(q.MaxResources)
	minResources := k8s.NewResourceList(q.MinResources)
	log.Debugf("Elastic resource quota max resources:%v,  min resources %v", maxResources, minResources)

	equota := &schedulingv1beta1.ElasticResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:   q.Name,
			Labels: q.Location,
		},
		Spec: schedulingv1beta1.ElasticResourceQuotaSpec{
			Max:         maxResources,
			Min:         minResources,
			Namespace:   q.Namespace,
			Reclaimable: true,
		},
	}
	log.Debugf("Create elastic resource quota info:%#v", equota)
	if err := executor.Create(equota, k8s.EQuotaGVK, kr.dynamicClientOpt); err != nil {
		log.Errorf("CreateQueue on cluster falied. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) DeleteQueue(q *model.Queue) error {
	var gvk = k8s.VCQueueGVK
	switch q.QuotaType {
	case schema.TypeVolcanoCapabilityQuota:
		gvk = k8s.VCQueueGVK
	case schema.TypeElasticQuota:
		gvk = k8s.EQuotaGVK
	default:
		return fmt.Errorf("quota type %s is not supported", q.QuotaType)
	}

	err := executor.Delete("", q.Name, gvk, kr.dynamicClientOpt)
	if err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("DeleteQueue error. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) CloseQueue(q *model.Queue) error {
	switch q.QuotaType {
	case schema.TypeVolcanoCapabilityQuota:
		return kr.executeVCQueueAction(q, busv1alpha1.CloseQueueAction)
	case schema.TypeElasticQuota:
		return nil
	default:
		return fmt.Errorf("quota type %s is not supported", q.QuotaType)
	}
}

func (kr *KubeRuntime) executeVCQueueAction(q *model.Queue, action busv1alpha1.Action) error {
	obj, err := executor.Get("", q.Name, k8s.VCQueueGVK, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("execute queue action get queue failed. queueName:[%s]", q.Name)
		return err
	}

	ctrlRef := metav1.NewControllerRef(obj, helpers.V1beta1QueueKind)
	cmd := &busv1alpha1.Command{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-",
				obj.GetName(), strings.ToLower(string(action))),
			OwnerReferences: []metav1.OwnerReference{
				*ctrlRef,
			},
		},
		TargetObject: ctrlRef,
		Action:       string(action),
	}
	if err = executor.Create(cmd, k8s.VCQueueGVK, kr.dynamicClientOpt); err != nil {
		log.Errorf("execute queue action failed. queueName:[%s] err:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) UpdateQueue(q *model.Queue) error {
	switch q.QuotaType {
	case schema.TypeVolcanoCapabilityQuota:
		return kr.updateVCQueue(q)
	case schema.TypeElasticQuota:
		return kr.updateElasticResourceQuota(q)
	default:
		return fmt.Errorf("quota type %s is not supported", q.QuotaType)
	}
}

func (kr *KubeRuntime) updateVCQueue(q *model.Queue) error {
	capability := k8s.NewResourceList(q.MaxResources)
	log.Debugf("UpdateQueue resourceList[%v]", capability)
	object, err := executor.Get("", q.Name, k8s.VCQueueGVK, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("execute action of getting queue failed. queueName:[%s]", q.Name)
		return err
	}
	var queue schedulingv1beta1.Queue
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &queue)
	queue.Spec.Capability = capability
	queue.Status.State = schedulingv1beta1.QueueState(q.Status)

	log.Infof("UpdateQueue queue info:%#v", queue)
	if err := executor.Update(&queue, k8s.VCQueueGVK, kr.dynamicClientOpt); err != nil {
		log.Errorf("UpdateQueue error. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) updateElasticResourceQuota(q *model.Queue) error {
	maxResources := k8s.NewResourceList(q.MaxResources)
	minResources := k8s.NewResourceList(q.MinResources)
	log.Debugf("Elastic resource quota max resources:%v,  min resources %v", maxResources, minResources)
	object, err := executor.Get("", q.Name, k8s.EQuotaGVK, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("execute action of getting queue failed. queueName:[%s]", q.Name)
		return err
	}
	var equota schedulingv1beta1.ElasticResourceQuota
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &equota)

	equota.Spec.Max = maxResources
	equota.Spec.Min = minResources
	equota.Spec.Namespace = q.Namespace
	// update labels
	if equota.Labels == nil {
		equota.Labels = make(map[string]string)
	}
	newLabels := make(map[string]string)
	for key, v := range q.Location {
		newLabels[key] = v
	}
	equota.Labels = newLabels

	log.Infof("Update elastic resource quota info:%#v", equota)
	if err := executor.Update(&equota, k8s.EQuotaGVK, kr.dynamicClientOpt); err != nil {
		log.Errorf("UpdateQueue on cluster falied. queueName:[%s], error:[%s]", q.Name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) GetQueueUsedQuota(q *model.Queue) (*resources.Resource, error) {
	log.Infof("get used quota for queue %s, namespace %s", q.Name, q.Namespace)

	fieldSelector := fmt.Sprintf(
		"status.phase!=Succeeded,status.phase!=Failed,status.phase!=Unknown,spec.schedulerName=%s",
		config.GlobalServerConfig.Job.SchedulerName)
	// TODO: add label selector
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

func isAllocatedPod(pod *v1.Pod, queueName string) bool {
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
	if err := executor.Create(obj, gvk, kr.dynamicClientOpt); err != nil {
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
	if err := executor.Update(obj, gvk, kr.dynamicClientOpt); err != nil {
		log.Errorf("update kubernetes %s resource failed, name:[%s/%s] err:[%v]", gvk.String(), namespace, name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) GetObject(namespace, name string, gvk k8sschema.GroupVersionKind) (interface{}, error) {
	log.Debugf("get kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	resourceObj, err := executor.Get(namespace, name, gvk, kr.dynamicClientOpt)
	if err != nil {
		log.Errorf("get kubernetes %s resource %s/%s failed, err: %v", gvk.String(), namespace, name, err.Error())
		return nil, err
	}
	return resourceObj, nil
}

func (kr *KubeRuntime) DeleteObject(namespace, name string, gvk k8sschema.GroupVersionKind) error {
	log.Infof("delete kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	if err := executor.Delete(namespace, name, gvk, kr.dynamicClientOpt); err != nil {
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
	newPV := &apiv1.PersistentVolume{}
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

func (kr *KubeRuntime) buildPV(pv *apiv1.PersistentVolume, fsID string) error {
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
	newPVC := &apiv1.PersistentVolumeClaim{}
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
	return getKubernetesLogs(kr.clientset, jobLogRequest)
}

func (kr *KubeRuntime) ListNamespaces(listOptions metav1.ListOptions) (*v1.NamespaceList, error) {
	return kr.clientset.CoreV1().Namespaces().List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) createPersistentVolume(pv *apiv1.PersistentVolume) (*apiv1.PersistentVolume, error) {
	return kr.clientset.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}

func (kr *KubeRuntime) DeletePersistentVolume(name string, deleteOptions metav1.DeleteOptions) error {
	return kr.clientset.CoreV1().PersistentVolumes().Delete(context.TODO(), name, deleteOptions)
}

func (kr *KubeRuntime) getPersistentVolume(name string, getOptions metav1.GetOptions) (*apiv1.PersistentVolume, error) {
	return kr.clientset.CoreV1().PersistentVolumes().Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) createPersistentVolumeClaim(namespace string, pvc *apiv1.PersistentVolumeClaim) (*apiv1.
	PersistentVolumeClaim, error) {
	return kr.clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
}

func (kr *KubeRuntime) DeletePersistentVolumeClaim(namespace string, name string,
	deleteOptions metav1.DeleteOptions) error {
	return kr.clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, deleteOptions)
}

func (kr *KubeRuntime) getPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*apiv1.
	PersistentVolumeClaim, error) {
	return kr.clientset.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) listNodes(listOptions metav1.ListOptions) (*v1.NodeList, error) {
	return kr.clientset.CoreV1().Nodes().List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) ListPods(namespace string, listOptions metav1.ListOptions) (*v1.PodList, error) {
	return kr.clientset.CoreV1().Pods(namespace).List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) DeletePod(namespace, name string) error {
	return kr.clientset.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func (kr *KubeRuntime) getNodeQuotaListImpl(subQuotaFn func(r *resources.Resource, pod *apiv1.Pod) error) (schema.QuotaSummary, []schema.NodeQuotaInfo, error) {
	result := []schema.NodeQuotaInfo{}
	summary := schema.QuotaSummary{
		TotalQuota: *k8s.NewResource(v1.ResourceList{}),
		IdleQuota:  *k8s.NewResource(v1.ResourceList{}),
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
