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
	"os"
	"sort"
	"strconv"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/controller"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type KubeRuntime struct {
	cluster    pfschema.Cluster
	kubeClient framework.RuntimeClientInterface
}

func NewKubeRuntime(cluster pfschema.Cluster) RuntimeService {
	cluster.Type = pfschema.KubernetesType
	return &KubeRuntime{
		cluster: cluster,
	}
}

func (kr *KubeRuntime) Name() string {
	return fmt.Sprintf("kubernetes runtime for cluster: %s", kr.cluster.Name)
}

func (kr *KubeRuntime) String() string {
	msg := "kubernetes runtime"
	if kr.kubeClient != nil {
		msg = kr.kubeClient.Cluster()
	}
	return msg
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
	kubeClient, err := client.CreateKubeRuntimeClient(config, &kr.cluster)
	if err != nil {
		log.Errorf("create kubernetes client failed, err: %v", err)
		return err
	}
	kr.kubeClient = kubeClient
	return nil
}

func (kr *KubeRuntime) SubmitJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("submit job failed, job is nil")
	}
	// add trace log point
	jobID := job.ID
	traceLogger := trace_logger.KeyWithUpdate(jobID)
	msg := fmt.Sprintf("submit job[%v] to cluster[%s] queue[%s]", job.ID, kr.cluster.ID, job.QueueID)
	log.Infof(msg)
	traceLogger.Infof(msg)
	// prepare kubernetes storage
	traceLogger.Infof("prepare kubernetes storage")
	jobFileSystems := job.Conf.GetAllFileSystem()
	for _, task := range job.Tasks {
		jobFileSystems = append(jobFileSystems, task.Conf.GetAllFileSystem()...)
	}
	for _, fs := range jobFileSystems {
		if fs.Type == pfschema.PFSTypeLocal {
			log.Infof("skip create pv/pvc, fs type is local")
			continue
		}
		fsID := common.ID(job.UserName, fs.Name)
		pvName, err := kr.CreatePV(job.Namespace, fsID)
		if err != nil {
			log.Errorf("create pv for job[%s] failed, err: %v", job.ID, err)
			return err
		}
		msg = fmt.Sprintf("SubmitJob CreatePV fsID=%s pvName=%s", fsID, pvName)
		log.Infof(msg)
		traceLogger.Infof(msg)
		err = kr.CreatePVC(job.Namespace, fsID, pvName)
		if err != nil {
			log.Errorf("create pvc for job[%s] failed, err: %v", job.ID, err)
			return err
		}
	}
	// submit job
	traceLogger.Infof("submit kubernetes job")
	fwVersion := kr.Client().JobFrameworkVersion(job.JobType, job.Framework)
	err := kr.Job(fwVersion).Submit(context.TODO(), job)
	if err != nil {
		log.Warnf("create kubernetes job[%s] failed, err: %v", job.Name, err)
		return err
	}
	traceLogger.Infof("submit kubernetes job[%s] successful", job.ID)
	log.Debugf("submit kubernetes job[%s] successful", jobID)
	return nil
}

func (kr *KubeRuntime) StopJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("stop job failed, job is nil")
	}
	fwVersion := kr.Client().JobFrameworkVersion(job.JobType, job.Framework)
	return kr.Job(fwVersion).Stop(context.TODO(), job)
}

func (kr *KubeRuntime) UpdateJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("update job failed, job is nil")
	}
	fwVersion := kr.Client().JobFrameworkVersion(job.JobType, job.Framework)
	return kr.Job(fwVersion).Update(context.TODO(), job)
}

func (kr *KubeRuntime) DeleteJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("delete job failed, job is nil")
	}
	fwVersion := kr.Client().JobFrameworkVersion(job.JobType, job.Framework)
	return kr.Job(fwVersion).Delete(context.TODO(), job)
}

func (kr *KubeRuntime) Job(fwVersion pfschema.FrameworkVersion) framework.JobInterface {
	jobPlugin, found := framework.GetJobPlugin(kr.cluster.Type, fwVersion)
	if !found {
		log.Errorf("get job plugin on %s failed, err: %s job is not implemented", kr.String(), fwVersion)
		return &framework.JobSample{}
	}
	return jobPlugin(kr.kubeClient)
}

func getQueueFrameworkVersion(quotaType string) pfschema.FrameworkVersion {
	var gvk schema.GroupVersionKind
	switch quotaType {
	case pfschema.TypeVolcanoCapabilityQuota:
		gvk = k8s.VCQueueGVK
	case pfschema.TypeElasticQuota:
		gvk = k8s.EQuotaGVK
	}
	return pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
}

func (kr *KubeRuntime) CreateQueue(queue *api.QueueInfo) error {
	if queue == nil {
		return fmt.Errorf("create queue failed, queue is nil")
	}
	fwVersion := getQueueFrameworkVersion(queue.Type)
	return kr.Queue(fwVersion).Create(context.TODO(), queue)
}

func (kr *KubeRuntime) UpdateQueue(queue *api.QueueInfo) error {
	if queue == nil {
		return fmt.Errorf("update queue failed, queue is nil")
	}
	fwVersion := getQueueFrameworkVersion(queue.Type)
	return kr.Queue(fwVersion).Update(context.TODO(), queue)
}

func (kr *KubeRuntime) DeleteQueue(queue *api.QueueInfo) error {
	if queue == nil {
		return fmt.Errorf("delete queue failed, queue is nil")
	}
	fwVersion := getQueueFrameworkVersion(queue.Type)
	return kr.Queue(fwVersion).Delete(context.TODO(), queue)
}

func (kr *KubeRuntime) Queue(fwVersion pfschema.FrameworkVersion) framework.QueueInterface {
	queuePlugin, found := framework.GetQueuePlugin(kr.cluster.Type, fwVersion)
	if !found {
		log.Errorf("get queue plugin on %s failed, err: %s queue is not implemented", kr.String(), fwVersion)
		return &framework.QueueSample{}
	}
	return queuePlugin(kr.kubeClient)
}

func (kr *KubeRuntime) SyncController(stopCh <-chan struct{}) {
	log.Infof("start job/queue controller on %s", kr.String())
	var err error
	jobQueueSync := os.Getenv(pfschema.EnvEnableJobQueueSync)
	if jobQueueSync == "false" {
		log.Warnf("skip job and queue syn controller on %s", kr.String())
	} else {
		jobController := controller.NewJobSync()
		err = jobController.Initialize(kr.kubeClient)
		if err != nil {
			log.Errorf("init job controller on %s failed, err: %v", kr.String(), err)
			return
		}
		queueController := controller.NewQueueSync()
		err = queueController.Initialize(kr.kubeClient)
		if err != nil {
			log.Errorf("init queue controller on %s failed, err: %v", kr.String(), err)
			return
		}
		go jobController.Run(stopCh)
		go queueController.Run(stopCh)
	}

	nodeResourceController := controller.NewNodeResourceSync()
	err = nodeResourceController.Initialize(kr.kubeClient)
	if err != nil {
		log.Errorf("init node resource controller on %s failed, err: %v", kr.String(), err)
		return
	}
	go nodeResourceController.Run(stopCh)
}

func (kr *KubeRuntime) Client() framework.RuntimeClientInterface {
	return kr.kubeClient
}

func (kr *KubeRuntime) GetQueueUsedQuota(q *api.QueueInfo) (*resources.Resource, error) {
	log.Infof("on %s, get used quota for queue %s, namespace %s", kr.String(), q.Name, q.Namespace)

	fieldSelector := fmt.Sprintf(
		"status.phase!=Succeeded,status.phase!=Failed,status.phase!=Unknown,spec.schedulerName=%s",
		config.GlobalServerConfig.Job.SchedulerName)
	// TODO: add label selector after set queue in pod labels
	listOpts := metav1.ListOptions{
		FieldSelector: fieldSelector,
	}
	podList, err := kr.ListPods(q.Namespace, listOpts)
	if err != nil || podList == nil {
		err = fmt.Errorf("on %s, get queue used quota failed, err: %v", kr.String(), err)
		log.Errorln(err)
		return nil, err
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

func (kr *KubeRuntime) GetObject(namespace, name string, gvk schema.GroupVersionKind) (interface{}, error) {
	log.Debugf("get kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	resourceObj, err := kr.kubeClient.Get(namespace, name, client.KubeFrameworkVersion(gvk))
	if err != nil {
		log.Errorf("get kubernetes %s resource %s/%s failed, err: %v", gvk.String(), namespace, name, err.Error())
		return nil, err
	}
	return resourceObj, nil
}

func (kr *KubeRuntime) DeleteObject(namespace, name string, gvk schema.GroupVersionKind) error {
	log.Infof("delete kubernetes %s resource: %s/%s", gvk.String(), namespace, name)
	if err := kr.kubeClient.Delete(namespace, name, client.KubeFrameworkVersion(gvk)); err != nil {
		log.Errorf("delete kubernetes %s resource %s/%s failed, err: %v", gvk.String(), namespace, name, err.Error())
		return err
	}
	return nil
}

func (kr *KubeRuntime) CreatePV(namespace, fsID string) (string, error) {
	pv := config.DefaultPV
	pv.Name = pfschema.ConcatenatePVName(namespace, fsID)
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
	pv.Spec.CSI.VolumeAttributes[pfschema.PFSID] = fsID
	pv.Spec.CSI.VolumeAttributes[pfschema.PFSClusterID] = kr.cluster.ID
	pv.Spec.CSI.VolumeAttributes[pfschema.PFSServer] = config.GetServiceAddress()
	pv.Spec.CSI.VolumeAttributes[pfschema.PFSInfo] = base64.StdEncoding.EncodeToString(fsStr)
	pv.Spec.CSI.VolumeAttributes[pfschema.PFSCache] = base64.StdEncoding.EncodeToString(fsCacheConfigStr)
	return nil
}

func (kr *KubeRuntime) CreatePVC(namespace, fsId, pv string) error {
	pvc := config.DefaultPVC
	pvcName := pfschema.ConcatenatePVCName(fsId)
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

func (kr *KubeRuntime) GetLog(jobLogRequest pfschema.JobLogRequest, mixedLogRequest pfschema.MixedLogRequest) (pfschema.JobLogInfo, error) {
	// todo GetJobLog will be merged into getLog
	if jobLogRequest.JobID != "" {
		return kr.GetJobLog(jobLogRequest)
	} else {
		return kr.getLog(mixedLogRequest)
	}
}

func (kr *KubeRuntime) GetJobLog(jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	jobLogInfo := pfschema.JobLogInfo{
		JobID: jobLogRequest.JobID,
	}
	labelSelector := metav1.LabelSelector{}
	switch pfschema.JobType(jobLogRequest.JobType) {
	case pfschema.TypeSingle, pfschema.TypeDistributed, pfschema.TypeWorkflow:
		labelSelector.MatchLabels = map[string]string{
			pfschema.JobIDLabel: jobLogRequest.JobID,
		}
	default:
		log.Errorf("unknown job type %s, skip get log for job[%s]", jobLogRequest.JobType, jobLogRequest.JobID)
		return pfschema.JobLogInfo{}, errors.New("unknown job type")
	}
	labelMap, err := metav1.LabelSelectorAsMap(&labelSelector)
	if err != nil {
		log.Errorf("job[%s] parse selector to map failed", jobLogRequest.JobID)
		return pfschema.JobLogInfo{}, err
	}
	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelMap).String(),
	}
	podList, err := kr.ListPods(jobLogRequest.Namespace, listOptions)
	if err != nil {
		log.Errorf("job[%s] get pod list failed", jobLogRequest.JobID)
		return pfschema.JobLogInfo{}, err
	}
	taskLogInfoList := make([]pfschema.TaskLogInfo, 0)
	kubeClient := kr.kubeClient.(*client.KubeRuntimeClient)
	for _, pod := range podList.Items {
		itemLogInfoList, err := kubeClient.GetTaskLog(jobLogRequest.Namespace, pod.Name, jobLogRequest.LogFilePosition,
			jobLogRequest.LogPageSize, jobLogRequest.LogPageNo)
		if err != nil {
			log.Errorf("job[%s] construct task[%s] log failed", jobLogRequest.JobID, pod.Name)
			return pfschema.JobLogInfo{}, err
		}
		taskLogInfoList = append(taskLogInfoList, itemLogInfoList...)
	}
	jobLogInfo.TaskList = taskLogInfoList
	return jobLogInfo, nil
}

// GetEvents get events by name and namespace
func (kr *KubeRuntime) GetEvents(namespace, name string) ([]corev1.Event, error) {
	log.Infof("get %s/%s events info from Kubernetes", namespace, name)
	var response []corev1.Event
	listOptions := metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s", name),
	}
	// current k8s unsupported 'sort by time'
	events, err := kr.clientset().CoreV1().Events(namespace).List(context.TODO(), listOptions)
	if err != nil {
		log.Errorf("list events for resource %s/%s failed, err: %v", namespace, name, err)
		return response, err
	}
	return events.Items, nil
}

// GetPod get pod by namespace and name
func (kr *KubeRuntime) GetPod(namespace, name string) (*corev1.Pod, error) {
	log.Debugf("get kubernetes pod: %s/%s", namespace, name)
	return kr.clientset().CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// GetPodsByDeployName return pod list of a deployments
func (kr *KubeRuntime) GetPodsByDeployName(mixedLogRequest pfschema.MixedLogRequest) ([]corev1.Pod, error) {
	name := mixedLogRequest.Name
	namespace := mixedLogRequest.Namespace
	log.Debugf("get kubernetes deploy: %s/%s", namespace, name)
	label := fmt.Sprintf("app=%s", name)
	podList, err := kr.clientset().CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: label})
	if err != nil {
		log.Errorf("get kubernetes pods for deploy %s/%s failed, err: %v", namespace, name, err.Error())
		return nil, err
	}
	return podList.Items, nil
}

// GetMixedLog query logs for deployments/daemonsets and its pods
func (kr *KubeRuntime) getLog(mixedLogRequest pfschema.MixedLogRequest) (pfschema.JobLogInfo, error) {
	var mixedRes pfschema.JobLogInfo
	if mixedLogRequest.Name == "" || mixedLogRequest.Namespace == "" {
		err := fmt.Errorf("name or namespace is absent, cannot find resource")
		log.Errorln(err)
		return mixedRes, err
	}
	var err error
	// get pods list
	var pods []corev1.Pod
	switch mixedLogRequest.ResourceType {
	case string(pfschema.TypePodJob):
		// resourceType is pod and framework is nil
		pod, err := kr.GetPod(mixedLogRequest.Namespace, mixedLogRequest.Name)
		if err != nil {
			log.Errorf("failed to get pods %s/%s. err: %s", mixedLogRequest.Namespace, mixedLogRequest.Name, err.Error())
			return mixedRes, err
		}
		pods = append(pods, *pod)
	case string(pfschema.TypeDeployment):
		// get pods of deployment
		pods, err = kr.GetPodsByDeployName(mixedLogRequest)
		if err != nil {
			log.Errorf("failed to get pods by deployment %s/%s. err: %s",
				mixedLogRequest.Namespace, mixedLogRequest.Name, err.Error())
			return mixedRes, err
		}
	default:
		err = fmt.Errorf("unknown jobtype %s, only support {%s, %s}",
			mixedLogRequest.ResourceType, pfschema.TypeDeployment, pfschema.TypePodJob)
		log.Errorln(err)
		return mixedRes, err
	}

	// get pods logs
	logFilePosition := common.BeginFilePosition
	if mixedLogRequest.IsReadFromTail {
		logFilePosition = common.EndFilePosition
	}
	lineLimitInt, err := strconv.Atoi(mixedLogRequest.LineLimit)
	if err != nil {
		log.Errorf("parse linelimit %s faild, err: %v", mixedLogRequest.LineLimit, err)
		return mixedRes, err
	}
	logPage := utils.LogPage{
		LogFilePosition: logFilePosition,
		LineLimit:       lineLimitInt,
		SizeLimit:       mixedLogRequest.SizeLimit,
	}

	taskLogInfoList := make([]pfschema.TaskLogInfo, 0)
	eventsList := make([]corev1.Event, 0)
	kubeClient := kr.kubeClient.(*client.KubeRuntimeClient)
	for _, pod := range pods {
		itemLogInfoList, err := kubeClient.GetTaskLogV2(mixedLogRequest.Namespace, pod.Name, logPage)
		if err != nil {
			log.Errorf("%s construct %s task log failed, err: %v", mixedLogRequest.ResourceType, pod.Name, err)
			return mixedRes, err
		}
		taskLogInfoList = append(taskLogInfoList, itemLogInfoList...)
		// TODO it can be divided into go routine
		events, err := kr.GetEvents(pod.Namespace, pod.Name)
		if err != nil {
			log.Errorf("failed to get events for %s/%s. err: %v", mixedLogRequest.Namespace, mixedLogRequest.Name, err)
			return mixedRes, err
		}
		eventsList = append(eventsList, events...)
	}
	// events
	events, err := kr.GetEvents(mixedLogRequest.Namespace, mixedLogRequest.Name)
	if err != nil {
		log.Errorf("failed to get events for %s/%s. err: %v", mixedLogRequest.Namespace, mixedLogRequest.Name, err)
		return mixedRes, err
	}
	eventsList = append(eventsList, events...)

	mixedRes.Events = formatAllEventLogs(eventsList, logPage)
	mixedRes.TaskList = taskLogInfoList
	return mixedRes, nil
}

func formatAllEventLogs(events []corev1.Event, logPage utils.LogPage) []string {
	log.Debugf("formatAllEventLogs, events: %v", events)
	sort.Slice(events, func(i, j int) bool {
		// sort by name first, and EventTime second
		if events[i].InvolvedObject.Name != events[j].InvolvedObject.Name {
			return events[i].InvolvedObject.Name < events[j].InvolvedObject.Name
		}
		return events[i].CreationTimestamp.Before(&events[j].CreationTimestamp)
	})
	var formatedEvents []string
	for _, event := range events {
		// Type-Reason-Timestamp-Message
		str := fmt.Sprintf("type: %s\treason: %s\teventsTime: %s \tmessage: %s",
			event.Type, event.Reason, event.CreationTimestamp.Format("2006-01-02 15:04:05"), event.Message)
		formatedEvents = append(formatedEvents, str)
	}
	formatedEvents = logPage.SlicePaging(formatedEvents)
	return formatedEvents
}

func (kr *KubeRuntime) clientset() kubernetes.Interface {
	kubeClient := kr.kubeClient.(*client.KubeRuntimeClient)
	return kubeClient.Client
}

// CreateNamespace Create namespace if not exist
func (kr *KubeRuntime) CreateNamespace(namespace string, opts metav1.CreateOptions) (*corev1.Namespace, error) {
	coreNs := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	ns, err := kr.clientset().CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	} else if ns != nil && err == nil {
		return ns, nil
	}
	return kr.clientset().CoreV1().Namespaces().Create(context.TODO(), coreNs, opts)
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

func (kr *KubeRuntime) GetPersistentVolumeClaims(namespace, name string, getOptions metav1.GetOptions) (*corev1.PersistentVolumeClaim, error) {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) DeletePersistentVolumeClaim(namespace string, name string,
	deleteOptions metav1.DeleteOptions) error {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, deleteOptions)
}

func (kr *KubeRuntime) PatchPVCFinalizerNull(namespace, name string) error {
	type patchStruct struct {
		Op    string   `json:"op"`
		Path  string   `json:"path"`
		Value []string `json:"value"`
	}
	payload := []patchStruct{{
		Op:    "replace",
		Path:  "/metadata/finalizers",
		Value: nil,
	}}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("parse pvc[%s-%s] finalizer null error: %v", namespace, name, err)
		return err
	}
	if err := kr.patchPersistentVolumeClaim(namespace, name, payloadBytes); err != nil {
		log.Errorf("patch pvc[%s-%s] [%s] error: %v", namespace, name, string(payloadBytes), err)
		return err
	}
	return nil
}

func (kr *KubeRuntime) patchPersistentVolumeClaim(namespace, name string, data []byte) error {
	_, err := kr.clientset().CoreV1().PersistentVolumeClaims(namespace).
		Patch(context.TODO(), name, types.JSONPatchType, data, metav1.PatchOptions{})
	return err
}

func (kr *KubeRuntime) getPersistentVolumeClaim(namespace, name string, getOptions metav1.GetOptions) (*corev1.
	PersistentVolumeClaim, error) {
	return kr.clientset().CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, getOptions)
}

func (kr *KubeRuntime) listNodes(listOptions metav1.ListOptions) (*corev1.NodeList, error) {
	return kr.clientset().CoreV1().Nodes().List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) ListPods(namespace string, listOptions metav1.ListOptions) (*corev1.PodList, error) {
	return listPods(kr.clientset(), namespace, listOptions)
}

func listPods(client kubernetes.Interface, namespace string, listOptions metav1.ListOptions) (*corev1.PodList, error) {
	return client.CoreV1().Pods(namespace).List(context.TODO(), listOptions)
}

func (kr *KubeRuntime) DeletePod(namespace, name string) error {
	return kr.clientset().CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func (kr *KubeRuntime) getNodeQuotaListImpl(subQuotaFn func(r *resources.Resource, pod *corev1.Pod) error) (
	pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	result := []pfschema.NodeQuotaInfo{}
	summary := pfschema.QuotaSummary{
		TotalQuota: *k8s.NewResource(corev1.ResourceList{}),
		IdleQuota:  *k8s.NewResource(corev1.ResourceList{}),
	}
	nodes, _ := kr.listNodes(metav1.ListOptions{})
	log.Infof("ListNodeQuota nodes Items len: %d", len(nodes.Items))

	for _, node := range nodes.Items {
		nodeSchedulable := !node.Spec.Unschedulable
		// skip unschedulable node
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

		nodeQuota := pfschema.NodeQuotaInfo{
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

// ListNodeQuota return node resources, including cpu, memory, and extend resources
func (kr *KubeRuntime) ListNodeQuota() (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	return kr.getNodeQuotaListImpl(k8s.SubQuota)
}
