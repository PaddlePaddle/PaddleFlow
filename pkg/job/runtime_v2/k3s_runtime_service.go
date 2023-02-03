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
	"errors"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/controller"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type K3SRuntimeService struct {
	cluster *pfschema.Cluster
	client  framework.RuntimeClientInterface
}

func NewK3SRuntime(cluster pfschema.Cluster) RuntimeService {
	cluster.Type = pfschema.K3SType
	return &K3SRuntimeService{
		cluster: &cluster,
	}
}

func (k3srs *K3SRuntimeService) String() string {
	msg := "k3s runtime service"
	if k3srs.client != nil {
		msg = k3srs.client.Cluster()
	}
	return msg
}

func (k3srs *K3SRuntimeService) Name() string {
	return fmt.Sprintf("k3s for single node: %s", k3srs.client.ClusterName())
}

func (k3srs *K3SRuntimeService) buildConfig() (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if len(k3srs.cluster.ClientOpt.Config) == 0 {
		if cfg, err = clientcmd.BuildConfigFromFlags("", ""); err != nil {
			log.Errorf("Failed to build rest.config by BuildConfigFromFlags, err:[%v]", err)
			return nil, err
		}
	} else {
		// decode credential base64 string to []byte
		configBytes, decodeErr := base64.StdEncoding.DecodeString(k3srs.cluster.ClientOpt.Config)
		if decodeErr != nil {
			err := fmt.Errorf("decode cluster[%s] credential base64 string error! msg: %s",
				k3srs.cluster.Name, decodeErr.Error())
			return nil, err
		}
		cfg, err = clientcmd.RESTConfigFromKubeConfig(configBytes)
		if err != nil {
			log.Errorf("Failed to build rest.config from k3sConfBytes[%s], err:[%v]", string(configBytes[:]), err)
			return nil, err
		}
	}
	// set qps, burst
	cfg.QPS = k3srs.cluster.ClientOpt.QPS
	cfg.Burst = k3srs.cluster.ClientOpt.Burst
	return cfg, nil
}

func (k3srs *K3SRuntimeService) Init() error {
	// new k3s client
	// 先判断是否已经配置了cluster信息，如果没有则判断是否配置了account service
	initConfig := &rest.Config{}
	if k3srs.cluster != nil {
		config, err := k3srs.buildConfig()
		if err != nil {
			log.Errorf("Build k3s config from setting cluster %v, err : %v", k3srs.cluster, err)
			return err
		}
		initConfig = config
	} else {
		log.Infof("Begin build k3s config use service account.")
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Errorf("Create k3s client faild, maybe not set "+
				"account service or not deploy in cluster, err: %v", err)
			return err
		}
		initConfig = config
	}

	k3sClient, err := kubernetes.NewForConfig(initConfig)
	if err != nil {
		log.Errorf("create k3s client failed, err: %v", err)
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(initConfig)
	if err != nil {
		log.Errorf("init k3s dynamic client failed. error:%s", err)
		return err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)

	cli := &client.K3SRuntimeClient{
		Client:          k3sClient,
		InformerFactory: informers.NewSharedInformerFactory(k3sClient, 0),
		DynamicClient:   dynamicClient,
		DynamicFactory:  factory,
		Config:          initConfig,
		ClusterInfo:     k3srs.cluster,
		JobInformerMap:  make(map[schema.GroupVersionResource]cache.SharedIndexInformer),
	}
	k3srs.client = cli
	return nil
}

func (k3srs *K3SRuntimeService) Client() framework.RuntimeClientInterface {
	return k3srs.client
}

func (k3srs *K3SRuntimeService) SyncController(stopCh <-chan struct{}) {
	log.Infof("start job/queue controller on %s", k3srs.String())
	var err error
	jobQueueSync := os.Getenv(pfschema.EnvEnableJobQueueSync)
	if jobQueueSync == "false" {
		log.Warnf("skip job and queue syn controller on %s", k3srs.String())
	} else {
		jobController := controller.NewJobSync()
		err = jobController.Initialize(k3srs.client)
		if err != nil {
			log.Errorf("init job controller on %s failed, err: %v", k3srs.String(), err)
			return
		}
		go jobController.Run(stopCh)
	}

	nodeResourceController := controller.NewNodeResourceSync()
	err = nodeResourceController.Initialize(k3srs.client)
	if err != nil {
		log.Errorf("init node resource controller on %s failed, err: %v", k3srs.String(), err)
		return
	}
	go nodeResourceController.Run(stopCh)
}

func (k3srs *K3SRuntimeService) getNodeName() (string, error) {
	nodes, err := k3srs.clientSet().CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil || len(nodes.Items) != 1 {
		return "", fmt.Errorf("K3S Get Node Err %v, nodeLen %v ", err.Error(), len(nodes.Items))
	}
	return nodes.Items[0].Name, nil
}

func (k3srs *K3SRuntimeService) SubmitJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("submit job failed, job is nil")
	}
	// add trace log point
	jobID := job.ID
	traceLogger := trace_logger.KeyWithUpdate(jobID)
	msg := fmt.Sprintf("submit job[%v] to cluster[%v] queue[%s]", job.ID, k3srs.cluster, job.QueueID)
	log.Infof(msg)
	traceLogger.Infof(msg)
	// prepare kubernetes storage
	traceLogger.Infof("prepare k3s storage")
	jobFileSystems := job.Conf.GetAllFileSystem()
	for _, task := range job.Tasks {
		jobFileSystems = append(jobFileSystems, task.Conf.GetAllFileSystem()...)
	}
	processedFS := job.Conf.GetProcessedFileSystem()
	for _, fs := range jobFileSystems {
		if fs.Type == pfschema.PFSTypeLocal {
			traceLogger.Infof("only support local filesystem : %v", fs)
			processedFS = append(processedFS, fs)
		} else {
			traceLogger.Infof("skip non local filesystem : %v", fs)
			traceLogger.Infof("no local file system setting is not working.")
		}
	}
	// only support local fs
	job.Conf.SetProcessedFileSystem(processedFS)
	for _, task := range job.Tasks {
		task.Conf.SetProcessedFileSystem(processedFS)
	}
	// set specified node name to env
	nodeName, err := k3srs.getNodeName()
	if err != nil {
		traceLogger.Errorf("k3s get node err :%v", err)
		log.Errorf("k3s get node err: %v", err)
		return err
	}
	job.Conf.SetEnv(pfschema.ENVK3SNodeName, nodeName)
	// submit job
	traceLogger.Infof("submit k3s job")
	traceLogger.Infof("k3s only support single job.")
	traceLogger.Infof("current submit job type: %v, job framework: %v", job.JobType, job.Framework)
	err = k3srs.Job(pfschema.FrameworkVersion{}).Submit(context.TODO(), job)
	if err != nil {
		errMsg := fmt.Sprintf("create k3s job[%s] failed, err: %v", job.Name, err)
		log.Warnf(errMsg)
		traceLogger.Infof(errMsg)
		return err
	}
	traceLogger.Infof("submit k3s job[%s] success", job.ID)
	log.Debugf("submit k3s job[%s] success", jobID)
	return nil
}

func (k3srs *K3SRuntimeService) Job(fwVersion pfschema.FrameworkVersion) framework.JobInterface {
	// default use pod gvk
	gvk := k8s.PodGVK
	fv := pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	jobPlugin, found := framework.GetJobPlugin(pfschema.K3SType, fv)
	if !found {
		errMsg := fmt.Sprintf("get job plugin on %s failed, err: %s job is not implemented", k3srs.String(), fv)
		log.Errorf(errMsg)
	}
	return jobPlugin(k3srs.client)
}

func (k3srs *K3SRuntimeService) StopJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("stop job failed, job is nil")
	}
	return k3srs.Job(pfschema.FrameworkVersion{}).Stop(context.TODO(), job)
}

func (k3srs *K3SRuntimeService) UpdateJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("update job failed, job is nil")
	}
	return k3srs.Job(pfschema.FrameworkVersion{}).Update(context.TODO(), job)
}

func (k3srs *K3SRuntimeService) DeleteJob(job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("delete job failed, job is nil")
	}
	return k3srs.Job(pfschema.FrameworkVersion{}).Stop(context.TODO(), job)
}

func (k3srs *K3SRuntimeService) GetLog(jobLogRequest pfschema.JobLogRequest, mixedLogRequest pfschema.MixedLogRequest) (pfschema.JobLogInfo, error) {
	if jobLogRequest.JobID == "" {
		log.Errorf("must set job id, skip get log for job log request[%v]", jobLogRequest)
		return pfschema.JobLogInfo{}, errors.New(" Must use a right job info.")
	}
	if jobLogRequest.JobType != string(pfschema.TypeSingle) {
		log.Errorf("Unsupport job type, skip get log for job log request[%v].", jobLogRequest)
		return pfschema.JobLogInfo{}, fmt.Errorf(" Unsupport job type %v", jobLogRequest.JobType)
	}
	jobLogInfo := pfschema.JobLogInfo{
		JobID: jobLogRequest.JobID,
	}
	labelSelector := metav1.LabelSelector{}
	labelSelector.MatchLabels = map[string]string{
		pfschema.JobIDLabel: jobLogRequest.JobID,
	}
	labelMap, err := metav1.LabelSelectorAsMap(&labelSelector)
	if err != nil {
		log.Errorf("job[%s] parse selector to map failed.", jobLogRequest.JobID)
		return pfschema.JobLogInfo{}, err
	}
	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelMap).String(),
	}
	podList, err := k3srs.ListPods(jobLogRequest.Namespace, listOptions)
	if err != nil || podList.Items == nil {
		log.Errorf("job[%s] get pod list failed, maybe job is not exist.", jobLogRequest.JobID)
		return pfschema.JobLogInfo{}, err
	}
	taskLogInfoList := make([]pfschema.TaskLogInfo, 0)
	k3sClient := k3srs.client.(*client.K3SRuntimeClient)
	for _, pod := range podList.Items {
		itemLogInfoList, err := k3sClient.GetTaskLog(jobLogRequest.Namespace, pod.Name, jobLogRequest.LogFilePosition,
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

// Queue quota type???
func (k3srs *K3SRuntimeService) Queue(quotaType pfschema.FrameworkVersion) framework.QueueInterface {
	log.Infof("k3s runtime not support queue info, so skip it, queue info:%v", quotaType)
	return nil
}

func (k3srs *K3SRuntimeService) CreateQueue(queue *api.QueueInfo) error {
	log.Infof("k3s runtime not support queue created, so skip it, queue info:%v", queue)
	return nil
}

func (k3srs *K3SRuntimeService) DeleteQueue(queue *api.QueueInfo) error {
	log.Infof("k3s runtime not support queue deleted, so skip it, queue info:%v", queue)
	return nil
}

func (k3srs *K3SRuntimeService) UpdateQueue(queue *api.QueueInfo) error {
	log.Infof("k3s runtime not support queue updated, so skip it, queue info:%v", queue)
	return nil
}

func (k3srs *K3SRuntimeService) ListNodeQuota() (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	return k3srs.getNodeQuotaListImpl(k8s.SubQuota)
}

func (k3srs *K3SRuntimeService) clientSet() kubernetes.Interface {
	k3sClient := k3srs.client.(*client.K3SRuntimeClient)
	return k3sClient.Client
}

func (k3srs *K3SRuntimeService) ListPods(namespace string, listOptions metav1.ListOptions) (*corev1.PodList, error) {
	return k3srs.clientSet().CoreV1().Pods(namespace).List(context.TODO(), listOptions)
}

func (k3srs *K3SRuntimeService) getNodeQuotaListImpl(subQuotaFn func(r *resources.Resource, pod *corev1.Pod) error) (
	pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	result := []pfschema.NodeQuotaInfo{}
	summary := pfschema.QuotaSummary{
		TotalQuota: *k8s.NewResource(corev1.ResourceList{}),
		IdleQuota:  *k8s.NewResource(corev1.ResourceList{}),
	}
	nodes, _ := k3srs.clientSet().CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
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

		pods, _ := k3srs.ListPods("", metav1.ListOptions{
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
