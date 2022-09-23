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

package argoworkflow

import (
	"context"
	"fmt"

	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/util/kuberuntime"
)

var (
	JobGVK                    = k8s.ArgoWorkflowGVK
	KubeArgoWorkflowFwVersion = client.KubeFrameworkVersion(JobGVK)
)

// KubeArgoWorkflowJob is a struct that runs an argo workflow
type KubeArgoWorkflowJob struct {
	GVK              schema.GroupVersionKind
	frameworkVersion pfschema.FrameworkVersion
	runtimeClient    framework.RuntimeClientInterface
	jobQueue         workqueue.RateLimitingInterface
}

func New(kubeClient framework.RuntimeClientInterface) framework.JobInterface {
	return &KubeArgoWorkflowJob{
		runtimeClient:    kubeClient,
		GVK:              JobGVK,
		frameworkVersion: KubeArgoWorkflowFwVersion,
	}
}

func (pj *KubeArgoWorkflowJob) String(name string) string {
	return fmt.Sprintf("%s job %s on %s", pj.GVK.String(), name, pj.runtimeClient.Cluster())
}

func (pj *KubeArgoWorkflowJob) Submit(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	argoWfJob := &wfv1.Workflow{}
	if err := kuberuntime.CreateKubeJobFromYaml(argoWfJob, pj.GVK, job); err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}

	var err error
	// set metadata field
	kuberuntime.BuildJobMetadata(&argoWfJob.ObjectMeta, job)
	// set spec field
	if job.IsCustomYaml {
		// set custom argo workflow Spec from user
		err = pj.customTFJobSpec(&argoWfJob.Spec, job)
	} else {
		// set builtin argo workflow Spec
		err = fmt.Errorf("cannot create %s from builtin template", pj.String(jobName))
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(jobName), err)
		return err
	}
	log.Debugf("begin to create %s, job info: %v", pj.String(jobName), argoWfJob)
	err = pj.runtimeClient.Create(argoWfJob, pj.frameworkVersion)
	if err != nil {
		log.Errorf("create %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeArgoWorkflowJob) customTFJobSpec(spec *wfv1.WorkflowSpec, job *api.PFJob) error {
	// TODO: patch workflow spec
	return nil
}

func (pj *KubeArgoWorkflowJob) Stop(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to stop %s", pj.String(jobName))
	if err := pj.runtimeClient.Delete(job.Namespace, job.ID, pj.frameworkVersion); err != nil {
		log.Errorf("stop %s failed, err: %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeArgoWorkflowJob) Update(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to update %s", pj.String(jobName))
	if err := kuberuntime.UpdateKubeJob(job, pj.runtimeClient, pj.frameworkVersion); err != nil {
		log.Errorf("update %s failed, err: %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeArgoWorkflowJob) Delete(ctx context.Context, job *api.PFJob) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	jobName := job.NamespacedName()
	log.Infof("begin to delete %s ", pj.String(jobName))
	if err := pj.runtimeClient.Delete(job.Namespace, job.ID, pj.frameworkVersion); err != nil {
		log.Errorf("delete %s failed, err %v", pj.String(jobName), err)
		return err
	}
	return nil
}

func (pj *KubeArgoWorkflowJob) GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error) {
	// TODO: add get log logic
	return pfschema.JobLogInfo{}, nil
}

func (pj *KubeArgoWorkflowJob) AddEventListener(ctx context.Context, listenerType string, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	var err error
	switch listenerType {
	case pfschema.ListenerTypeJob:
		err = pj.addJobEventListener(ctx, jobQueue, listener)
	default:
		err = fmt.Errorf("listenerType %s is not supported", listenerType)
	}
	return err
}

func (pj *KubeArgoWorkflowJob) addJobEventListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, listener interface{}) error {
	if jobQueue == nil || listener == nil {
		return fmt.Errorf("add job event listener failed, err: listener is nil")
	}
	pj.jobQueue = jobQueue
	informer := listener.(cache.SharedIndexInformer)
	informer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kuberuntime.ResponsibleForJob,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    pj.addJob,
			UpdateFunc: pj.updateJob,
			DeleteFunc: pj.deleteJob,
		},
	})
	return nil
}

func (pj *KubeArgoWorkflowJob) addJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobAddFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubeArgoWorkflowJob) updateJob(old, new interface{}) {
	jobSyncInfo, err := kuberuntime.JobUpdateFunc(old, new, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

func (pj *KubeArgoWorkflowJob) deleteJob(obj interface{}) {
	jobSyncInfo, err := kuberuntime.JobDeleteFunc(obj, pj.JobStatus)
	if err != nil {
		return
	}
	pj.jobQueue.Add(jobSyncInfo)
}

// JobStatus get the statusInfo of Argo Workflow job, including origin status, pf status and message
func (pj *KubeArgoWorkflowJob) JobStatus(obj interface{}) (api.StatusInfo, error) {
	unObj := obj.(*unstructured.Unstructured)
	// convert to argo workflow struct
	job := &wfv1.Workflow{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unObj.Object, job); err != nil {
		log.Errorf("convert unstructured object [%+v] to %s job failed. error: %s", obj, pj.GVK.String(), err)
		return api.StatusInfo{}, err
	}
	// convert job status
	state, err := pj.getArgoWorkflowStatus(job.Status.Phase)
	if err != nil {
		log.Errorf("get ArgoWorkflow status failed, err: %v", err)
		return api.StatusInfo{}, err
	}
	log.Infof("ArgoWorkflow status: %s", state)
	return api.StatusInfo{
		OriginStatus: string(job.Status.Phase),
		Status:       state,
		Message:      job.Status.Message,
	}, nil
}

func (pj *KubeArgoWorkflowJob) getArgoWorkflowStatus(phase wfv1.NodePhase) (pfschema.JobStatus, error) {
	status := pfschema.JobStatus("")
	switch phase {
	case wfv1.NodePending, wfv1.NodeOmitted, wfv1.NodeSkipped:
		status = pfschema.StatusJobPending
	case wfv1.NodeRunning:
		status = pfschema.StatusJobRunning
	case wfv1.NodeSucceeded:
		status = pfschema.StatusJobSucceeded
	case wfv1.NodeFailed, wfv1.NodeError:
		status = pfschema.StatusJobFailed
	default:
		return status, fmt.Errorf("unexpected ArgoWorkflow status: %s", phase)
	}
	return status, nil
}
