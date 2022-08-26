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

package executor

import (
	wfv1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	log "github.com/sirupsen/logrus"
)

// WorkflowJob is a executor struct that runs a workflow
type WorkflowJob struct {
	KubeJob
}

func (wfj *WorkflowJob) CreateJob() (string, error) {
	if err := wfj.validateJob(); err != nil {
		log.Errorf("validate %s job failed, err %v", wfj.JobType, err)
		return "", err
	}
	workflowJob := &wfv1.Workflow{}
	if err := wfj.createJobFromYaml(workflowJob); err != nil {
		log.Errorf("create job failed, err %v", err)
		return "", err
	}

	// patch .metadata field
	wfj.patchMetadata(&workflowJob.ObjectMeta, wfj.ID)
	// patch .spec field
	err := wfj.patchWorkflowSpec(&workflowJob.Spec)
	if err != nil {
		log.Errorf("build job spec failed, err %v", err)
		return "", err
	}
	// create workflow on cluster
	log.Infof("create %s job %s/%s on cluster", wfj.JobType, wfj.Namespace, wfj.ID)
	log.Infof("workflow job: %v", workflowJob)
	if err = Create(workflowJob, wfj.GroupVersionKind, wfj.DynamicClientOption); err != nil {
		log.Errorf("create %s job %s/%s on cluster failed, err %v", wfj.JobType, wfj.Namespace, wfj.ID, err)
		return "", err
	}
	return wfj.ID, err
}

func (wfj *WorkflowJob) patchWorkflowSpec(spec *wfv1.WorkflowSpec) error {
	// TODO: patch workflow spec
	return nil
}
