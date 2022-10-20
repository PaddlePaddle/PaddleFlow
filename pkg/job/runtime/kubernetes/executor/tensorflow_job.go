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
	"fmt"

	tfv1 "github.com/kubeflow/training-operator/pkg/apis/tensorflow/v1"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type TFJob struct {
	KubeJob
}

func (j *TFJob) CreateJob() (string, error) {
	log.Debugf("tf job %s creating, job struct: %+v", j.Name, j)
	tfJob := &tfv1.TFJob{}
	if err := j.createJobFromYaml(tfJob); err != nil {
		log.Errorf("create %s failed, err %v", j.String(), err)
		return "", err
	}
	if err := j.validateJob(); err != nil {
		log.Errorf("validate %s failed, err %v", j.String(), err)
		return "", err
	}

	var err error
	// set metadata field
	j.patchMetadata(&tfJob.ObjectMeta, j.ID)
	// set spec field
	if j.IsCustomYaml {
		// set custom TFJob Spec from user
		err = j.customTFJobSpec(&tfJob.Spec)
	} else {
		// set builtin TFJob Spec
		err = j.builtinTFJobSpec(&tfJob.Spec)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", j.String(), err)
		return "", err
	}
	// create job on cluster
	log.Infof("create %s on cluster", j.String())
	log.Debugf("tf job %s created, job struct: %+v", j.Name, j)
	if err = Create(tfJob, j.GroupVersionKind, j.DynamicClientOption); err != nil {
		log.Errorf("create %s on cluster failed, err: %v", j.String(), err)
		return "", err
	}
	return j.ID, err
}

// builtinTensorFlowJobSpec set build-in TFJob spec
// TODO: add GPU supports for TFJob, now only support for CPU type job
func (j *TFJob) builtinTFJobSpec(tfJobSpec *tfv1.TFJobSpec) error {
	log.Debugf("patch %s spec:%#v", j.String(), tfJobSpec)
	// TODO: set ElasticPolicy for TFJob
	// set TFReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range j.Tasks {
		// tf parameter server for distributed training
		replicaType := tfv1.TFReplicaTypePS
		// if role is worker, set it to tf worker
		if task.Role == schema.RoleWorker || task.Role == schema.RolePWorker {
			// tf worker for distributed training
			replicaType = tfv1.TFReplicaTypeWorker
		}
		replicaSpec, ok := tfJobSpec.TFReplicaSpecs[replicaType]
		if !ok {
			return fmt.Errorf("replica type %s for %s is not supported", replicaType, j.String())
		}
		if err := j.kubeflowReplicaSpec(replicaSpec, &task); err != nil {
			log.Errorf("build %s RepilcaSpec for %s failed, err: %v", replicaType, j.String(), err)
			return err
		}
		// calculate job minResources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", j.String(), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
	}
	// set RunPolicy
	resourceList := k8s.NewResourceList(minResources)
	return j.kubeflowRunPolicy(&tfJobSpec.RunPolicy, &resourceList)
}

func (j *TFJob) validateCustomYaml(tfSpec *tfv1.TFJobSpec) error {
	log.Infof("validate custom yaml for %s, pdj from yaml: %v", j.String(), tfSpec)
	// TODO: add validate for custom yaml
	return nil
}

// customPyTorchJobSpec set custom PyTorchJob Spec
func (j *TFJob) customTFJobSpec(tfJobSpec *tfv1.TFJobSpec) error {
	log.Debugf("patch %s spec:%#v", j.String(), tfJobSpec)
	if tfJobSpec == nil || tfJobSpec.TFReplicaSpecs == nil {
		err := fmt.Errorf("build custom %s failed, TFJobSpec or TFReplicaSpecs is nil", j.String())
		log.Errorf("%v", err)
		return err
	}
	err := j.validateCustomYaml(tfJobSpec)
	if err != nil {
		return err
	}
	// patch metadata
	ps, find := tfJobSpec.TFReplicaSpecs[tfv1.TFReplicaTypePS]
	if find && ps != nil {
		j.patchTaskMetadata(&ps.Template.ObjectMeta, schema.Member{})
	}
	worker, find := tfJobSpec.TFReplicaSpecs[tfv1.TFReplicaTypeWorker]
	if find && worker != nil {
		j.patchTaskMetadata(&worker.Template.ObjectMeta, schema.Member{})
	}
	// check RunPolicy
	return nil
}
