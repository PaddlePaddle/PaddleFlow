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

func (tfj *TFJob) CreateJob() (string, error) {
	pdj := &tfv1.TFJob{}
	if err := tfj.createJobFromYaml(pdj); err != nil {
		log.Errorf("create %s failed, err %v", tfj.String(), err)
		return "", err
	}
	if err := tfj.validateJob(); err != nil {
		log.Errorf("validate %s failed, err %v", tfj.String(), err)
		return "", err
	}

	var err error
	// set metadata field
	tfj.patchMetadata(&pdj.ObjectMeta, tfj.ID)
	// set spec field
	if tfj.IsCustomYaml {
		// set custom TFJob Spec from user
		err = tfj.customTFJobSpec(&pdj.Spec)
	} else {
		// set builtin TFJob Spec
		err = tfj.builtinTFJobSpec(&pdj.Spec)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", tfj.String(), err)
		return "", err
	}
	// create job on cluster
	log.Infof("create %s on cluster", tfj.String())
	if err = Create(pdj, tfj.GroupVersionKind, tfj.DynamicClientOption); err != nil {
		log.Errorf("create %s on cluster failed, err: %v", tfj.String(), err)
		return "", err
	}
	return tfj.ID, err
}

// builtinTensorFlowJobSpec set build-in TFJob spec
func (tfj *TFJob) builtinTFJobSpec(tfJobSpec *tfv1.TFJobSpec) error {
	log.Debugf("patch %s spec:%#v", tfj.String(), tfJobSpec)
	// TODO: set ElasticPolicy for TFJob
	// set TFReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range tfj.Tasks {
		replicaType := tfv1.TFReplicaTypeMaster
		if task.Role == schema.RoleWorker || task.Role == schema.RolePWorker {
			// tf worker
			replicaType = tfv1.TFReplicaTypeWorker
		}
		replicaSpec, ok := tfJobSpec.TFReplicaSpecs[replicaType]
		if !ok {
			return fmt.Errorf("replica type %s for %s is not supported", replicaType, tfj.String())
		}
		if err := tfj.kubeflowReplicaSpec(replicaSpec, &task); err != nil {
			log.Errorf("build %s RepilcaSpec for %s failed, err: %v", replicaType, tfj.String(), err)
			return err
		}
		// calculate job minResources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", tfj.String(), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
	}
	// set RunPolicy
	resourceList := k8s.NewResourceList(minResources)
	return tfj.kubeflowRunPolicy(&tfJobSpec.RunPolicy, &resourceList)
}

func (tfj *TFJob) validateCustomYaml(tfSpec *tfv1.TFJobSpec) error {
	log.Infof("validate custom yaml for %s, pdj from yaml: %v", tfj.String(), tfSpec)
	// TODO: add validate for custom yaml
	return nil
}

// customPyTorchJobSpec set custom PyTorchJob Spec
func (tfj *TFJob) customTFJobSpec(tfJobSpec *tfv1.TFJobSpec) error {
	log.Debugf("patch %s spec:%#v", tfj.String(), tfJobSpec)
	err := tfj.validateCustomYaml(tfJobSpec)
	if err != nil {
		return err
	}
	// TODO: patch tf job from user
	// check RunPolicy
	return nil
}
