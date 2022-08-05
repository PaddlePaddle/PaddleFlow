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

	pytorchv1 "github.com/kubeflow/training-operator/pkg/apis/pytorch/v1"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type PyTorchJob struct {
	KubeJob
}

func (pj *PyTorchJob) CreateJob() (string, error) {
	pdj := &pytorchv1.PyTorchJob{}
	if err := pj.createJobFromYaml(pdj); err != nil {
		log.Errorf("create %s failed, err %v", pj.String(), err)
		return "", err
	}
	if err := pj.validateJob(); err != nil {
		log.Errorf("validate %s failed, err %v", pj.String(), err)
		return "", err
	}

	var err error
	// set metadata field
	pj.patchMetadata(&pdj.ObjectMeta, pj.ID)
	// set spec field
	if pj.IsCustomYaml {
		// set custom PyTorchJob Spec from user
		err = pj.customPyTorchJobSpec(&pdj.Spec)
	} else {
		// set builtin PyTorchJob Spec
		err = pj.builtinPyTorchJobSpec(&pdj.Spec)
	}
	if err != nil {
		log.Errorf("build %s spec failed, err %v", pj.String(), err)
		return "", err
	}
	// create job on cluster
	log.Infof("create %s on cluster", pj.String())
	if err = Create(pdj, pj.GroupVersionKind, pj.DynamicClientOption); err != nil {
		log.Errorf("create %s on cluster failed, err: %v", pj.String(), err)
		return "", err
	}
	return pj.ID, err
}

// builtinPyTorchJobSpec set build-in PyTorchJob spec
func (pj *PyTorchJob) builtinPyTorchJobSpec(torchJobSpec *pytorchv1.PyTorchJobSpec) error {
	log.Debugf("patch %s spec:%#v", pj.String(), torchJobSpec)
	// TODO: set ElasticPolicy for PyTorchJob
	// set PyTorchReplicaSpecs
	minResources := resources.EmptyResource()
	for _, task := range pj.Tasks {
		replicaType := pytorchv1.PyTorchReplicaTypeMaster
		if task.Role == schema.RoleWorker || task.Role == schema.RolePWorker {
			// pytorch worker
			replicaType = pytorchv1.PyTorchReplicaTypeWorker
		}
		replicaSpec, ok := torchJobSpec.PyTorchReplicaSpecs[replicaType]
		if !ok {
			return fmt.Errorf("replica type %s for %s is not supported", replicaType, pj.String())
		}
		if err := pj.kubeflowReplicaSpec(replicaSpec, &task); err != nil {
			log.Errorf("build %s RepilcaSpec for %s failed, err: %v", replicaType, pj.String(), err)
			return err
		}
		// calculate job minResources
		taskResources, err := resources.NewResourceFromMap(task.Flavour.ToMap())
		if err != nil {
			log.Errorf("parse resources for %s task failed, err: %v", pj.String(), err)
			return err
		}
		taskResources.Multi(task.Replicas)
		minResources.Add(taskResources)
	}
	// set RunPolicy
	resourceList := k8s.NewResourceList(minResources)
	return pj.kubeflowRunPolicy(&torchJobSpec.RunPolicy, &resourceList)
}

func (pj *PyTorchJob) validateCustomYaml(torchJobSpec *pytorchv1.PyTorchJobSpec) error {
	log.Infof("validate custom yaml for %s, pdj from yaml: %v", pj.String(), torchJobSpec)
	// TODO: add validate for custom yaml
	return nil
}

// customPyTorchJobSpec set custom PyTorchJob Spec
func (pj *PyTorchJob) customPyTorchJobSpec(torchJobSpec *pytorchv1.PyTorchJobSpec) error {
	log.Debugf("patch %s spec:%#v", pj.String(), torchJobSpec)
	err := pj.validateCustomYaml(torchJobSpec)
	if err != nil {
		return err
	}
	// TODO: patch pytorch job from user
	// check RunPolicy
	return nil
}
