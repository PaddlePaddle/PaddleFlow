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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	rayV1alpha1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/ray-operator/v1alpha1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	envGroupName   = "GROUP_NAME"
	envMinReplicas = "MIN_REPLICAS"
	envMaxReplicas = "MAX_REPLICAS"
)

type RayJob struct {
	KubeJob
}

func (j *RayJob) validateJob() error {
	if err := j.KubeJob.validateJob(); err != nil {
		log.Errorf("validate rayJob %s failed, err %v", j.String(), err)
		return err
	}
	// todo check rayJob specified envs
	return nil
}

func (j *RayJob) CreateJob() (string, error) {
	log.Debugf("ray job %s creating, job struct: %+v", j.Name, j)
	rayJob := &rayV1alpha1.RayJob{}
	if err := j.createJobFromYaml(rayJob); err != nil {
		log.Errorf("create %s failed, err %v", j.String(), err)
		return "", err
	}
	if err := j.validateJob(); err != nil {
		log.Errorf("validate %s failed, err %v", j.String(), err)
		return "", err
	}

	var err error
	// set metadata field
	j.patchMetadata(&rayJob.ObjectMeta, j.ID)
	rayJob.Spec.JobId = j.ID
	// set spec field
	if j.IsCustomYaml {
		// set custom RayJob Spec from user
		err = j.customRayJobSpec(&rayJob.Spec)
	} else {
		// set builtin RayJob Spec
		err = j.builtinRayJobSpec(&rayJob.Spec)
	}
	// todo set resource flavour
	if err != nil {
		log.Errorf("build %s spec failed, err %v", j.String(), err)
		return "", err
	}
	// create job on cluster
	log.Infof("create %s on cluster", j.String())
	log.Debugf("ray job %s is created, job struct: %+v", j.Name, j)
	if err = Create(rayJob, j.GroupVersionKind, j.DynamicClientOption); err != nil {
		log.Errorf("create %s on cluster failed, err: %v", j.String(), err)
		return "", err
	}
	return j.ID, err
}

// builtinTensorFlowJobSpec set build-in RayJob spec
func (j *RayJob) builtinRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec) error {
	log.Debugf("patch %s spec:%#v", j.String(), rayJobSpec)
	workerIndex := 0
	rayWorkersLength := len(rayJobSpec.RayClusterSpec.WorkerGroupSpecs)
	for _, member := range j.Tasks {
		var err error
		if member.Role == schema.RoleMaster {
			// keep the same style with other Framework Job, the command store in master task
			fillRayJobSpec(rayJobSpec, member)
			// head
			err = j.buildHeadPod(rayJobSpec, member)
		} else {
			//worker
			err = j.buildWorkerPod(rayJobSpec, member, workerIndex, rayWorkersLength)
			workerIndex++
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func fillRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec, member schema.Member) {
	// 	Entrypoint string `json:"entrypoint"`
	rayJobSpec.Entrypoint = member.Command
	//	RuntimeEnv string `json:"runtimeEnv,omitempty"`
	if runtimeEnv, exist := member.Env["runtimeEnv"]; exist {
		rayJobSpec.Entrypoint = runtimeEnv
	}
	//	ShutdownAfterJobFinishes bool `json:"shutdownAfterJobFinishes,omitempty"`
	rayJobSpec.ShutdownAfterJobFinishes = true
	//	todo Metadata map[string]string `json:"metadata,omitempty"`
	//	todo TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
	//	todo RayClusterSpec RayClusterSpec `json:"rayClusterSpec,omitempty"`
	//	todo ClusterSelector map[string]string `json:"clusterSelector,omitempty"`
}

func (j *RayJob) buildHeadPod(rayJobSpec *rayV1alpha1.RayJobSpec, member schema.Member) error {
	// todo ServiceType is Kubernetes service type. it will be used by the workers to connect to the head pod
	// todo it also need to be validate in validateJob
	// todo EnableIngress indicates whether operator should create ingress object for head service or not.

	headGroupSpec := &rayJobSpec.RayClusterSpec.HeadGroupSpec
	// RayStartParams is start command: node-manager-port, object-store-memory, ...
	if headGroupSpec.RayStartParams == nil {
		headGroupSpec.RayStartParams = make(map[string]string)
	}
	for _, argv := range member.Args {
		paramName, paramValue := parseRayArgs(argv)
		headGroupSpec.RayStartParams[paramName] = paramValue
	}
	// remove command args, which is not necessary in ray headGroupSpec
	member.Command = ""
	member.Args = []string{}
	// Template
	if err := j.fillPodTemplateSpec(&headGroupSpec.Template, member); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}

	return nil
}

func (j *RayJob) buildWorkerPod(rayJobSpec *rayV1alpha1.RayJobSpec, member schema.Member, workerIndex int, rayWorkersLength int) error {
	if member.Env == nil {
		member.Env = make(map[string]string)
	}
	var worker rayV1alpha1.WorkerGroupSpec
	// use exist workerSpec in template
	if workerIndex < rayWorkersLength {
		worker = rayJobSpec.RayClusterSpec.WorkerGroupSpecs[workerIndex]
	}
	// RayStartParams
	if worker.RayStartParams == nil {
		worker.RayStartParams = make(map[string]string)
	}
	for _, argv := range member.Args {
		paramName, paramValue := parseRayArgs(argv)
		worker.RayStartParams[paramName] = paramValue
	}
	//	todo ScaleStrategy defines which pods to remove
	// GroupName
	if groupName, exist := member.Env[envGroupName]; exist {
		worker.GroupName = groupName
	}
	// Replicas
	replicas := int32(member.Replicas)
	worker.Replicas = &replicas
	// minReplicas
	if val, exist, err := getInt32FromEnv(member.Env, envMinReplicas); err != nil {
		err := fmt.Errorf("get minReplicas failed, err: %s", err)
		log.Error(err)
		return err
	} else if exist {
		worker.MinReplicas = &val
	}
	// maxReplicas
	if val, exist, err := getInt32FromEnv(member.Env, envMaxReplicas); err != nil {
		err := fmt.Errorf("get maxReplicas failed, err: %s", err)
		log.Error(err)
		return err
	} else if exist {
		worker.MaxReplicas = &val
	}

	// remove command args, which is not necessary in ray workerGroupSpec
	member.Command = ""
	member.Args = []string{}
	// Template
	if err := j.fillPodTemplateSpec(&worker.Template, member); err != nil {
		log.Errorf("failed to fill containers, err=%v", err)
		return err
	}
	// Template.Containers
	if len(worker.Template.Spec.Containers) != 1 {
		worker.Template.Spec.Containers = []v1.Container{{}}
	}
	if err := j.fillContainerInTasks(&worker.Template.Spec.Containers[0], member); err != nil {
		log.Errorf("fill container in task failed, err=[%v]", err)
		return err
	}
	// append into container.VolumeMounts
	taskFs := member.Conf.GetAllFileSystem()
	worker.Template.Spec.Volumes = appendVolumesIfAbsent(worker.Template.Spec.Volumes, generateVolumes(taskFs))
	// worker save into WorkerGroupSpecs finally
	if workerIndex < rayWorkersLength {
		rayJobSpec.RayClusterSpec.WorkerGroupSpecs[workerIndex] = worker
	} else {
		rayJobSpec.RayClusterSpec.WorkerGroupSpecs = append(rayJobSpec.RayClusterSpec.WorkerGroupSpecs, worker)
	}
	return nil
}

func (j *RayJob) validateCustomYaml(rayJobSpec *rayV1alpha1.RayJobSpec) error {
	log.Infof("validate custom yaml for %s, rayJob from yaml: %v", j.String(), rayJobSpec)
	// TODO: add validate for custom yaml
	return nil
}

// customPyTorchJobSpec set custom PyTorchJob Spec
func (j *RayJob) customRayJobSpec(rayJobSpec *rayV1alpha1.RayJobSpec) error {
	log.Debugf("patch %s spec:%#v", j.String(), rayJobSpec)
	err := j.validateCustomYaml(rayJobSpec)
	if err != nil {
		return err
	}
	// TODO: patch ray job from user
	// check RunPolicy
	return nil
}

func getInt32FromEnv(env map[string]string, key string) (int32, bool, error) {
	if valueStr, exist := env[key]; exist {
		valInt, err := strconv.Atoi(valueStr)
		if err != nil {
			err := fmt.Errorf("%s is not valid int type, err: %s", key, err)
			log.Error(err)
			return 0, true, err
		}
		valInt32 := int32(valInt)
		return valInt32, true, nil
	}
	return 0, false, nil
}

func parseRayArgs(argv string) (string, string) {
	args := strings.SplitN(argv, ":", 2)
	// todo validate to ensure args can be split into 2 part
	rayStartParam := strings.TrimSpace(args[1])
	rayStartParam = strings.TrimPrefix(rayStartParam, "'")
	rayStartParam = strings.TrimSuffix(rayStartParam, "'")
	return args[0], rayStartParam
}
